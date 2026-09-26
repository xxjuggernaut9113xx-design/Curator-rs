//! Windows-native playback bridge.
//!
//! Curator keeps rendering in the native Host and delegates media decoding to
//! the bundled-or-installed mpv executable.  mpv's JSON IPC endpoint gives us
//! one long-lived decoder process, so changing an item replaces the current
//! file instead of accumulating media windows or decoder threads.

use serde_json::{json, Value};
use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::mpsc::{self, Receiver},
    thread,
    time::Duration,
};

#[derive(Debug, Clone)]
pub enum PlayerCommand {
    Load { source: String },
    SetPaused(bool),
    Seek(f64),
    SetVolume(f64),
    SetSpeed(f64),
    SetLoop(bool),
    ToggleFullscreen,
    Stop,
    Shutdown,
}

#[derive(Debug, Clone, Default)]
pub struct PlayerStatus {
    pub message: String,
    pub position_secs: f64,
    pub duration_secs: f64,
    pub volume: f64,
    pub speed: f64,
    pub paused: bool,
    pub looping: bool,
    pub ended: bool,
}

impl PlayerStatus {
    fn loading() -> Self {
        Self {
            message: "Loading native player…".into(),
            volume: 100.0,
            speed: 1.0,
            ..Self::default()
        }
    }
}

pub struct NativePlayer {
    child: Option<Child>,
    writer: Option<File>,
    events: Option<Receiver<Value>>,
    pipe: PathBuf,
    status: PlayerStatus,
    /// True once mpv reported `file-loaded` for the most recent `Load`.
    /// Every `Load`/`Stop` resets it, so an `end-file` that was queued for
    /// the previous file but arrives after the replacement is recognized
    /// as stale and can never mark the new media ended. The mpv event pipe
    /// is ordered: the old file's `end-file` always precedes the new
    /// file's `file-loaded`, which makes this gate exact.
    loaded: bool,
}

impl Default for NativePlayer {
    fn default() -> Self {
        let pipe_name = format!("curator-mpv-{}", std::process::id());
        Self {
            child: None,
            writer: None,
            events: None,
            // mpv uses a Windows named pipe here. Unix builds retain a local
            // socket path so workspace checks continue to cover this module.
            pipe: if cfg!(windows) {
                PathBuf::from(format!(r"\\.\pipe\{pipe_name}"))
            } else {
                std::env::temp_dir().join(format!("{pipe_name}.sock"))
            },
            status: PlayerStatus {
                message: "Choose media to play".into(),
                volume: 100.0,
                speed: 1.0,
                ..Self::default_status()
            },
            loaded: false,
        }
    }
}

impl NativePlayer {
    fn default_status() -> PlayerStatus {
        PlayerStatus::default()
    }

    pub fn status(&self) -> PlayerStatus {
        self.status.clone()
    }

    /// True once mpv confirmed the most recent `Load` with `file-loaded`.
    fn mark_loading(&mut self) {
        self.loaded = false;
        self.status.ended = false;
    }

    /// Resolves the mpv executable Curator would launch, verifying it
    /// actually exists so the UI can report a missing player before the
    /// first playback attempt.
    pub fn mpv_probe() -> Result<String, String> {
        let from_env =
            std::env::var_os("CURATOR_MPV_BIN").map(|value| value.to_string_lossy().into_owned());
        if let Some(path) = from_env {
            return if std::path::Path::new(&path).is_file() {
                Ok(path)
            } else {
                Err(format!(
                    "CURATOR_MPV_BIN points at {path}, which is not a file"
                ))
            };
        }
        if let Ok(executable) = std::env::current_exe() {
            let sibling = executable.with_file_name(if cfg!(windows) { "mpv.exe" } else { "mpv" });
            if sibling.is_file() {
                return Ok(sibling.to_string_lossy().into_owned());
            }
        }
        let file_name = if cfg!(windows) { "mpv.exe" } else { "mpv" };
        if let Some(directories) = std::env::var_os("PATH") {
            for directory in std::env::split_paths(&directories) {
                let candidate = directory.join(file_name);
                if candidate.is_file() {
                    return Ok(candidate.to_string_lossy().into_owned());
                }
            }
        }
        Err(format!(
            "No {file_name} found beside Curator or on PATH. Install mpv or set CURATOR_MPV_BIN."
        ))
    }

    fn start(&mut self) -> Result<(), String> {
        if self.child.is_some() && self.writer.is_some() {
            return Ok(());
        }
        self.teardown();
        let _ = std::fs::remove_file(&self.pipe);
        let executable = Self::mpv_probe()?;
        let mut command = Command::new(executable);
        command
            .arg("--idle=yes")
            .arg("--force-window=yes")
            .arg("--keep-open=yes")
            .arg("--osc=yes")
            .arg("--msg-level=all=warn")
            .arg("--title=Curator Player")
            .arg(format!("--input-ipc-server={}", self.pipe.display()))
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let child = command.spawn().map_err(|error| {
            format!(
                "Could not start native media player ({error}). Install mpv beside Curator or set CURATOR_MPV_BIN."
            )
        })?;
        let mut connected = None;
        for _ in 0..100 {
            match OpenOptions::new().read(true).write(true).open(&self.pipe) {
                Ok(file) => {
                    connected = Some(file);
                    break;
                }
                Err(_) => thread::sleep(Duration::from_millis(30)),
            }
        }
        let Some(file) = connected else {
            let mut child = child;
            let _ = child.kill();
            let _ = child.wait();
            return Err("Native media player did not expose its IPC endpoint".into());
        };
        let reader = file.try_clone().map_err(|error| error.to_string())?;
        let (event_send, event_receive) = mpsc::channel();
        thread::spawn(move || {
            let mut reader = BufReader::new(reader);
            let mut line = String::new();
            loop {
                line.clear();
                match reader.read_line(&mut line) {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {
                        if let Ok(event) = serde_json::from_str::<Value>(&line) {
                            if event_send.send(event).is_err() {
                                break;
                            }
                        }
                    }
                }
            }
        });
        self.child = Some(child);
        self.writer = Some(file);
        self.events = Some(event_receive);
        // mpv only delivers log-message events to clients that opt in.
        let _ = self.send(json!({"command":["enable_event","log-message"]}));
        for (id, property) in [(1, "time-pos"), (2, "duration"), (3, "pause")] {
            self.send(json!({"command":["observe_property", id, property]}))?;
        }
        Ok(())
    }

    fn send(&mut self, command: Value) -> Result<(), String> {
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| "Native media player is unavailable".to_owned())?;
        serde_json::to_writer(&mut *writer, &command).map_err(|error| error.to_string())?;
        writer.write_all(b"\n").map_err(|error| error.to_string())?;
        writer.flush().map_err(|error| error.to_string())
    }

    fn teardown(&mut self) {
        self.writer.take();
        self.events.take();
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = std::fs::remove_file(&self.pipe);
    }

    pub fn apply(&mut self, command: PlayerCommand) -> PlayerStatus {
        let result = match command {
            PlayerCommand::Load { source } => {
                // A fresh load resets every per-file flag first. mpv reports
                // the previous file's end-file before the new file-loaded,
                // so ended must be cleared here and not only on file-loaded.
                // Resetting the loaded gate makes that stale end-file
                // unmistakable even if it arrives after the new load.
                self.mark_loading();
                self.status = PlayerStatus::loading();
                self.start()
                    .and_then(|_| self.send(json!({"command":["loadfile", source, "replace"]})))
            }
            PlayerCommand::SetPaused(paused) => {
                self.status.paused = paused;
                self.send(json!({"command":["set_property", "pause", paused]}))
            }
            PlayerCommand::Seek(position) => {
                let position = position.max(0.0);
                self.status.position_secs = position;
                self.send(json!({"command":["set_property", "time-pos", position]}))
            }
            PlayerCommand::SetVolume(volume) => {
                let volume = volume.clamp(0.0, 100.0);
                self.status.volume = volume;
                self.send(json!({"command":["set_property", "volume", volume]}))
            }
            PlayerCommand::SetSpeed(speed) => {
                let speed = speed.clamp(0.25, 4.0);
                self.status.speed = speed;
                self.send(json!({"command":["set_property", "speed", speed]}))
            }
            PlayerCommand::SetLoop(looping) => {
                self.status.looping = looping;
                self.send(json!({"command":["set_property", "loop-file", if looping { "inf" } else { "no" }]}))
            }
            PlayerCommand::ToggleFullscreen => {
                self.send(json!({"command":["cycle", "fullscreen"]}))
            }
            PlayerCommand::Stop => {
                self.mark_loading();
                self.status.message = "Stopped".into();
                self.status.position_secs = 0.0;
                self.send(json!({"command":["stop"]}))
            }
            PlayerCommand::Shutdown => {
                self.teardown();
                self.status.message = "Stopped".into();
                Ok(())
            }
        };
        if let Err(error) = result {
            self.status.message = error;
        }
        self.status()
    }

    pub fn drain_events(&mut self) -> Vec<PlayerStatus> {
        let mut updates = Vec::new();
        let Some(events) = &self.events else {
            return updates;
        };
        while let Ok(event) = events.try_recv() {
            match event.get("event").and_then(Value::as_str) {
                Some("file-loaded") => {
                    // A stale end-file from a replaced file can only arrive
                    // before this event on the ordered pipe, so clearing the
                    // ended flag here keeps rapid switches from advancing the
                    // wrong queue item. This event also opens the gate: only
                    // end-files observed after it can mark the media ended.
                    self.loaded = true;
                    self.status.ended = false;
                    self.status.position_secs = 0.0;
                    self.status.duration_secs = 0.0;
                    self.status.message = "Playing".into();
                }
                Some("end-file") => {
                    // Only a genuine end-of-file advances the queue or the
                    // automated modes. Replacing the file (`loadfile`
                    // `replace`) and explicit stops report reason "stop", and
                    // acting on those would skip the item that was just
                    // loaded. A failed decode reports "error": it surfaces
                    // through the message/log handlers and the user skips
                    // manually, so a broken file can never silently advance
                    // past content the user has not seen. An end-file that
                    // arrives before the current file's file-loaded is stale
                    // (it belongs to the replaced file) and is ignored.
                    let reason = event.get("reason").and_then(Value::as_str).unwrap_or("");
                    if reason == "eof" && self.loaded {
                        self.status.message = "Finished".into();
                        self.status.ended = true;
                    } else if reason == "error" {
                        self.status.message = "Playback error".into();
                    }
                }
                Some("property-change") => {
                    let property = event
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    match property {
                        "time-pos" => {
                            self.status.position_secs = event["data"].as_f64().unwrap_or(0.0)
                        }
                        "duration" => {
                            self.status.duration_secs = event["data"].as_f64().unwrap_or(0.0)
                        }
                        "pause" => self.status.paused = event["data"].as_bool().unwrap_or(false),
                        _ => continue,
                    }
                }
                Some("log-message") if event["level"].as_str() == Some("error") => {
                    self.status.message = event["text"]
                        .as_str()
                        .unwrap_or("Native player error")
                        .trim()
                        .to_owned();
                }
                _ => continue,
            }
            updates.push(self.status());
        }
        updates
    }
}

impl Drop for NativePlayer {
    fn drop(&mut self) {
        self.teardown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // The probe reads process-wide environment state, so tests that mutate
    // CURATOR_MPV_BIN serialize on this lock.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn status_starts_with_a_safe_idle_state() {
        let player = NativePlayer::default();
        assert_eq!(player.status().message, "Choose media to play");
        assert_eq!(player.status().volume, 100.0);
        assert_eq!(player.status().speed, 1.0);
    }

    #[test]
    fn command_clamps_user_supplied_playback_values() {
        let mut player = NativePlayer::default();
        // The unavailable IPC endpoint still preserves validated UI state and
        // gives an actionable error instead of panicking.
        let status = player.apply(PlayerCommand::SetSpeed(99.0));
        assert_eq!(status.speed, 4.0);
        let status = player.apply(PlayerCommand::SetVolume(-1.0));
        assert_eq!(status.volume, 0.0);
    }

    #[test]
    fn ended_flag_cannot_survive_a_fresh_load_or_stop() {
        let _guard = ENV_LOCK.lock().unwrap();
        let mut player = NativePlayer::default();
        player.status.ended = true;
        let status = player.apply(PlayerCommand::Stop);
        assert!(!status.ended, "Stop must clear a stale ended flag");

        // A failing load still replaces the status wholesale, so a stale
        // end-file from the previous media cannot leak into the next item.
        std::env::set_var("CURATOR_MPV_BIN", "/nonexistent/curator-test-mpv");
        let status = player.apply(PlayerCommand::Load {
            source: "clip.mp4".into(),
        });
        std::env::remove_var("CURATOR_MPV_BIN");
        assert!(!status.ended, "Load must clear a stale ended flag");
        assert!(status.message.contains("CURATOR_MPV_BIN"));
    }

    #[test]
    fn only_eof_end_file_marks_the_item_ended() {
        let mut player = NativePlayer::default();
        let (tx, rx) = mpsc::channel();
        player.events = Some(rx);
        // Replacing the file reports reason "stop": it must never advance.
        tx.send(json!({"event": "end-file", "reason": "stop"}))
            .unwrap();
        let statuses = player.drain_events();
        assert!(
            !statuses.last().expect("stop event drains").ended,
            "a replacement load must not end the new item"
        );
        // An end-file that arrives before the file-loaded gate opens belongs
        // to a replaced file and must never advance.
        tx.send(json!({"event": "end-file", "reason": "eof"}))
            .unwrap();
        let statuses = player.drain_events();
        assert!(
            !statuses.last().expect("stale eof drains").ended,
            "an end-file before file-loaded must not end the item"
        );
        // A genuine end-of-file for the loaded file advances.
        tx.send(json!({"event": "file-loaded"})).unwrap();
        tx.send(json!({"event": "end-file", "reason": "eof"}))
            .unwrap();
        let statuses = player.drain_events();
        assert!(
            statuses.last().expect("eof event drains").ended,
            "a genuine end-of-file must mark the item ended"
        );
    }

    #[test]
    fn error_end_file_surfaces_a_message_without_advancing() {
        let mut player = NativePlayer::default();
        let (tx, rx) = mpsc::channel();
        player.events = Some(rx);
        tx.send(json!({"event": "end-file", "reason": "error"}))
            .unwrap();
        let statuses = player.drain_events();
        let status = statuses.last().expect("error event drains");
        assert!(!status.ended, "a decode error must not advance the queue");
        assert!(
            status.message.to_lowercase().contains("error"),
            "a decode error must surface in the player message"
        );
    }

    /// Injects a fresh mpv event channel. Each `apply(Load|Stop)` tears the
    /// previous channel down via `start()`/`teardown()`; in production the
    /// mpv child stays alive across loads so the channel persists, but tests
    /// without a real mpv must re-inject after every `apply`.
    fn inject_events(player: &mut NativePlayer) -> mpsc::Sender<Value> {
        let (tx, rx) = mpsc::channel();
        player.events = Some(rx);
        tx
    }

    #[test]
    fn load_and_stop_reset_the_loaded_gate() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("CURATOR_MPV_BIN", "/nonexistent/curator-test-mpv");
        let mut player = NativePlayer::default();
        player.apply(PlayerCommand::Load {
            source: "a.mp4".into(),
        });
        let tx = inject_events(&mut player);
        tx.send(json!({"event": "file-loaded"})).unwrap();
        player.drain_events();
        assert!(player.loaded, "file-loaded opens the end-file gate");
        // A failing load still resets the gate: the stale media is gone
        // either way.
        player.apply(PlayerCommand::Load {
            source: "b.mp4".into(),
        });
        assert!(
            !player.loaded,
            "a new load closes the gate until file-loaded"
        );
        let tx = inject_events(&mut player);
        tx.send(json!({"event": "file-loaded"})).unwrap();
        player.drain_events();
        player.apply(PlayerCommand::Stop);
        assert!(
            !player.loaded,
            "stop closes the gate until the next file-loaded"
        );
        std::env::remove_var("CURATOR_MPV_BIN");
    }

    #[test]
    fn stale_end_file_after_rapid_replacement_never_advances() {
        let mut player = NativePlayer::default();
        let tx = inject_events(&mut player);
        // Item A loads and plays to the end; its end-file is still queued
        // when the user picks item B.
        player.mark_loading();
        tx.send(json!({"event": "file-loaded"})).unwrap();
        player.drain_events();
        tx.send(json!({"event": "end-file", "reason": "eof"}))
            .unwrap();
        // The replacement load closes the gate before anything is drained.
        // (Through `apply` this is `Load`; here the gate transition is
        // driven directly because the test has no live mpv child and
        // `apply` would tear the injected channel down.)
        player.mark_loading();
        // The stale end-file arrives after the new load; the ordered pipe
        // guarantees the new file-loaded follows it.
        let statuses = player.drain_events();
        assert!(
            statuses.iter().all(|status| !status.ended),
            "a stale end-of-file from the replaced item must never mark the new media ended"
        );
        tx.send(json!({"event": "file-loaded"})).unwrap();
        player.drain_events();
        tx.send(json!({"event": "end-file", "reason": "eof"}))
            .unwrap();
        let statuses = player.drain_events();
        assert!(
            statuses.last().expect("eof event drains").ended,
            "the new item's own end-of-file still advances"
        );
    }

    #[test]
    fn mpv_probe_reports_a_missing_executable_actionably() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("CURATOR_MPV_BIN", "/nonexistent/curator-test-mpv");
        let error = NativePlayer::mpv_probe().expect_err("missing binary must fail the probe");
        std::env::remove_var("CURATOR_MPV_BIN");
        assert!(error.contains("CURATOR_MPV_BIN"), "{error}");
    }
}
