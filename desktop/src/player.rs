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
    Load { source: String, title: String },
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
    pub title: String,
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
    fn loading(title: String) -> Self {
        Self {
            title,
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

    fn executable() -> String {
        if let Some(path) = std::env::var_os("CURATOR_MPV_BIN") {
            return path.to_string_lossy().into_owned();
        }
        if let Ok(executable) = std::env::current_exe() {
            let sibling = executable.with_file_name(if cfg!(windows) { "mpv.exe" } else { "mpv" });
            if sibling.is_file() {
                return sibling.to_string_lossy().into_owned();
            }
        }
        if cfg!(windows) { "mpv.exe" } else { "mpv" }.into()
    }

    fn start(&mut self) -> Result<(), String> {
        if self.child.is_some() && self.writer.is_some() {
            return Ok(());
        }
        self.teardown();
        let _ = std::fs::remove_file(&self.pipe);
        let mut command = Command::new(Self::executable());
        command
            .arg("--idle=yes")
            .arg("--force-window=yes")
            .arg("--keep-open=yes")
            .arg("--osc=yes")
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
        for _ in 0..50 {
            match OpenOptions::new().read(true).write(true).open(&self.pipe) {
                Ok(file) => {
                    connected = Some(file);
                    break;
                }
                Err(_) => thread::sleep(Duration::from_millis(20)),
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
            PlayerCommand::Load { source, title } => {
                self.status = PlayerStatus::loading(title);
                self.start().and_then(|_| self.send(json!({"command":["loadfile", source, "replace"]})))
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
            PlayerCommand::ToggleFullscreen => self.send(json!({"command":["cycle", "fullscreen"]})),
            PlayerCommand::Stop => {
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
                Some("file-loaded") => self.status.message = "Playing".into(),
                Some("end-file") => {
                    self.status.message = "Finished".into();
                    self.status.ended = true;
                }
                Some("property-change") => {
                    let property = event.get("name").and_then(Value::as_str).unwrap_or_default();
                    match property {
                        "time-pos" => self.status.position_secs = event["data"].as_f64().unwrap_or(0.0),
                        "duration" => self.status.duration_secs = event["data"].as_f64().unwrap_or(0.0),
                        "pause" => self.status.paused = event["data"].as_bool().unwrap_or(false),
                        _ => continue,
                    }
                }
                Some("log-message") if event["level"].as_str() == Some("error") => {
                    self.status.message = event["text"].as_str().unwrap_or("Native player error").trim().to_owned();
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
}
