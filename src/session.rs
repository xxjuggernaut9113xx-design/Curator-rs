//! Versioned, deterministic session domain shared by native and remote clients.
//!
//! This module deliberately owns no UI, database connection, media player, or
//! wall-clock timer.  A caller feeds monotonic timestamps through [`SessionCommand`]
//! and performs returned [`SessionEffect`] values with the relevant adapter.

use rand::{rngs::StdRng, Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    OnceLock,
};
use std::time::Instant;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

pub const GAME_CONFIG_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GameMetadata {
    pub name: String,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EndCondition {
    pub active_duration_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DifficultyConfig {
    pub tempo_multiplier: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TempoCurve {
    Constant {
        bpm: f64,
    },
    Linear {
        start_bpm: f64,
        end_bpm: f64,
    },
    Stepped {
        start_bpm: f64,
        end_bpm: f64,
        steps: u16,
    },
    Eased {
        start_bpm: f64,
        end_bpm: f64,
    },
    Adaptive {
        base_bpm: f64,
    },
}

impl TempoCurve {
    fn target_bpm(&self, progress: f64) -> f64 {
        let progress = progress.clamp(0.0, 1.0);
        match self {
            Self::Constant { bpm } => *bpm,
            Self::Linear { start_bpm, end_bpm } => start_bpm + (end_bpm - start_bpm) * progress,
            Self::Stepped {
                start_bpm,
                end_bpm,
                steps,
            } => {
                let steps = f64::from((*steps).max(1));
                let stepped_progress = (progress * steps).floor() / steps;
                start_bpm + (end_bpm - start_bpm) * stepped_progress
            }
            Self::Eased { start_bpm, end_bpm } => {
                let eased = progress * progress * (3.0 - 2.0 * progress);
                start_bpm + (end_bpm - start_bpm) * eased
            }
            Self::Adaptive { base_bpm } => *base_bpm,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TempoConfig {
    pub curve: TempoCurve,
    pub min_bpm: f64,
    pub max_bpm: f64,
    /// Maximum fraction of the remaining target difference applied per second.
    pub smoothing_per_second: f64,
    pub meter: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SelectionStrategy {
    Sequential,
    Shuffle,
    SeededRandom,
    WeightedRandom,
    RatingWeighted,
    TagWeighted,
    LeastRecentlyShown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MediaConfig {
    pub selection_strategy: SelectionStrategy,
    pub required_tags: Vec<String>,
    pub excluded_tags: Vec<String>,
    pub allow_video_interruption: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PhaseConfig {
    pub id: String,
    pub duration_ms: u64,
    pub tempo_multiplier: f64,
    pub event_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EventDefinition {
    pub id: String,
    /// 0..=10,000 chance evaluated when the applicable phase begins.
    pub probability_basis_points: u16,
    pub minimum_occurrences: u16,
    pub maximum_occurrences: u16,
    pub cooldown_ms: u64,
    pub duration_ms: u64,
    pub priority: u8,
    pub allowed_phase_ids: Vec<String>,
    pub instruction: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InstructorConfig {
    pub voice_pack_id: Option<String>,
    pub script_preset_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AudioConfig {
    pub music_playlist_id: Option<String>,
    pub metronome_enabled: bool,
    pub music_volume_percent: u8,
    pub voice_volume_percent: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GameConfig {
    pub schema_version: u32,
    pub metadata: GameMetadata,
    pub duration: EndCondition,
    pub difficulty: DifficultyConfig,
    pub tempo: TempoConfig,
    pub media: MediaConfig,
    pub phases: Vec<PhaseConfig>,
    pub events: Vec<EventDefinition>,
    pub instructor: InstructorConfig,
    pub audio: AudioConfig,
    pub seed: Option<u64>,
}

impl GameConfig {
    /// Conservative native Quick-mode baseline. The generator surfaces the
    /// remaining fields as it grows, but it always compiles to this same
    /// versioned configuration rather than a UI-specific runtime format.
    pub fn quick_default() -> Self {
        Self {
            schema_version: GAME_CONFIG_SCHEMA_VERSION,
            metadata: GameMetadata {
                name: "Quick session".into(),
                description: "Native quick-mode baseline".into(),
            },
            duration: EndCondition {
                active_duration_ms: 15 * 60 * 1_000,
            },
            difficulty: DifficultyConfig {
                tempo_multiplier: 1.0,
            },
            tempo: TempoConfig {
                curve: TempoCurve::Linear {
                    start_bpm: 80.0,
                    end_bpm: 120.0,
                },
                min_bpm: 40.0,
                max_bpm: 180.0,
                smoothing_per_second: 4.0,
                meter: 4,
            },
            media: MediaConfig {
                selection_strategy: SelectionStrategy::SeededRandom,
                required_tags: Vec::new(),
                excluded_tags: Vec::new(),
                allow_video_interruption: false,
            },
            phases: vec![PhaseConfig {
                id: "main".into(),
                duration_ms: 15 * 60 * 1_000,
                tempo_multiplier: 1.0,
                event_ids: Vec::new(),
            }],
            events: Vec::new(),
            instructor: InstructorConfig {
                voice_pack_id: None,
                script_preset_id: None,
            },
            audio: AudioConfig {
                music_playlist_id: None,
                metronome_enabled: true,
                music_volume_percent: 80,
                voice_volume_percent: 80,
            },
            seed: None,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != GAME_CONFIG_SCHEMA_VERSION {
            return Err(format!(
                "Unsupported game-config schema {}",
                self.schema_version
            ));
        }
        if self.duration.active_duration_ms == 0 {
            return Err("A session requires a positive active duration".into());
        }
        if self.phases.is_empty() || self.phases.len() > 128 {
            return Err("A session requires between 1 and 128 phases".into());
        }
        if !self.difficulty.tempo_multiplier.is_finite()
            || !(0.1..=4.0).contains(&self.difficulty.tempo_multiplier)
        {
            return Err("Difficulty tempo multiplier must be between 0.1 and 4".into());
        }
        if !self.tempo.min_bpm.is_finite()
            || !self.tempo.max_bpm.is_finite()
            || self.tempo.min_bpm <= 0.0
            || self.tempo.min_bpm > self.tempo.max_bpm
            || !(1..=16).contains(&self.tempo.meter)
        {
            return Err("Tempo configuration is invalid".into());
        }
        let mut phase_ids = std::collections::HashSet::new();
        for phase in &self.phases {
            if phase.id.trim().is_empty() || phase.duration_ms == 0 || !phase_ids.insert(&phase.id)
            {
                return Err("Every phase needs a unique id and positive duration".into());
            }
            if !phase.tempo_multiplier.is_finite() || !(0.1..=4.0).contains(&phase.tempo_multiplier)
            {
                return Err("Phase tempo multiplier must be between 0.1 and 4".into());
            }
        }
        let mut event_ids = std::collections::HashSet::new();
        for event in &self.events {
            if event.id.trim().is_empty()
                || event.probability_basis_points > 10_000
                || event.minimum_occurrences > event.maximum_occurrences
                || event.duration_ms == 0
                || !event_ids.insert(&event.id)
            {
                return Err("An event definition is invalid".into());
            }
            if event
                .allowed_phase_ids
                .iter()
                .any(|id| !phase_ids.contains(id))
            {
                return Err(format!("Event {} references an unknown phase", event.id));
            }
        }
        for phase in &self.phases {
            if let Some(event_id) = phase.event_ids.iter().find(|id| !event_ids.contains(*id)) {
                return Err(format!(
                    "Phase {} references an unknown event {}",
                    phase.id, event_id
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SessionStatus {
    Ready,
    Running,
    Paused,
    Completed,
    Cancelled,
    Interrupted,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PhaseState {
    pub index: usize,
    pub id: String,
    pub elapsed_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TempoState {
    pub target_bpm: f64,
    pub current_bpm: f64,
    /// User supplied audio/metronome alignment. It is deliberately stored
    /// separately from the monotonic session clock, so pause/resume cannot
    /// accumulate or reset a timing correction.
    pub offset_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EventRecord {
    pub id: String,
    pub started_at_active_ms: u64,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct SessionStatistics {
    pub phase_changes: u32,
    pub events_started: u32,
    pub timing_corrections: u32,
    pub recovery_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SessionState {
    pub session_id: String,
    pub seed: u64,
    pub status: SessionStatus,
    pub active_elapsed_ms: u64,
    pub paused_elapsed_ms: u64,
    pub phase: PhaseState,
    pub tempo: TempoState,
    pub current_event: Option<EventRecord>,
    pub event_history: Vec<EventRecord>,
    pub instruction: Option<String>,
    pub statistics: SessionStatistics,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum SessionCommand {
    Start { monotonic_ms: u64 },
    Tick { monotonic_ms: u64 },
    Pause { monotonic_ms: u64 },
    Resume { monotonic_ms: u64 },
    UserAction { name: String },
    SkipMedia,
    ReportPlaybackFailure { detail: String },
    ChangeTempoOffset { offset_ms: i64 },
    End { completed: bool, monotonic_ms: u64 },
    Interrupt { monotonic_ms: u64 },
}

/// UI and HTTP adapters use this clock-free command shape. The shared service
/// stamps it from one process monotonic clock, keeping UI freezes and remote
/// request latency out of the session's ownership model.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum SessionControl {
    Tick,
    Pause,
    Resume,
    UserAction { name: String },
    SkipMedia,
    ReportPlaybackFailure { detail: String },
    ChangeTempoOffset { offset_ms: i64 },
    End { completed: bool },
    Interrupt,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum SessionEffect {
    StartMusic,
    StopMusic,
    ScheduleMetronome { target_bpm: f64, meter: u8 },
    StartPhase { phase_id: String },
    StartEvent { event_id: String, duration_ms: u64 },
    EndEvent { event_id: String },
    ShowInstruction { text: String },
    SkipMedia,
    PlaybackFailed { detail: String },
    PersistCheckpoint,
    EndSession { status: SessionStatus },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SessionUpdate {
    pub state: SessionState,
    pub effects: Vec<SessionEffect>,
}

/// A deterministic engine. Monotonic time is supplied by the caller, making
/// it testable and safe from UI stalls, hidden windows, and wall-clock edits.
pub struct SessionEngine {
    config: GameConfig,
    state: SessionState,
    rng: StdRng,
    last_monotonic_ms: Option<u64>,
    paused_started_ms: Option<u64>,
}

/// Process-shared session ownership. Desktop and HTTP adapters send commands
/// through this service; neither obtains mutable access to the engine itself.
#[derive(Clone)]
pub struct SessionService {
    engine: std::sync::Arc<std::sync::Mutex<Option<SessionEngine>>>,
    updates: broadcast::Sender<SessionUpdate>,
    runner: std::sync::Arc<std::sync::Mutex<Option<CancellationToken>>>,
    clock: std::sync::Arc<dyn Fn() -> u64 + Send + Sync>,
}

impl Default for SessionService {
    fn default() -> Self {
        let (updates, _) = broadcast::channel(64);
        Self {
            engine: std::sync::Arc::new(std::sync::Mutex::new(None)),
            updates,
            runner: std::sync::Arc::new(std::sync::Mutex::new(None)),
            clock: std::sync::Arc::new(monotonic_ms),
        }
    }
}

impl SessionService {
    #[cfg(test)]
    fn with_clock(clock: impl Fn() -> u64 + Send + Sync + 'static) -> Self {
        Self {
            clock: std::sync::Arc::new(clock),
            ..Self::default()
        }
    }

    fn now(&self) -> u64 {
        (self.clock)()
    }

    pub fn start(&self, config: GameConfig) -> Result<SessionState, String> {
        let mut slot = self
            .engine
            .lock()
            .map_err(|_| "Session service is unavailable")?;
        if slot.as_ref().is_some_and(|engine| {
            matches!(
                engine.state.status,
                SessionStatus::Ready | SessionStatus::Running | SessionStatus::Paused
            )
        }) {
            return Err("A session is already active".into());
        }
        let mut engine = SessionEngine::new(config)?;
        // Config seeds make selection reproducible, but a durable session ID
        // must identify each run independently (including repeated test/demo
        // runs with the same seed).
        static SESSION_SEQUENCE: AtomicU64 = AtomicU64::new(1);
        let sequence = SESSION_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        engine.state.session_id = format!("session-{:016x}-{sequence:016x}", engine.state.seed);
        let snapshot = engine.snapshot();
        *slot = Some(engine);
        Ok(snapshot)
    }

    pub fn dispatch(&self, command: SessionCommand) -> Result<SessionUpdate, String> {
        let mut slot = self
            .engine
            .lock()
            .map_err(|_| "Session service is unavailable")?;
        let update = slot
            .as_mut()
            .map(|engine| engine.dispatch(command))
            .ok_or_else(|| "No active session".to_string())?;
        let _ = self.updates.send(update.clone());
        Ok(update)
    }

    fn tick_for_session(
        &self,
        session_id: &str,
        monotonic_ms: u64,
    ) -> Result<SessionUpdate, String> {
        let mut slot = self
            .engine
            .lock()
            .map_err(|_| "Session service is unavailable")?;
        let engine = slot
            .as_mut()
            .ok_or_else(|| "No active session".to_string())?;
        if engine.state.session_id != session_id {
            return Err("Session was replaced".into());
        }
        let update = engine.dispatch(SessionCommand::Tick { monotonic_ms });
        let _ = self.updates.send(update.clone());
        Ok(update)
    }

    /// Starts a session through the shared application boundary and advances
    /// it immediately. Adapters never need to manufacture timestamps.
    pub fn start_running(&self, config: GameConfig) -> Result<SessionUpdate, String> {
        self.start(config)?;
        // Do not cancel the current runner until the new session has been
        // admitted. A racing rejected Start must leave that runner alive.
        self.stop_runner();
        let update = self.dispatch(SessionCommand::Start {
            monotonic_ms: self.now(),
        })?;
        self.start_runner(update.state.session_id.clone());
        Ok(update)
    }

    /// Converts an adapter intent to the engine command with an authoritative
    /// process-monotonic timestamp. This is the entry point used by native and
    /// remote UI surfaces.
    pub fn control(&self, control: SessionControl) -> Result<SessionUpdate, String> {
        let now = self.now();
        let command = match control {
            // Kept as a read-compatible remote command, but the application
            // runner alone advances authoritative time.
            SessionControl::Tick => {
                return self
                    .snapshot()
                    .map(|state| SessionUpdate {
                        state,
                        effects: Vec::new(),
                    })
                    .ok_or_else(|| "No active session".into());
            }
            SessionControl::Pause => SessionCommand::Pause { monotonic_ms: now },
            SessionControl::Resume => SessionCommand::Resume { monotonic_ms: now },
            SessionControl::UserAction { name } => SessionCommand::UserAction { name },
            SessionControl::SkipMedia => SessionCommand::SkipMedia,
            SessionControl::ReportPlaybackFailure { detail } => {
                SessionCommand::ReportPlaybackFailure { detail }
            }
            SessionControl::ChangeTempoOffset { offset_ms } => {
                SessionCommand::ChangeTempoOffset { offset_ms }
            }
            SessionControl::End { completed } => SessionCommand::End {
                completed,
                monotonic_ms: now,
            },
            SessionControl::Interrupt => SessionCommand::Interrupt { monotonic_ms: now },
        };
        self.dispatch(command)
    }

    pub fn snapshot(&self) -> Option<SessionState> {
        self.engine
            .lock()
            .ok()
            .and_then(|slot| slot.as_ref().map(SessionEngine::snapshot))
    }

    pub fn interrupt_active(&self) -> Option<SessionUpdate> {
        self.dispatch(SessionCommand::Interrupt {
            monotonic_ms: self.now(),
        })
        .ok()
    }

    /// Consumers can render or persist every authoritative engine transition
    /// without polling. Slow consumers may coalesce updates; timing remains
    /// owned by the runner, not by a subscriber.
    pub fn subscribe(&self) -> broadcast::Receiver<SessionUpdate> {
        self.updates.subscribe()
    }

    pub fn stop_runner(&self) {
        if let Ok(mut runner) = self.runner.lock() {
            if let Some(cancel) = runner.take() {
                cancel.cancel();
            }
        }
    }

    fn start_runner(&self, session_id: String) {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            // The native Slint shell may be hosted without a Tokio runtime.
            // Its commands still work; the application service supplies the
            // self-advancing runner whenever it is available.
            return;
        };
        let cancellation = CancellationToken::new();
        if let Ok(mut runner) = self.runner.lock() {
            if let Some(previous) = runner.replace(cancellation.clone()) {
                previous.cancel();
            }
        }
        let service = self.clone();
        handle.spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(50));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = cancellation.cancelled() => break,
                    _ = interval.tick() => {
                        if service.snapshot().as_ref().map(|state| &state.session_id) != Some(&session_id) {
                            break;
                        }
                        match service.tick_for_session(&session_id, service.now()) {
                            Ok(update) if matches!(update.state.status, SessionStatus::Completed | SessionStatus::Cancelled | SessionStatus::Interrupted) => break,
                            Ok(_) => {},
                            Err(_) => break,
                        }
                    }
                }
            }
        });
    }
}

fn monotonic_ms() -> u64 {
    static PROCESS_STARTED: OnceLock<Instant> = OnceLock::new();
    PROCESS_STARTED
        .get_or_init(Instant::now)
        .elapsed()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

impl SessionEngine {
    pub fn new(config: GameConfig) -> Result<Self, String> {
        config.validate()?;
        let seed = config.seed.unwrap_or_else(rand::random);
        let first = &config.phases[0];
        let target = Self::target_tempo_for(&config, 0, 0);
        Ok(Self {
            state: SessionState {
                session_id: format!("session-{seed:016x}"),
                seed,
                status: SessionStatus::Ready,
                active_elapsed_ms: 0,
                paused_elapsed_ms: 0,
                phase: PhaseState {
                    index: 0,
                    id: first.id.clone(),
                    elapsed_ms: 0,
                },
                tempo: TempoState {
                    target_bpm: target,
                    current_bpm: target,
                    offset_ms: 0,
                },
                current_event: None,
                event_history: Vec::new(),
                instruction: None,
                statistics: SessionStatistics::default(),
            },
            config,
            rng: StdRng::seed_from_u64(seed),
            last_monotonic_ms: None,
            paused_started_ms: None,
        })
    }

    pub fn snapshot(&self) -> SessionState {
        self.state.clone()
    }

    pub fn dispatch(&mut self, command: SessionCommand) -> SessionUpdate {
        let mut effects = Vec::new();
        match command {
            SessionCommand::Start { monotonic_ms }
                if matches!(self.state.status, SessionStatus::Ready) =>
            {
                self.state.status = SessionStatus::Running;
                self.last_monotonic_ms = Some(monotonic_ms);
                effects.push(SessionEffect::StartMusic);
                effects.push(SessionEffect::StartPhase {
                    phase_id: self.state.phase.id.clone(),
                });
                self.schedule_phase_events(&mut effects);
            }
            SessionCommand::Tick { monotonic_ms } => self.tick(monotonic_ms, &mut effects),
            SessionCommand::Pause { monotonic_ms }
                if self.state.status == SessionStatus::Running =>
            {
                self.tick(monotonic_ms, &mut effects);
                self.state.status = SessionStatus::Paused;
                self.paused_started_ms = Some(monotonic_ms);
                self.last_monotonic_ms = None;
                effects.push(SessionEffect::PersistCheckpoint);
            }
            SessionCommand::Resume { monotonic_ms }
                if self.state.status == SessionStatus::Paused =>
            {
                if let Some(started) = self.paused_started_ms.take() {
                    self.state.paused_elapsed_ms = self
                        .state
                        .paused_elapsed_ms
                        .saturating_add(monotonic_ms.saturating_sub(started));
                }
                self.state.status = SessionStatus::Running;
                self.last_monotonic_ms = Some(monotonic_ms);
            }
            SessionCommand::UserAction { name } => {
                self.state.instruction = Some(name.clone());
                effects.push(SessionEffect::ShowInstruction { text: name });
            }
            SessionCommand::SkipMedia => effects.push(SessionEffect::SkipMedia),
            SessionCommand::ReportPlaybackFailure { detail } => {
                self.state.statistics.recovery_count += 1;
                effects.push(SessionEffect::PlaybackFailed { detail });
                effects.push(SessionEffect::SkipMedia);
            }
            SessionCommand::ChangeTempoOffset { offset_ms } => {
                self.state.tempo.offset_ms = offset_ms.clamp(-30_000, 30_000);
                self.state.statistics.timing_corrections += 1;
                effects.push(SessionEffect::PersistCheckpoint);
            }
            SessionCommand::End {
                completed,
                monotonic_ms,
            } => {
                self.settle_terminal_time(monotonic_ms, &mut effects);
                self.end(
                    if completed {
                        SessionStatus::Completed
                    } else {
                        SessionStatus::Cancelled
                    },
                    &mut effects,
                );
            }
            SessionCommand::Interrupt { monotonic_ms } => {
                self.settle_terminal_time(monotonic_ms, &mut effects);
                self.end(SessionStatus::Interrupted, &mut effects);
            }
            _ => {}
        }
        SessionUpdate {
            state: self.snapshot(),
            effects,
        }
    }

    fn settle_terminal_time(&mut self, monotonic_ms: u64, effects: &mut Vec<SessionEffect>) {
        match self.state.status {
            SessionStatus::Running => {
                self.advance_running_time(monotonic_ms, effects);
            }
            SessionStatus::Paused => {
                if let Some(started) = self.paused_started_ms.take() {
                    self.state.paused_elapsed_ms = self
                        .state
                        .paused_elapsed_ms
                        .saturating_add(monotonic_ms.saturating_sub(started));
                }
            }
            _ => {}
        }
    }

    fn tick(&mut self, monotonic_ms: u64, effects: &mut Vec<SessionEffect>) {
        if self.state.status != SessionStatus::Running {
            return;
        }
        let target = self.advance_running_time(monotonic_ms, effects);
        if target >= self.config.duration.active_duration_ms {
            self.end(SessionStatus::Completed, effects);
        }
    }

    /// Advance accounting and scheduled milestones without deciding terminal
    /// status. Explicit End/Interrupt commands call this before applying the
    /// requested terminal transition.
    fn advance_running_time(&mut self, monotonic_ms: u64, effects: &mut Vec<SessionEffect>) -> u64 {
        let previous = self
            .last_monotonic_ms
            .replace(monotonic_ms)
            .unwrap_or(monotonic_ms);
        let target = self
            .state
            .active_elapsed_ms
            .saturating_add(monotonic_ms.saturating_sub(previous))
            .min(self.config.duration.active_duration_ms);
        let before = self.state.active_elapsed_ms;
        self.advance_to_active(target, effects);
        self.update_tempo(target.saturating_sub(before), effects);
        target
    }

    /// Move across each meaningful active-time milestone in order. This keeps
    /// delayed scheduler ticks equivalent to a sequence of timely ticks.
    fn advance_to_active(&mut self, target: u64, effects: &mut Vec<SessionEffect>) {
        while self.state.active_elapsed_ms < target {
            let current = self.state.active_elapsed_ms;
            let event_deadline = self
                .state
                .current_event
                .as_ref()
                .map(|event| event.started_at_active_ms.saturating_add(event.duration_ms));
            let phase_deadline = self.next_phase_boundary();
            let next = [Some(target), event_deadline, phase_deadline]
                .into_iter()
                .flatten()
                .filter(|deadline| *deadline > current)
                .min()
                .unwrap_or(target);
            self.state.active_elapsed_ms = next;
            self.update_phase(effects);
            if event_deadline == Some(next) {
                if let Some(event) = self.state.current_event.take() {
                    effects.push(SessionEffect::EndEvent { event_id: event.id });
                    if self.state.active_elapsed_ms < self.config.duration.active_duration_ms {
                        self.schedule_phase_events(effects);
                    }
                }
            }
        }
        self.update_phase(effects);
    }

    fn next_phase_boundary(&self) -> Option<u64> {
        let mut boundary = 0_u64;
        for phase in self
            .config
            .phases
            .iter()
            .take(self.config.phases.len().saturating_sub(1))
        {
            boundary = boundary.saturating_add(phase.duration_ms);
            if boundary > self.state.active_elapsed_ms {
                return Some(boundary);
            }
        }
        None
    }

    fn update_phase(&mut self, effects: &mut Vec<SessionEffect>) {
        let mut remaining = self.state.active_elapsed_ms;
        let mut index = 0;
        for (candidate, phase) in self.config.phases.iter().enumerate() {
            if remaining < phase.duration_ms || candidate + 1 == self.config.phases.len() {
                index = candidate;
                break;
            }
            remaining = remaining.saturating_sub(phase.duration_ms);
        }
        if index != self.state.phase.index {
            self.state.phase.index = index;
            self.state.phase.id = self.config.phases[index].id.clone();
            self.state.phase.elapsed_ms = remaining;
            self.state.statistics.phase_changes += 1;
            effects.push(SessionEffect::StartPhase {
                phase_id: self.state.phase.id.clone(),
            });
            // A configured event owns its whole active-time duration even if
            // phase eligibility changes underneath it.
            if self.state.current_event.is_none()
                && self.state.active_elapsed_ms < self.config.duration.active_duration_ms
            {
                self.schedule_phase_events(effects);
            }
        } else {
            self.state.phase.elapsed_ms = remaining;
        }
    }

    fn update_tempo(&mut self, elapsed_ms: u64, effects: &mut Vec<SessionEffect>) {
        let target = Self::target_tempo_for(
            &self.config,
            self.state.phase.index,
            self.state.active_elapsed_ms,
        );
        self.state.tempo.target_bpm = target;
        let smoothing =
            (self.config.tempo.smoothing_per_second * elapsed_ms as f64 / 1_000.0).clamp(0.0, 1.0);
        self.state.tempo.current_bpm += (target - self.state.tempo.current_bpm) * smoothing;
        if self.config.audio.metronome_enabled {
            effects.push(SessionEffect::ScheduleMetronome {
                target_bpm: self.state.tempo.current_bpm,
                meter: self.config.tempo.meter,
            });
        }
    }

    fn target_tempo_for(config: &GameConfig, phase_index: usize, elapsed_ms: u64) -> f64 {
        let progress = elapsed_ms as f64 / config.duration.active_duration_ms as f64;
        let phase = &config.phases[phase_index];
        (config.tempo.curve.target_bpm(progress)
            * config.difficulty.tempo_multiplier
            * phase.tempo_multiplier)
            .clamp(config.tempo.min_bpm, config.tempo.max_bpm)
    }

    fn schedule_phase_events(&mut self, effects: &mut Vec<SessionEffect>) {
        let phase = &self.config.phases[self.state.phase.index];
        let event_ids = phase.event_ids.clone();
        for event_id in event_ids {
            let Some(definition) = self.config.events.iter().find(|event| event.id == event_id)
            else {
                continue;
            };
            if !definition.allowed_phase_ids.is_empty()
                && !definition
                    .allowed_phase_ids
                    .iter()
                    .any(|id| id == &phase.id)
            {
                continue;
            }
            let count = self
                .state
                .event_history
                .iter()
                .filter(|record| record.id == definition.id)
                .count() as u16;
            let cooldown_met = self
                .state
                .event_history
                .iter()
                .rev()
                .find(|record| record.id == definition.id)
                .is_none_or(|record| {
                    self.state
                        .active_elapsed_ms
                        .saturating_sub(record.started_at_active_ms)
                        >= definition.cooldown_ms
                });
            let required = count < definition.minimum_occurrences;
            let selected = required
                || (count < definition.maximum_occurrences
                    && cooldown_met
                    && self.rng.gen_range(0..10_000_u16) < definition.probability_basis_points);
            if !selected {
                continue;
            }
            let record = EventRecord {
                id: definition.id.clone(),
                started_at_active_ms: self.state.active_elapsed_ms,
                duration_ms: definition.duration_ms,
            };
            self.state.current_event = Some(record.clone());
            self.state.event_history.push(record);
            self.state.statistics.events_started += 1;
            effects.push(SessionEffect::StartEvent {
                event_id: definition.id.clone(),
                duration_ms: definition.duration_ms,
            });
            if let Some(instruction) = &definition.instruction {
                self.state.instruction = Some(instruction.clone());
                effects.push(SessionEffect::ShowInstruction {
                    text: instruction.clone(),
                });
            }
            break;
        }
    }

    fn end(&mut self, status: SessionStatus, effects: &mut Vec<SessionEffect>) {
        if !matches!(
            self.state.status,
            SessionStatus::Running | SessionStatus::Paused
        ) {
            return;
        }
        self.state.status = status.clone();
        self.last_monotonic_ms = None;
        effects.push(SessionEffect::StopMusic);
        effects.push(SessionEffect::PersistCheckpoint);
        effects.push(SessionEffect::EndSession { status });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(seed: u64) -> GameConfig {
        GameConfig {
            schema_version: GAME_CONFIG_SCHEMA_VERSION,
            metadata: GameMetadata {
                name: "Test".into(),
                description: String::new(),
            },
            duration: EndCondition {
                active_duration_ms: 10_000,
            },
            difficulty: DifficultyConfig {
                tempo_multiplier: 1.0,
            },
            tempo: TempoConfig {
                curve: TempoCurve::Linear {
                    start_bpm: 60.0,
                    end_bpm: 120.0,
                },
                min_bpm: 40.0,
                max_bpm: 180.0,
                smoothing_per_second: 4.0,
                meter: 4,
            },
            media: MediaConfig {
                selection_strategy: SelectionStrategy::SeededRandom,
                required_tags: Vec::new(),
                excluded_tags: Vec::new(),
                allow_video_interruption: false,
            },
            phases: vec![
                PhaseConfig {
                    id: "warmup".into(),
                    duration_ms: 5_000,
                    tempo_multiplier: 1.0,
                    event_ids: vec!["cue".into()],
                },
                PhaseConfig {
                    id: "finish".into(),
                    duration_ms: 5_000,
                    tempo_multiplier: 1.25,
                    event_ids: Vec::new(),
                },
            ],
            events: vec![EventDefinition {
                id: "cue".into(),
                probability_basis_points: 10_000,
                minimum_occurrences: 0,
                maximum_occurrences: 1,
                cooldown_ms: 0,
                duration_ms: 1_000,
                priority: 1,
                allowed_phase_ids: vec!["warmup".into()],
                instruction: Some("Ready".into()),
            }],
            instructor: InstructorConfig {
                voice_pack_id: None,
                script_preset_id: None,
            },
            audio: AudioConfig {
                music_playlist_id: None,
                metronome_enabled: true,
                music_volume_percent: 80,
                voice_volume_percent: 80,
            },
            seed: Some(seed),
        }
    }

    #[test]
    fn paused_time_is_excluded_from_active_elapsed() {
        let mut engine = SessionEngine::new(config(7)).unwrap();
        engine.dispatch(SessionCommand::Start { monotonic_ms: 100 });
        engine.dispatch(SessionCommand::Tick {
            monotonic_ms: 1_100,
        });
        engine.dispatch(SessionCommand::Pause {
            monotonic_ms: 1_100,
        });
        engine.dispatch(SessionCommand::Resume {
            monotonic_ms: 9_100,
        });
        let update = engine.dispatch(SessionCommand::Tick {
            monotonic_ms: 10_100,
        });
        assert_eq!(update.state.active_elapsed_ms, 2_000);
        assert_eq!(update.state.paused_elapsed_ms, 8_000);
    }

    #[test]
    fn deterministic_seed_produces_the_same_effects() {
        let mut first = SessionEngine::new(config(9)).unwrap();
        let mut second = SessionEngine::new(config(9)).unwrap();
        assert_eq!(
            first.dispatch(SessionCommand::Start { monotonic_ms: 0 }),
            second.dispatch(SessionCommand::Start { monotonic_ms: 0 })
        );
        assert_eq!(
            first.dispatch(SessionCommand::Tick {
                monotonic_ms: 1_000
            }),
            second.dispatch(SessionCommand::Tick {
                monotonic_ms: 1_000
            })
        );
    }

    #[test]
    fn invalid_phase_and_event_references_fail_before_start() {
        let mut invalid = config(1);
        invalid.events[0].allowed_phase_ids = vec!["missing".into()];
        assert!(SessionEngine::new(invalid).is_err());

        let mut invalid = config(1);
        invalid.phases[0].event_ids = vec!["missing".into()];
        assert!(SessionEngine::new(invalid).is_err());
    }

    #[test]
    fn quick_mode_compiles_to_a_valid_versioned_game_config() {
        GameConfig::quick_default().validate().unwrap();
    }

    #[test]
    fn service_is_the_only_mutation_boundary_for_an_active_engine() {
        let service = SessionService::default();
        assert!(service.start(config(13)).is_ok());
        let update = service
            .dispatch(SessionCommand::Start { monotonic_ms: 1 })
            .unwrap();
        assert_eq!(update.state.status, SessionStatus::Running);
        assert!(service.start(config(14)).is_err());
        assert_eq!(
            service.interrupt_active().unwrap().state.status,
            SessionStatus::Interrupted
        );
    }

    #[test]
    fn tempo_offset_is_stable_across_repeated_pause_resume() {
        let mut engine = SessionEngine::new(config(21)).unwrap();
        engine.dispatch(SessionCommand::Start { monotonic_ms: 100 });
        engine.dispatch(SessionCommand::ChangeTempoOffset { offset_ms: 240 });
        engine.dispatch(SessionCommand::Pause {
            monotonic_ms: 1_100,
        });
        engine.dispatch(SessionCommand::Resume {
            monotonic_ms: 2_100,
        });
        engine.dispatch(SessionCommand::Pause {
            monotonic_ms: 3_100,
        });
        let update = engine.dispatch(SessionCommand::Resume {
            monotonic_ms: 7_100,
        });

        assert_eq!(update.state.tempo.offset_ms, 240);
        assert_eq!(update.state.active_elapsed_ms, 2_000);
        assert_eq!(update.state.paused_elapsed_ms, 5_000);
    }

    #[test]
    fn terminal_commands_settle_running_and_paused_intervals_once() {
        let mut running = SessionEngine::new(config(31)).unwrap();
        running.dispatch(SessionCommand::Start { monotonic_ms: 100 });
        let ended = running.dispatch(SessionCommand::End {
            completed: false,
            monotonic_ms: 1_100,
        });
        assert_eq!(ended.state.active_elapsed_ms, 1_000);
        assert_eq!(ended.state.status, SessionStatus::Cancelled);
        assert_eq!(
            ended
                .effects
                .iter()
                .filter(|effect| matches!(effect, SessionEffect::EndSession { .. }))
                .count(),
            1
        );
        let repeated = running.dispatch(SessionCommand::End {
            completed: false,
            monotonic_ms: 2_100,
        });
        assert_eq!(repeated.state.active_elapsed_ms, 1_000);
        assert!(repeated.effects.is_empty());

        let mut paused = SessionEngine::new(config(32)).unwrap();
        paused.dispatch(SessionCommand::Start { monotonic_ms: 100 });
        paused.dispatch(SessionCommand::Pause {
            monotonic_ms: 1_100,
        });
        let ended = paused.dispatch(SessionCommand::Interrupt {
            monotonic_ms: 9_100,
        });
        assert_eq!(ended.state.active_elapsed_ms, 1_000);
        assert_eq!(ended.state.paused_elapsed_ms, 8_000);
        assert_eq!(ended.state.status, SessionStatus::Interrupted);
    }

    #[test]
    fn delayed_ticks_emit_every_crossed_phase_in_order() {
        let mut cfg = config(41);
        cfg.phases = vec![
            PhaseConfig {
                id: "one".into(),
                duration_ms: 2_000,
                tempo_multiplier: 1.0,
                event_ids: vec![],
            },
            PhaseConfig {
                id: "two".into(),
                duration_ms: 2_000,
                tempo_multiplier: 1.0,
                event_ids: vec![],
            },
            PhaseConfig {
                id: "three".into(),
                duration_ms: 6_000,
                tempo_multiplier: 1.0,
                event_ids: vec![],
            },
        ];
        cfg.events.clear();
        let mut engine = SessionEngine::new(cfg).unwrap();
        engine.dispatch(SessionCommand::Start { monotonic_ms: 0 });
        let update = engine.dispatch(SessionCommand::Tick {
            monotonic_ms: 6_500,
        });
        let phases: Vec<_> = update
            .effects
            .iter()
            .filter_map(|effect| match effect {
                SessionEffect::StartPhase { phase_id } => Some(phase_id.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(phases, ["two", "three"]);
        assert_eq!(update.state.statistics.phase_changes, 2);
    }

    #[test]
    fn active_event_survives_phase_change_then_expires_at_its_deadline() {
        let mut cfg = config(51);
        cfg.events[0].duration_ms = 6_000;
        let mut engine = SessionEngine::new(cfg).unwrap();
        engine.dispatch(SessionCommand::Start { monotonic_ms: 0 });
        let boundary = engine.dispatch(SessionCommand::Tick {
            monotonic_ms: 5_000,
        });
        assert_eq!(boundary.state.phase.id, "finish");
        assert_eq!(
            boundary
                .state
                .current_event
                .as_ref()
                .map(|event| event.id.as_str()),
            Some("cue")
        );
        assert!(!boundary
            .effects
            .iter()
            .any(|effect| matches!(effect, SessionEffect::EndEvent { .. })));
        let expiry = engine.dispatch(SessionCommand::Tick {
            monotonic_ms: 6_000,
        });
        assert!(expiry.state.current_event.is_none());
        assert!(expiry.effects.iter().any(
            |effect| matches!(effect, SessionEffect::EndEvent { event_id } if event_id == "cue")
        ));
    }

    #[test]
    fn cooldown_uses_the_latest_matching_event_not_an_interleaved_event() {
        let mut cfg = config(61);
        cfg.phases[0].event_ids = vec!["a".into()];
        cfg.events = vec![EventDefinition {
            id: "a".into(),
            probability_basis_points: 10_000,
            minimum_occurrences: 0,
            maximum_occurrences: 3,
            cooldown_ms: 5_000,
            duration_ms: 1_000,
            priority: 1,
            allowed_phase_ids: vec!["warmup".into()],
            instruction: None,
        }];
        let mut engine = SessionEngine::new(cfg).unwrap();
        engine.state.active_elapsed_ms = 11_000;
        engine.state.event_history = vec![
            EventRecord {
                id: "a".into(),
                started_at_active_ms: 0,
                duration_ms: 1_000,
            },
            EventRecord {
                id: "other".into(),
                started_at_active_ms: 9_000,
                duration_ms: 1_000,
            },
        ];
        let mut effects = Vec::new();
        engine.schedule_phase_events(&mut effects);
        assert!(effects.iter().any(
            |effect| matches!(effect, SessionEffect::StartEvent { event_id, .. } if event_id == "a")
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn service_runner_completes_a_started_session_without_a_client_tick() {
        let clock = std::sync::Arc::new(AtomicU64::new(0));
        let service_clock = std::sync::Arc::clone(&clock);
        let service = SessionService::with_clock(move || service_clock.load(Ordering::SeqCst));
        let mut updates = service.subscribe();
        let mut cfg = config(71);
        cfg.duration.active_duration_ms = 100;
        cfg.phases[0].duration_ms = 100;
        cfg.phases.truncate(1);
        let started = service.start_running(cfg).unwrap();
        assert_eq!(started.state.status, SessionStatus::Running);
        assert_eq!(
            service.start_running(config(72)).unwrap_err(),
            "A session is already active"
        );
        assert_eq!(
            service.snapshot().unwrap().session_id,
            started.state.session_id
        );

        // Advance the scheduler and injected process clock together. No wall
        // clock sleep or client Tick participates in session completion.
        tokio::task::yield_now().await;
        clock.store(100, Ordering::SeqCst);
        tokio::time::advance(std::time::Duration::from_millis(100)).await;
        tokio::task::yield_now().await;

        let terminal = tokio::time::timeout(std::time::Duration::from_millis(1), async {
            loop {
                let update = updates.recv().await.unwrap();
                if update
                    .effects
                    .iter()
                    .any(|effect| matches!(effect, SessionEffect::EndSession { .. }))
                {
                    return update;
                }
            }
        })
        .await
        .expect("the authoritative runner should complete from injected time");
        assert_eq!(terminal.state.status, SessionStatus::Completed);
    }

    #[test]
    fn a_ready_session_cannot_be_replaced_before_its_start_command() {
        let service = SessionService::default();
        let first = service.start(config(81)).unwrap();
        assert_eq!(
            service.start(config(82)).unwrap_err(),
            "A session is already active"
        );
        assert_eq!(service.snapshot().unwrap().session_id, first.session_id);
    }
}
