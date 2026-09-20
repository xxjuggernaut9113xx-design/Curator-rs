//! Native, managed P-HAR installation state.
//!
//! P-HAR is deliberately isolated below `data_dir/phar`. This module never
//! accepts WSL or an arbitrary Python executable. State is durable JSON, so a
//! Local Admin request can resume safely after Curator restarts.

use crate::edition::InstallScope;
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
    thread,
};

pub const MANIFEST_FORMAT: &str = "curator-phar-manifest-v2";
pub const UPSTREAM_REPOSITORY: &str = "https://github.com/rlleshi/phar.git";
pub const UPSTREAM_REVISION: &str = "94adf9900cd36360795709d920b44404f29bad3e";
const STATE_FILE: &str = "state.json";
const MANIFEST_FILE: &str = "manifest.json";
const RUNTIME_FILE: &str = "runtime.json";
const CANCEL_FILE: &str = "cancel.requested";
static JOB_SEQUENCE: AtomicU64 = AtomicU64::new(1);

/// `auto` prefers compatible CUDA and then compatible ROCm. WSL is accepted
/// only as an old config migration value and is never a runtime selection.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PharBackend {
    Auto,
    Cuda,
    Rocm,
}
impl PharBackend {
    pub fn parse(value: Option<&str>) -> Self {
        match value.map(str::trim).map(str::to_ascii_lowercase).as_deref() {
            Some("cuda") => Self::Cuda,
            Some("rocm") => Self::Rocm,
            _ => Self::Auto,
        }
    }
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Cuda => "cuda",
            Self::Rocm => "rocm",
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PharSupportTier {
    Supported,
    Unavailable,
}
#[derive(Debug, Clone, Serialize)]
pub struct PharSupport {
    pub tier: PharSupportTier,
    pub backend: &'static str,
    pub detail: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DetectedGpu {
    pub backend: PharBackend,
    pub name: String,
    pub driver: Option<String>,
    pub compatible: bool,
    pub detail: String,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Platform {
    Linux,
    Windows,
    MacOs,
    Other,
}
pub const fn current_platform() -> Platform {
    #[cfg(target_os = "linux")]
    {
        Platform::Linux
    }
    #[cfg(target_os = "windows")]
    {
        Platform::Windows
    }
    #[cfg(target_os = "macos")]
    {
        Platform::MacOs
    }
    #[cfg(not(any(target_os = "linux", target_os = "windows", target_os = "macos")))]
    {
        Platform::Other
    }
}
pub const fn default_backend(_: Platform) -> PharBackend {
    PharBackend::Auto
}
pub fn support_for(platform: Platform, backend: PharBackend) -> PharSupport {
    let backend = backend.as_str();
    match platform {
        Platform::Windows | Platform::Linux => PharSupport {
            tier: PharSupportTier::Supported,
            backend,
            detail: format!(
                "Native P-HAR uses {backend} GPU selection; auto prefers CUDA before ROCm."
            ),
        },
        Platform::MacOs => PharSupport {
            tier: PharSupportTier::Unavailable,
            backend,
            detail: "Native P-HAR CUDA/ROCm installation is unavailable on macOS.".into(),
        },
        Platform::Other => PharSupport {
            tier: PharSupportTier::Unavailable,
            backend,
            detail: "Native P-HAR is not supported on this platform.".into(),
        },
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct PharManifest {
    pub format: &'static str,
    pub upstream_repository: &'static str,
    pub upstream_revision: &'static str,
    pub source_archive: &'static str,
    pub checkpoints: Vec<CheckpointManifest>,
}
#[derive(Debug, Clone, Serialize)]
pub struct CheckpointManifest {
    pub id: &'static str,
    pub source_url: &'static str,
    pub bytes: Option<u64>,
    pub sha256: Option<&'static str>,
    pub license: &'static str,
}
/// Publisher hash data is deliberately required before any checkpoint transfer.
pub fn pinned_manifest() -> PharManifest {
    PharManifest { format: MANIFEST_FORMAT, upstream_repository: UPSTREAM_REPOSITORY, upstream_revision: UPSTREAM_REVISION, source_archive: "https://github.com/rlleshi/phar/archive/94adf9900cd36360795709d920b44404f29bad3e.tar.gz", checkpoints: vec![
        CheckpointManifest { id: "p-har-audio", source_url: "https://github.com/rlleshi/phar/releases/download/v1.0.0/audio.pth", bytes: Some(85_800_759), sha256: None, license: "P-HAR v1.0.0 publisher terms; SHA-256 not published" },
        CheckpointManifest { id: "p-har-posec3d", source_url: "https://github.com/rlleshi/phar/releases/download/v1.0.0/posec3d.pth", bytes: Some(16_168_879), sha256: None, license: "P-HAR v1.0.0 publisher terms; SHA-256 not published" },
        CheckpointManifest { id: "p-har-timesformer", source_url: "https://github.com/rlleshi/phar/releases/download/v1.0.0/timeSformer.pth", bytes: Some(970_440_477), sha256: None, license: "P-HAR v1.0.0 publisher terms; SHA-256 not published" },
        CheckpointManifest { id: "openmmlab-human-detector", source_url: "http://download.openmmlab.com/mmdetection/v2.0/faster_rcnn/faster_rcnn_r50_fpn_2x_coco/faster_rcnn_r50_fpn_2x_coco_bbox_mAP-0.384_20200504_210434-a5d8aa15.pth", bytes: None, sha256: None, license: "OpenMMLab publisher terms; SHA-256 not published" },
        CheckpointManifest { id: "openmmlab-pose", source_url: "https://download.openmmlab.com/mmpose/top_down/hrnet/hrnet_w32_coco_256x192-c78dce93_20200708.pth", bytes: None, sha256: None, license: "OpenMMLab publisher terms; SHA-256 not published" },
    ] }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PharPhase {
    Disabled,
    Requested,
    Detecting,
    ProvisioningRuntime,
    FetchingSource,
    InstallingDependencies,
    DownloadingModels,
    Verifying,
    Probing,
    Ready,
    Failed,
    Cancelled,
}
#[derive(Debug, Clone, Serialize)]
pub struct PharStatus {
    pub requested: bool,
    pub ready: bool,
    pub phase: PharPhase,
    pub progress_percent: u8,
    pub backend: PharBackend,
    pub detected_gpu: Option<DetectedGpu>,
    pub support: PharSupport,
    pub message: String,
    pub actionable_error: Option<String>,
    pub job_id: Option<String>,
    pub repair_required: bool,
    pub manifest_revision: &'static str,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
struct PersistedState {
    phase: PharPhase,
    progress_percent: u8,
    message: String,
    #[serde(default)]
    actionable_error: Option<String>,
    #[serde(default)]
    detected_gpu: Option<DetectedGpu>,
    #[serde(default)]
    job_id: Option<String>,
    #[serde(default)]
    repair_required: bool,
}

pub fn environment_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("phar")
}
fn staging_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("phar.staging")
}
fn manifest_path(data_dir: &Path) -> PathBuf {
    environment_dir(data_dir).join(MANIFEST_FILE)
}
fn state_path(data_dir: &Path) -> PathBuf {
    environment_dir(data_dir).join(STATE_FILE)
}
fn cancel_path(data_dir: &Path) -> PathBuf {
    environment_dir(data_dir).join(CANCEL_FILE)
}

pub fn ensure_manifest(data_dir: &Path) -> Result<()> {
    let path = manifest_path(data_dir);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, serde_json::to_string_pretty(&pinned_manifest())?)?;
    Ok(())
}

/// Normalizes a legacy `phar_runtime: wsl2` config immediately to `auto`.
fn configured_backend(scope: InstallScope) -> PharBackend {
    let mut cfg = crate::config::load_config_for(scope);
    let legacy = cfg.phar_runtime.take();
    let selected = cfg.phar_backend.clone().or(legacy.clone());
    let backend = PharBackend::parse(selected.as_deref());
    if cfg.phar_backend.as_deref() != Some(backend.as_str()) || legacy.is_some() {
        cfg.phar_backend = Some(backend.as_str().into());
        let _ = crate::config::save_config_for(scope, &cfg);
    }
    backend
}
fn read_state(data_dir: &Path) -> Option<PersistedState> {
    std::fs::read_to_string(state_path(data_dir))
        .ok()
        .and_then(|v| serde_json::from_str(&v).ok())
}
fn write_state(data_dir: &Path, state: PersistedState) -> Result<()> {
    ensure_manifest(data_dir)?;
    let path = state_path(data_dir);
    let temporary = path.with_extension("json.new");
    std::fs::write(&temporary, serde_json::to_vec_pretty(&state)?)?;
    std::fs::rename(temporary, path)?;
    Ok(())
}
fn new_state(phase: PharPhase, percent: u8, message: impl Into<String>) -> PersistedState {
    PersistedState {
        phase,
        progress_percent: percent,
        message: message.into(),
        actionable_error: None,
        detected_gpu: None,
        job_id: None,
        repair_required: false,
    }
}

pub fn status(data_dir: &Path, scope: InstallScope) -> PharStatus {
    let cfg = crate::config::load_config_for(scope);
    let legacy_wsl = cfg
        .phar_runtime
        .as_deref()
        .is_some_and(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "wsl" | "wsl2"));
    let backend = configured_backend(scope);
    let support = support_for(current_platform(), backend);
    let mut current = read_state(data_dir).unwrap_or_else(|| {
        if cfg.phar_setup_requested {
            new_state(
                PharPhase::Requested,
                0,
                "P-HAR setup was requested; choose Install / resume when ready.",
            )
        } else {
            new_state(
                PharPhase::Disabled,
                0,
                "P-HAR is opt-in and currently disabled.",
            )
        }
    });
    if legacy_wsl {
        current.repair_required = true;
        current.message = "Legacy WSL P-HAR preference migrated to native auto-detection. Run Repair to validate native files; old files stay until explicit cleanup.".into();
        let _ = write_state(data_dir, current.clone());
    }
    let ready = current.phase == PharPhase::Ready && runtime_is_verified(data_dir);
    if current.phase == PharPhase::Ready && !ready {
        current.phase = PharPhase::Failed;
        current.repair_required = true;
        current.actionable_error =
            Some("Ready marker is incomplete or not using a managed P-HAR interpreter.".into());
    }
    PharStatus {
        requested: cfg.phar_setup_requested,
        ready,
        phase: current.phase,
        progress_percent: current.progress_percent,
        backend,
        detected_gpu: current.detected_gpu,
        support,
        message: current.message,
        actionable_error: current.actionable_error,
        job_id: current.job_id,
        repair_required: current.repair_required,
        manifest_revision: UPSTREAM_REVISION,
    }
}

pub fn record_install_intent(
    data_dir: &Path,
    scope: InstallScope,
    enabled: bool,
    backend: Option<PharBackend>,
) -> Result<PharStatus> {
    let mut cfg = crate::config::load_config_for(scope);
    cfg.phar_setup_requested = enabled;
    cfg.phar_runtime = None;
    if let Some(backend) = backend {
        cfg.phar_backend = Some(backend.as_str().into());
    }
    if cfg.phar_backend.is_none() {
        cfg.phar_backend = Some(default_backend(current_platform()).as_str().into());
    }
    crate::config::save_config_for(scope, &cfg)?;
    let next = if enabled {
        new_state(PharPhase::Requested, 0, "P-HAR setup requested. Installation runs after Curator starts, outside the OS installer.")
    } else {
        new_state(
            PharPhase::Disabled,
            0,
            "P-HAR is disabled. Managed files remain until explicit P-HAR cleanup.",
        )
    };
    write_state(data_dir, next)?;
    Ok(status(data_dir, scope))
}

/// Starts/resumes the durable setup job and returns its status without waiting.
pub fn start_install(data_dir: &Path, scope: InstallScope) -> Result<PharStatus> {
    let current = status(data_dir, scope);
    if !current.requested || current.support.tier == PharSupportTier::Unavailable {
        return Ok(current);
    }
    if matches!(current.phase, PharPhase::Cancelled | PharPhase::Ready) {
        return Ok(current);
    }
    if matches!(
        current.phase,
        PharPhase::Detecting
            | PharPhase::ProvisioningRuntime
            | PharPhase::FetchingSource
            | PharPhase::InstallingDependencies
            | PharPhase::DownloadingModels
            | PharPhase::Verifying
            | PharPhase::Probing
    ) {
        return Ok(current);
    }
    let _ = std::fs::remove_file(cancel_path(data_dir));
    let mut initial = new_state(
        PharPhase::Detecting,
        2,
        "Detecting native CUDA and ROCm hardware.",
    );
    initial.job_id = Some(format!(
        "phar-{}",
        JOB_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    write_state(data_dir, initial)?;
    let owned = data_dir.to_path_buf();
    thread::Builder::new()
        .name("curator-phar-install".into())
        .spawn(move || run_install(&owned, scope))
        .map_err(|error| anyhow!(error))?;
    Ok(status(data_dir, scope))
}
pub fn resume_requested_setup(data_dir: &Path, scope: InstallScope) -> Result<PharStatus> {
    start_install(data_dir, scope)
}
fn cancelled(data_dir: &Path) -> bool {
    cancel_path(data_dir).is_file()
}
fn cancelled_state(data_dir: &Path, gpu: Option<DetectedGpu>) {
    let mut state = new_state(
        PharPhase::Cancelled,
        0,
        "P-HAR setup was cancelled. Staged data is retained for resume or Repair.",
    );
    state.detected_gpu = gpu;
    let _ = write_state(data_dir, state);
}
fn fail(data_dir: &Path, _phase: PharPhase, gpu: Option<DetectedGpu>, message: impl Into<String>) {
    let error = message.into();
    let mut state = new_state(PharPhase::Failed, 0, error.clone());
    state.detected_gpu = gpu;
    state.actionable_error = Some(error);
    state.repair_required = true;
    let _ = write_state(data_dir, state);
}
fn run_install(data_dir: &Path, scope: InstallScope) {
    let requested = configured_backend(scope);
    let gpu = match detect_gpu(requested) {
        Ok(gpu) => gpu,
        Err(error) => {
            fail(data_dir, PharPhase::Detecting, None, error.to_string());
            return;
        }
    };
    if cancelled(data_dir) {
        cancelled_state(data_dir, Some(gpu));
        return;
    }
    let mut state = new_state(
        PharPhase::ProvisioningRuntime,
        12,
        format!(
            "Selected {} on {}. Provisioning private native runtime.",
            gpu.backend.as_str(),
            gpu.name
        ),
    );
    state.detected_gpu = Some(gpu.clone());
    if write_state(data_dir, state).is_err() {
        return;
    }
    if let Err(error) = std::fs::create_dir_all(staging_dir(data_dir)) {
        fail(
            data_dir,
            PharPhase::ProvisioningRuntime,
            Some(gpu),
            error.to_string(),
        );
        return;
    }
    if cancelled(data_dir) {
        cancelled_state(data_dir, Some(gpu));
        return;
    }
    // Do not transfer a checkpoint until every publisher-provided artifact
    // record has a URL, license, size, and fixed SHA-256.
    if pinned_manifest()
        .checkpoints
        .iter()
        .any(|entry| entry.sha256.is_none())
    {
        fail(data_dir, PharPhase::DownloadingModels, Some(gpu), "P-HAR publisher metadata is incomplete: a checkpoint SHA-256 is missing. No model was downloaded. Add verified upstream URL, license, size, and checksum metadata, then use Repair.");
        return;
    }
    fail(
        data_dir,
        PharPhase::Verifying,
        Some(gpu),
        "Verified native P-HAR artifact installation is not available in this build.",
    );
}

pub fn cancel(data_dir: &Path, scope: InstallScope) -> Result<PharStatus> {
    let current = status(data_dir, scope);
    if current.requested {
        ensure_manifest(data_dir)?;
        std::fs::write(cancel_path(data_dir), b"cancel\n")?;
        cancelled_state(data_dir, current.detected_gpu);
    }
    Ok(status(data_dir, scope))
}
pub fn disable(data_dir: &Path, scope: InstallScope) -> Result<PharStatus> {
    record_install_intent(data_dir, scope, false, None)
}
pub fn repair(data_dir: &Path, scope: InstallScope) -> Result<PharStatus> {
    record_install_intent(data_dir, scope, true, None)?;
    start_install(data_dir, scope)
}
pub fn self_test(data_dir: &Path, scope: InstallScope) -> Result<PharStatus> {
    let current = status(data_dir, scope);
    if current.requested && !runtime_is_verified(data_dir) {
        fail(data_dir, PharPhase::Failed, current.detected_gpu, "P-HAR self-test failed: no verified native runtime and inference probe are available. Run Repair after resolving the reported setup error.");
    }
    Ok(status(data_dir, scope))
}

/// The worker can only be launched with an interpreter contained in this
/// managed environment, never the configured general-purpose `python_bin`.
pub fn managed_python(data_dir: &Path) -> Option<PathBuf> {
    #[derive(Deserialize)]
    struct Marker {
        managed_python: String,
    }
    let text = std::fs::read_to_string(environment_dir(data_dir).join(RUNTIME_FILE)).ok()?;
    let marker: Marker = serde_json::from_str(&text).ok()?;
    let path = PathBuf::from(marker.managed_python);
    let root = environment_dir(data_dir).canonicalize().ok()?;
    let candidate = path.canonicalize().ok()?;
    (candidate.starts_with(root) && candidate.is_file()).then_some(candidate)
}
pub fn runtime_is_verified(data_dir: &Path) -> bool {
    #[derive(Deserialize)]
    struct Marker {
        format: String,
        upstream_revision: String,
        backend: PharBackend,
        checkpoints_verified: bool,
        inference_probe_passed: bool,
        managed_python: String,
        artifact_hashes: serde_json::Value,
    }
    let marker = std::fs::read_to_string(environment_dir(data_dir).join(RUNTIME_FILE))
        .ok()
        .and_then(|v| serde_json::from_str::<Marker>(&v).ok());
    let Some(marker) = marker else {
        return false;
    };
    marker.format == "curator-phar-runtime-v1"
        && marker.upstream_revision == UPSTREAM_REVISION
        && matches!(marker.backend, PharBackend::Cuda | PharBackend::Rocm)
        && marker.checkpoints_verified
        && marker.inference_probe_passed
        && !marker.managed_python.is_empty()
        && marker.artifact_hashes.is_object()
        && managed_python(data_dir).is_some()
}

fn command_output(program: &str, args: &[&str]) -> Option<String> {
    Command::new(program)
        .args(args)
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_owned())
}
/// Pure selection is exposed for deterministic hardware-free tests.
pub fn select_backend(
    requested: PharBackend,
    cuda: Option<DetectedGpu>,
    rocm: Option<DetectedGpu>,
) -> Result<DetectedGpu> {
    let compatible = |gpu: Option<DetectedGpu>| gpu.filter(|gpu| gpu.compatible);
    match requested {
        PharBackend::Cuda => compatible(cuda).ok_or_else(|| anyhow!("CUDA was requested, but no compatible NVIDIA GPU/driver was detected. Update the NVIDIA driver or choose ROCm only for an AMD GPU supported by AMD's Windows matrix.")),
        PharBackend::Rocm => compatible(rocm).ok_or_else(|| anyhow!("ROCm was requested, but no supported AMD GPU/driver was detected. Install an AMD-published Windows ROCm driver or choose CUDA.")),
        PharBackend::Auto => compatible(cuda).or_else(|| compatible(rocm)).ok_or_else(|| anyhow!("No compatible native CUDA or ROCm GPU was detected. P-HAR remains disabled; NudeNet and manual review are still available.")),
    }
}
fn detect_gpu(requested: PharBackend) -> Result<DetectedGpu> {
    if let Ok(fake) = std::env::var("CURATOR_PHAR_TEST_GPU") {
        let fake = fake.to_ascii_lowercase();
        let cuda = (fake == "cuda").then(|| DetectedGpu {
            backend: PharBackend::Cuda,
            name: "test NVIDIA GPU".into(),
            driver: Some("test".into()),
            compatible: true,
            detail: "test probe".into(),
        });
        let rocm = (fake == "rocm").then(|| DetectedGpu {
            backend: PharBackend::Rocm,
            name: "test AMD GPU".into(),
            driver: Some("test".into()),
            compatible: true,
            detail: "test probe".into(),
        });
        return select_backend(requested, cuda, rocm);
    }
    let cuda = command_output(
        "nvidia-smi",
        &["--query-gpu=name,driver_version", "--format=csv,noheader"],
    )
    .and_then(|out| {
        out.lines().next().map(|line| {
            let mut fields = line.split(',').map(str::trim);
            DetectedGpu {
                backend: PharBackend::Cuda,
                name: fields.next().unwrap_or("NVIDIA GPU").into(),
                driver: fields.next().map(str::to_owned),
                compatible: true,
                detail: "NVIDIA driver detected by nvidia-smi.".into(),
            }
        })
    });
    let rocm = command_output("wmic", &["path", "win32_VideoController", "get", "Name,DriverVersion", "/format:csv"]).and_then(|out| out.lines().find(|line| line.to_ascii_lowercase().contains("amd") || line.to_ascii_lowercase().contains("radeon")).map(|line| DetectedGpu { backend: PharBackend::Rocm, name: line.trim().into(), driver: None, compatible: false, detail: "AMD adapter found; ROCm compatibility requires AMD's published Windows matrix and provisioning probe.".into() }));
    select_backend(requested, cuda, rocm)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn gpu(backend: PharBackend) -> DetectedGpu {
        DetectedGpu {
            backend,
            name: "GPU".into(),
            driver: None,
            compatible: true,
            detail: String::new(),
        }
    }
    #[test]
    fn auto_prefers_cuda() {
        assert_eq!(
            select_backend(
                PharBackend::Auto,
                Some(gpu(PharBackend::Cuda)),
                Some(gpu(PharBackend::Rocm))
            )
            .unwrap()
            .backend,
            PharBackend::Cuda
        );
    }
    #[test]
    fn explicit_backend_does_not_fallback() {
        assert!(select_backend(PharBackend::Cuda, None, Some(gpu(PharBackend::Rocm))).is_err());
    }
    #[test]
    fn legacy_wsl_maps_to_auto() {
        assert_eq!(PharBackend::parse(Some("wsl2")), PharBackend::Auto);
    }
    #[test]
    fn saved_wsl_intent_migrates_to_auto_and_requires_repair() {
        let _guard = crate::PROCESS_ENV_LOCK.lock().unwrap();
        let data = tempfile::tempdir().unwrap();
        std::env::set_var("CURATOR_CONFIG_DIR", data.path());
        let legacy = crate::config::Config {
            phar_setup_requested: true,
            phar_runtime: Some("wsl2".into()),
            ..Default::default()
        };
        crate::config::save_config_for(InstallScope::CurrentUser, &legacy).unwrap();
        let current = status(data.path(), InstallScope::CurrentUser);
        let migrated = crate::config::load_config_for(InstallScope::CurrentUser);
        std::env::remove_var("CURATOR_CONFIG_DIR");
        assert_eq!(current.backend, PharBackend::Auto);
        assert!(current.repair_required);
        assert_eq!(migrated.phar_backend.as_deref(), Some("auto"));
        assert!(migrated.phar_runtime.is_none());
    }
}
