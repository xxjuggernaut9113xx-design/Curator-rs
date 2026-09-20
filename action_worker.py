#!/usr/bin/env python3
"""Curator adapter for the pinned upstream P-HAR multi-model demo.

P-HAR is an MMAction2/MMPose/MMDetection application that combines RGB,
pose, and audio streams. It is *not* loaded as a generic TorchScript file.
This adapter owns only Curator's newline-delimited JSON protocol and invokes
the upstream ``multimodial_demo.py`` entrypoint inside its managed environment.

``runtime.json`` is written only after Curator's managed installer has checked
the pinned revision, checkpoint checksums, and a real inference probe. Until
then this worker reports ``ready:false`` and Curator leaves clips in manual
review while NudeNet continues independently.
"""
import argparse
import json
import subprocess
import sys
import tempfile
from pathlib import Path


UPSTREAM_REVISION = "94adf9900cd36360795709d920b44404f29bad3e"
RUNTIME_FORMAT = "curator-phar-runtime-v1"


def emit(value):
    print(json.dumps(value, separators=(",", ":")), flush=True)


def load_runtime(environment):
    marker = environment / "runtime.json"
    try:
        runtime = json.loads(marker.read_text(encoding="utf-8"))
    except Exception as error:
        raise RuntimeError(f"P-HAR runtime marker unavailable: {error}")
    if runtime.get("format") != RUNTIME_FORMAT:
        raise RuntimeError("P-HAR runtime marker format is not recognized")
    if runtime.get("upstream_revision") != UPSTREAM_REVISION:
        raise RuntimeError("P-HAR runtime is not the pinned upstream revision")
    if not runtime.get("checkpoints_verified"):
        raise RuntimeError("P-HAR checkpoint checksums are not verified")
    if not runtime.get("inference_probe_passed"):
        raise RuntimeError("P-HAR has not passed a real inference probe")
    if runtime.get("backend") not in {"cuda", "rocm"}:
        raise RuntimeError("P-HAR runtime does not declare a verified native CUDA or ROCm backend")
    if str(runtime.get("device") or "").lower().startswith("wsl"):
        raise RuntimeError("WSL is not a supported P-HAR execution backend")
    if not runtime.get("managed_python"):
        raise RuntimeError("P-HAR runtime does not declare its managed interpreter")
    upstream = environment / "upstream"
    demo = upstream / "src" / "demo" / "multimodial_demo.py"
    if not demo.is_file():
        raise RuntimeError("Pinned P-HAR upstream demo is missing")
    return runtime, upstream, demo


def dependency_probe(upstream):
    code = "import cv2,numpy,torch; import mmaction; import mmdet; import mmpose"
    result = subprocess.run(
        [sys.executable, "-c", code], cwd=str(upstream), text=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=45,
    )
    if result.returncode:
        detail = (result.stderr or result.stdout).strip().splitlines()[-1:]
        suffix = f": {detail[0]}" if detail else ""
        raise RuntimeError("P-HAR dependency probe failed" + suffix)


def score_for_label(prediction, label):
    if not isinstance(prediction, dict):
        return 0.0
    best = 0.0
    for raw_score, raw_label in prediction.items():
        if str(raw_label).strip().lower() != label.lower():
            continue
        try:
            best = max(best, float(raw_score))
        except (TypeError, ValueError):
            pass
    return max(0.0, min(1.0, best))


def parse_timestamp(value):
    start, end = str(value).split(":", 1)
    return float(start), float(end)


def overlaps(left_start, left_end, right_start, right_end):
    return left_start < right_end and right_start < left_end


def environment_workdir(upstream):
    # Keep temporary inference files under the managed P-HAR directory, not
    # the library or source video path. ``upstream`` is environment/upstream.
    workdir = upstream.parent / "work"
    workdir.mkdir(parents=True, exist_ok=True)
    return workdir


def predict(runtime, upstream, demo, path, requested_windows):
    # The upstream demo uses its training-compatible seven-second segments.
    # Its timestamp output is returned directly rather than manufacturing
    # labels from arbitrary frame tensors or a substitute TorchScript model.
    with tempfile.TemporaryDirectory(prefix="curator-phar-", dir=str(environment_workdir(upstream))) as work:
        output = Path(work) / "predictions.json"
        device = str(runtime.get("device") or "cuda:0")
        command = [
            sys.executable, str(demo), str(path), str(output), "--timestamps",
            "--num-processes", "1", "--subclip-len", "7", "--device", device,
        ]
        result = subprocess.run(
            command, cwd=str(upstream), text=True, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, timeout=60 * 30,
        )
        if result.returncode:
            detail = (result.stderr or result.stdout).strip().splitlines()[-1:]
            suffix = f": {detail[0]}" if detail else ""
            raise RuntimeError("P-HAR upstream inference failed" + suffix)
        timestamps_path = output.with_name(output.stem + "_ts.json")
        predictions = json.loads(output.read_text(encoding="utf-8"))
        timestamps = json.loads(timestamps_path.read_text(encoding="utf-8"))

    windows = []
    for index, (range_text, label) in enumerate(timestamps.items()):
        start, end = parse_timestamp(range_text)
        if requested_windows and not any(
            overlaps(start, end, wanted[0], wanted[1]) for wanted in requested_windows
        ):
            continue
        score = score_for_label(predictions[index] if index < len(predictions) else {}, str(label))
        windows.append({"start_secs": start, "end_secs": end, "label": str(label), "score": score})
    if not windows:
        raise RuntimeError("P-HAR produced no temporal windows for this clip")
    return {
        "model": "P-HAR upstream multimodal",
        "version": UPSTREAM_REVISION,
        "windows": windows,
    }


def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--probe", action="store_true")
    args = parser.parse_args()
    environment = Path(args.environment).resolve()
    try:
        runtime, upstream, demo = load_runtime(environment)
        dependency_probe(upstream)
    except Exception as error:
        emit({"ready": False, "error": str(error)})
        return 1

    if args.probe:
        emit({"ready": True, "model": "P-HAR upstream multimodal", "version": UPSTREAM_REVISION})
        return 0

    emit({"ready": True, "model": "P-HAR upstream multimodal", "version": UPSTREAM_REVISION})
    for raw_line in sys.stdin:
        raw_line = raw_line.strip()
        if not raw_line:
            continue
        request_id = None
        try:
            request = json.loads(raw_line)
            request_id = request.get("id")
            path = Path(request["path"])
            requested_windows = request.get("windows")
            if not path.is_file() or not isinstance(requested_windows, list):
                raise ValueError("path and windows are required")
            windows = []
            for window in requested_windows[:64]:
                start, end = float(window[0]), float(window[1])
                if not (0 <= start < end):
                    raise ValueError("invalid temporal window")
                windows.append((start, end))
            if not windows:
                raise ValueError("at least one temporal window is required")
            emit({"id": request_id, "result": predict(runtime, upstream, demo, path, windows)})
        except Exception as error:
            emit({"id": request_id, "error": str(error)})
    return 0


if __name__ == "__main__":
    sys.exit(main())
