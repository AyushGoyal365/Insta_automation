"""
Template-2 Reel Editing Pipeline (R2 -> FFmpeg -> R2), bucket: reel-dump-b2.

Unlike video_editor.py (which composites a channel image over a raw clip),
this pipeline takes a single continuous raw shot and turns it into a
retention-optimized short: normalize -> trim to length -> hook text overlay
-> punch-in cut rhythm -> seamless loop -> loudness-normalized audio -> export.
Every stage is independently toggleable via t2_config.json / config_overrides.

Bucket layout expected:
    Raw/Video/{YYYY-MM-DD}/*.mp4             <- raw single-shot videos, date-wise (shared pool)
Outputs written:
    processed/{channel}/{date}/{video}.mp4   <- edited reel
    processed/{channel}/{date}/{video}.json  <- manifest (hook used, loop point, config snapshot)
    video_logs/{channel}/{date}/{video}.done <- edit log (skip if exists)
State kept in-bucket:
    t2_state/hook_index/{category}.txt       <- round-robin cursor per hook category

Each run edits at most DEFAULT_EDIT_LIMIT (20) videos, so one invocation --
local, scheduled, or a manual workflow_dispatch -- can't accidentally chew
through the whole raw backlog. Override with the T2_MAX_EDITS env var or an
explicit `limit` arg for a deliberately larger batch.

Usage:
    pip install boto3 Pillow imagehash numpy pyyaml
    Set the env vars below, then:  python video_edit_t2.py                    (every date, up to 20 edits)
                                   python video_edit_t2.py 2026-07-21         (one date, up to 20 edits)
                                   python video_edit_t2.py 2026-07-21 50      (one date, up to 50 edits)
Requires ffmpeg (with fontconfig/freetype) installed on the machine.
"""

import copy
import json
import math
import os
import random
import re
import subprocess
import sys
import tempfile

import boto3
import imagehash
import yaml
from PIL import Image, ImageFont

from env_local import load_dotenv

load_dotenv()

# ---------- CONFIG via environment variables (set in .env locally, or real env vars / secrets in CI) ----------
R2_ENDPOINT = os.environ["R2_ENDPOINT"]
R2_ACCESS_KEY = os.environ["R2_ACCESS_KEY"]
R2_SECRET_KEY = os.environ["R2_SECRET_KEY"]
BUCKET = os.environ.get("R2_BUCKET_T2", "reel-dump-b2")  # default bucket name
# -----------------------------------------------------------------------------------------------------------

VIDEO_EXTS = (".mp4", ".mov", ".mkv", ".webm", ".m4v")
DEFAULT_EDIT_LIMIT = 20  # per-run cap on (video, channel) edits; see run()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CONFIG_PATH = os.path.join(BASE_DIR, "t2_config.json")
HOOKS_PATH = os.path.join(BASE_DIR, "hooks.yaml")

s3 = boto3.client(
    "s3",
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY,
    region_name="auto",
)


# ---------------------------------------------------------------------------
# R2 helpers (mirrors video_editor.py conventions)
# ---------------------------------------------------------------------------

def list_keys(prefix: str) -> list[str]:
    """List all object keys under a prefix (handles pagination)."""
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def get_raw_videos(day: str) -> list[str]:
    """Return keys of raw videos for the given date."""
    return [k for k in list_keys(f"Raw/Video/{day}/") if k.lower().endswith(VIDEO_EXTS)]


def get_all_dates() -> list[str]:
    """Return every date folder under Raw/Video/ (e.g. ['2026-07-21', '2026-07-22'])."""
    dates = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix="Raw/Video/", Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            day = cp["Prefix"][len("Raw/Video/"):].rstrip("/")
            if day:
                dates.append(day)
    return sorted(dates)


def key_exists(key: str) -> bool:
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return True
    except s3.exceptions.ClientError:
        return False


def touch_key(key: str, body: bytes = b"ok") -> None:
    s3.put_object(Bucket=BUCKET, Key=key, Body=body)


def get_text(key: str, default: str = "") -> str:
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return obj["Body"].read().decode("utf-8").strip()
    except s3.exceptions.ClientError:
        return default


def already_done(day: str, channel: str, video_name: str) -> bool:
    return key_exists(f"video_logs/{channel}/{day}/{video_name}.done")


def mark_done(day: str, channel: str, video_name: str) -> None:
    touch_key(f"video_logs/{channel}/{day}/{video_name}.done")


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def load_default_config() -> dict:
    with open(DEFAULT_CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def resolve_config(defaults: dict, channel: dict) -> dict:
    """Merge global t2_config.json defaults with a channel's config_overrides."""
    cfg = copy.deepcopy(defaults)
    cfg.update(channel.get("config_overrides", {}) or {})
    if "hook_category" in channel:
        cfg["hook_category"] = channel["hook_category"]
    return cfg


def load_hooks() -> dict[str, list[str]]:
    with open(HOOKS_PATH, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return {k: list(v) for k, v in data.items()}


def next_hook(category: str, hooks_by_category: dict[str, list[str]]) -> tuple[str, str]:
    """Round-robin the next hook string within `category`. Cursor persisted in R2."""
    pool = hooks_by_category.get(category) or []
    if not pool:
        return "", category
    state_key = f"t2_state/hook_index/{category}.txt"
    idx = int(get_text(state_key, "0") or "0")
    hook = pool[idx % len(pool)]
    touch_key(state_key, str(idx + 1).encode("utf-8"))
    return hook, category


# ---------------------------------------------------------------------------
# ffprobe / ffmpeg helpers
# ---------------------------------------------------------------------------

def probe(path: str) -> dict:
    cmd = [
        "ffprobe", "-v", "quiet", "-print_format", "json",
        "-show_format", "-show_streams", path,
    ]
    out = subprocess.run(cmd, check=True, capture_output=True, text=True).stdout
    return json.loads(out)


def probe_duration(info: dict) -> float:
    return float(info["format"]["duration"])


def probe_video_stream(info: dict) -> dict:
    for s in info["streams"]:
        if s["codec_type"] == "video":
            return s
    raise RuntimeError("No video stream found")


def probe_fps(video_stream: dict) -> float:
    num, den = video_stream.get("r_frame_rate", "30/1").split("/")
    den = float(den) or 1.0
    return float(num) / den


def has_audio_stream(info: dict) -> bool:
    return any(s["codec_type"] == "audio" for s in info["streams"])


def ffmpeg_escape_text(text: str) -> str:
    """Escape a string for safe use inside an ffmpeg drawtext `text=` value."""
    text = text.replace("\\", "\\\\")
    text = text.replace(":", "\\:")
    text = text.replace("'", "’")  # avoid unbalanced quoting inside the filtergraph
    text = text.replace("%", "\\%")
    return text


def ffmpeg_escape_path(path: str) -> str:
    """Quote+escape a filesystem path for use as an ffmpeg filter option value.

    Needed on Windows where drive letters ("D:") contain the ':' that ffmpeg's
    filtergraph parser otherwise treats as an option separator -- empirically,
    on this ffmpeg build (8.1.2) the colon must be BOTH wrapped in single
    quotes AND backslash-escaped; either alone is rejected with "No option
    name near ...".
    """
    return "'" + path.replace("\\", "/").replace(":", "\\:") + "'"


# Bold sans-serif ttf files to fall back on, in order, when hook_font_path
# isn't set to a real file. ffmpeg's fontconfig-name lookup (font=) is not
# used here -- on a machine with no fontconfig config file present (common on
# Windows) it segfaults ffmpeg outright rather than failing gracefully, so
# drawtext is always driven from an explicit font FILE.
SYSTEM_FONT_CANDIDATES = [
    r"C:\Windows\Fonts\arialbd.ttf",
    r"C:\Windows\Fonts\seguisb.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
    "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
]


def resolve_font_file(cfg: dict) -> str:
    path = cfg.get("hook_font_path", "")
    if path and not os.path.isabs(path):
        path = os.path.join(BASE_DIR, path)
    if path and os.path.exists(path):
        return path
    for candidate in SYSTEM_FONT_CANDIDATES:
        if os.path.exists(candidate):
            return candidate
    raise RuntimeError(
        "No bold sans-serif font file found for the hook text overlay. "
        "Set hook_font_path in t2_config.json, or drop a .ttf at fonts/hook_font.ttf."
    )


# ---------------------------------------------------------------------------
# Stage 1+2: normalize + trim-to-length
# ---------------------------------------------------------------------------

def build_normalize_and_trim(raw_path: str, out_path: str, cfg: dict, src_info: dict) -> float:
    """Scale/crop to 1080x1920, cap fps, strip metadata, and trim/loop to target length.

    Returns the resulting output duration in seconds.
    """
    src_dur = probe_duration(src_info)
    vstream = probe_video_stream(src_info)
    src_fps = probe_fps(vstream)

    input_args = []
    ss = None
    out_dur = src_dur

    if cfg["enable_trim"]:
        target_min = cfg["target_min_s"]
        target_max = cfg["target_max_s"]
        if src_dur > target_max:
            ss = cfg.get("hook_start_offset_s", 0)
            out_dur = min(target_max - ss, src_dur - ss)
        elif src_dur < target_min:
            loops_needed = math.ceil(target_min / src_dur)
            input_args += ["-stream_loop", str(loops_needed - 1)]
            out_dur = min(loops_needed * src_dur, target_max)
        else:
            out_dur = src_dur

    filters = []
    if cfg["enable_normalize"]:
        filters.append("scale=1080:1920:force_original_aspect_ratio=increase")
        filters.append("crop=1080:1920")
        if src_fps > cfg["max_source_fps"]:
            filters.append(f"fps={cfg['max_source_fps']}")
    filter_str = ",".join(filters) if filters else "null"

    cmd = ["ffmpeg", "-y", "-loglevel", "error"]
    if ss is not None:
        cmd += ["-ss", str(ss)]
    cmd += input_args
    cmd += ["-i", raw_path]
    cmd += ["-t", f"{out_dur:.3f}", "-vf", filter_str]
    if cfg["enable_normalize"]:
        cmd += ["-map_metadata", "-1", "-map_chapters", "-1"]
    cmd += ["-c:v", "libx264", "-preset", "veryfast", "-crf", "16", "-pix_fmt", "yuv420p", "-g", "15"]
    if has_audio_stream(src_info):
        cmd += ["-c:a", "pcm_s16le"]
    else:
        cmd += ["-an"]
    cmd += [out_path]
    subprocess.run(cmd, check=True)
    return out_dur


# ---------------------------------------------------------------------------
# Stage 3: hook text wrap/shrink-to-fit
# ---------------------------------------------------------------------------

def _load_measure_font(cfg: dict, size: int):
    return ImageFont.truetype(resolve_font_file(cfg), size)


def _measure_width(text: str, font) -> float:
    return font.getlength(text)


def wrap_hook_text(text: str, cfg: dict) -> tuple[list[str], int]:
    """Wrap to <= hook_max_lines lines, shrinking font size until it fits 90% width."""
    max_width = 1080 * cfg["hook_max_width_ratio"]
    max_lines = cfg["hook_max_lines"]
    words = text.split()

    font_size = cfg["hook_font_size"]
    while font_size >= 24:
        font = _load_measure_font(cfg, font_size)
        lines, current = [], []
        fits = True
        for word in words:
            trial = " ".join(current + [word])
            if _measure_width(trial, font) <= max_width or not current:
                current.append(word)
            else:
                lines.append(" ".join(current))
                current = [word]
            if len(lines) >= max_lines:
                fits = False
                break
        if fits and current:
            lines.append(" ".join(current))
        if fits and len(lines) <= max_lines and all(
            _measure_width(line, font) <= max_width for line in lines
        ):
            return lines, font_size
        font_size -= 4
    return lines or [text], font_size


def build_hook_drawtext_filters(text: str, cfg: dict) -> str:
    lines, font_size = wrap_hook_text(text, cfg)
    font_path = resolve_font_file(cfg)

    line_height = font_size * 1.2
    base_y = f"h*{cfg['hook_y_ratio']}"
    enable = f"between(t\\,0\\,{cfg['hook_end_s']})"

    parts = []
    for i, line in enumerate(lines):
        y_expr = f"({base_y})+{i}*{line_height:.1f}" if i else base_y
        font_ref = f"fontfile={ffmpeg_escape_path(font_path)}"
        parts.append(
            "drawtext="
            f"{font_ref}:"
            f"text='{ffmpeg_escape_text(line)}':"
            f"fontsize={font_size}:fontcolor=white:borderw=4:bordercolor=black:"
            f"x=(w-text_w)/2:y={y_expr}:enable='{enable}'"
        )
    return ",".join(parts)


# ---------------------------------------------------------------------------
# Stage 4: punch-in cut rhythm
# ---------------------------------------------------------------------------

def build_punch_segments(duration: float, cfg: dict, rng: random.Random) -> list[tuple[float, float, float]]:
    """Return [(start, end, zoom), ...]. zoom == 1.0 means full-frame (no punch)."""
    first_cut = rng.uniform(cfg["punch_first_cut_min_s"], cfg["punch_first_cut_max_s"])
    first_cut = min(first_cut, max(duration - 0.5, 0.1))
    cuts = [0.0, first_cut]
    t = first_cut
    while t < duration:
        t += rng.uniform(cfg["punch_cut_interval_min_s"], cfg["punch_cut_interval_max_s"])
        if t < duration:
            cuts.append(t)
    cuts.append(duration)

    segments = []
    for i in range(len(cuts) - 1):
        start, end = cuts[i], cuts[i + 1]
        if end - start <= 0.05:
            continue
        punched = (i % 2 == 1)
        zoom = rng.uniform(cfg["punch_zoom_min"], cfg["punch_zoom_max"]) if punched else 1.0
        segments.append((start, end, zoom))
    return segments


def build_punch_and_hook_filter(segments: list[tuple[float, float, float]], hook_text: str | None, cfg: dict) -> str:
    """Build a single filter_complex: per-segment trim+crop, concat (one encode, no re-encode-per-segment), then hook drawtext."""
    parts = []
    labels = []
    for i, (start, end, zoom) in enumerate(segments):
        label = f"seg{i}"
        chain = f"[0:v]trim=start={start:.3f}:end={end:.3f},setpts=PTS-STARTPTS"
        if zoom != 1.0:
            sw, sh = round(1080 * zoom), round(1920 * zoom)
            ox, oy = (sw - 1080) // 2, (sh - 1920) // 2
            chain += f",scale={sw}:{sh},crop=1080:1920:{ox}:{oy}"
        # concat requires every segment to agree on SAR too, not just size --
        # without this a full-frame segment can carry a slightly different SAR
        # than a scaled/cropped one and concat refuses to configure the output.
        chain += ",setsar=1"
        chain += f"[{label}]"
        parts.append(chain)
        labels.append(f"[{label}]")

    parts.append("".join(labels) + f"concat=n={len(segments)}:v=1:a=0[punched]")

    if cfg["enable_hook_text"] and hook_text:
        hook_filters = build_hook_drawtext_filters(hook_text, cfg)
        parts.append(f"[punched]{hook_filters}[vout]")
        out_label = "[vout]"
    else:
        out_label = "[punched]"

    return ";".join(parts), out_label


def apply_punch_and_hook(in_path: str, out_path: str, duration: float, cfg: dict, hook_text: str | None, rng: random.Random) -> list[tuple[float, float, float]]:
    if cfg["enable_punch_in"]:
        segments = build_punch_segments(duration, cfg, rng)
    else:
        segments = [(0.0, duration, 1.0)]

    filter_complex, out_label = build_punch_and_hook_filter(segments, hook_text, cfg)
    cmd = [
        "ffmpeg", "-y", "-loglevel", "error",
        "-i", in_path,
        "-filter_complex", filter_complex,
        "-map", out_label,
        "-c:v", "libx264", "-preset", "veryfast", "-crf", "16", "-pix_fmt", "yuv420p", "-g", "15",
        "-an",
        out_path,
    ]
    subprocess.run(cmd, check=True)
    return segments


# ---------------------------------------------------------------------------
# Stage 5: seamless loop (perceptual-hash match, else crossfade fallback)
# ---------------------------------------------------------------------------

def extract_frame(video_path: str, t: float, out_png: str) -> None:
    cmd = [
        "ffmpeg", "-y", "-loglevel", "error",
        "-ss", f"{t:.3f}", "-i", video_path,
        "-vframes", "1", out_png,
    ]
    subprocess.run(cmd, check=True)


def extract_tail_frames(video_path: str, start: float, window: float, interval: float, tmp: str) -> list[tuple[float, str]]:
    pattern = os.path.join(tmp, "tail_%04d.png")
    fps = 1.0 / interval
    cmd = [
        "ffmpeg", "-y", "-loglevel", "error",
        "-ss", f"{start:.3f}", "-i", video_path,
        "-t", f"{window:.3f}", "-vf", f"fps={fps}",
        pattern,
    ]
    subprocess.run(cmd, check=True)
    frames = sorted(f for f in os.listdir(tmp) if f.startswith("tail_"))
    return [(start + i * interval, os.path.join(tmp, f)) for i, f in enumerate(frames)]


def find_loop_point(video_path: str, duration: float, cfg: dict, tmp: str) -> tuple[float | None, float]:
    """Return (loop_point_seconds_or_None, best_similarity_distance)."""
    frame0_png = os.path.join(tmp, "frame0.png")
    extract_frame(video_path, 0.0, frame0_png)
    hash0 = imagehash.phash(Image.open(frame0_png))

    window = min(cfg["loop_tail_window_s"], duration)
    start = max(duration - window, 0.0)
    candidates = extract_tail_frames(video_path, start, window, cfg["loop_sample_interval_s"], tmp)

    best_t, best_dist = None, None
    for t, png in candidates:
        if t <= start + 0.5:  # skip candidates too close to the window start / too far from the true tail
            continue
        dist = int(hash0 - imagehash.phash(Image.open(png)))  # numpy int64 -> plain int (JSON-serializable)
        if best_dist is None or dist < best_dist:
            best_dist, best_t = dist, t

    if best_dist is not None and best_dist <= cfg["loop_similarity_threshold"]:
        return best_t, best_dist
    return None, (best_dist if best_dist is not None else float("inf"))


# ---------------------------------------------------------------------------
# Stage 6: audio (two-pass loudnorm, or silent track)
# ---------------------------------------------------------------------------

LOUDNORM_STATS_RE = re.compile(r"\{[^{}]*\"input_i\"[^{}]*\}", re.DOTALL)


def measure_loudnorm(audio_src: str, cfg: dict) -> dict | None:
    cmd = [
        "ffmpeg", "-loglevel", "info", "-i", audio_src,
        "-af",
        f"loudnorm=I={cfg['loudnorm_target_i']}:TP={cfg['loudnorm_target_tp']}:"
        f"LRA={cfg['loudnorm_target_lra']}:print_format=json",
        "-f", "null", "-",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    match = LOUDNORM_STATS_RE.search(result.stderr)
    if not match:
        return None
    return json.loads(match.group(0))


def build_audio_track(src_path: str, out_path: str, duration: float, cfg: dict, src_info: dict) -> None:
    """Produce a final AAC track: loudnorm-normalized source audio, or silence."""
    use_silence = cfg["mute_audio"] or not has_audio_stream(src_info)

    if use_silence:
        cmd = [
            "ffmpeg", "-y", "-loglevel", "error",
            "-f", "lavfi", "-i", f"anullsrc=channel_layout=stereo:sample_rate={cfg['audio_sample_rate']}",
            "-t", f"{duration:.3f}",
            "-c:a", "aac", "-b:a", f"{cfg['audio_bitrate_k']}k",
            out_path,
        ]
        subprocess.run(cmd, check=True)
        return

    if cfg["enable_audio_normalize"]:
        stats = measure_loudnorm(src_path, cfg)
        if stats:
            af = (
                f"loudnorm=I={cfg['loudnorm_target_i']}:TP={cfg['loudnorm_target_tp']}:"
                f"LRA={cfg['loudnorm_target_lra']}:"
                f"measured_I={stats['input_i']}:measured_TP={stats['input_tp']}:"
                f"measured_LRA={stats['input_lra']}:measured_thresh={stats['input_thresh']}:"
                f"offset={stats['target_offset']}:linear=true:print_format=summary"
            )
        else:
            af = f"loudnorm=I={cfg['loudnorm_target_i']}:TP={cfg['loudnorm_target_tp']}:LRA={cfg['loudnorm_target_lra']}"
    else:
        af = "anull"

    cmd = [
        "ffmpeg", "-y", "-loglevel", "error",
        "-i", src_path, "-t", f"{duration:.3f}",
        "-af", af,
        "-ar", str(cfg["audio_sample_rate"]), "-ac", "2",
        "-c:a", "aac", "-b:a", f"{cfg['audio_bitrate_k']}k",
        out_path,
    ]
    subprocess.run(cmd, check=True)


# ---------------------------------------------------------------------------
# Stage 7: export (final mux, loop trim/crossfade, x264 settings)
# ---------------------------------------------------------------------------

def export_final(video_in: str, audio_in: str, out_path: str, cfg: dict,
                  duration: float, loop_point: float | None) -> tuple[float, str]:
    """Mux video+audio with the final encode settings, applying the loop decision.

    Returns (final_duration, loop_method).
    """
    max_rate = f"{cfg['video_max_bitrate_m']}M"
    bufsize = f"{cfg['video_max_bitrate_m'] * 2}M"
    encode_args = [
        "-c:v", "libx264", "-preset", cfg["video_preset"], "-crf", str(cfg["video_crf"]),
        "-maxrate", max_rate, "-bufsize", bufsize,
        "-pix_fmt", "yuv420p", "-profile:v", "high", "-level", "4.0",
        "-c:a", "aac", "-b:a", f"{cfg['audio_bitrate_k']}k",
        "-ar", str(cfg["audio_sample_rate"]), "-ac", "2",
        "-movflags", "+faststart",
    ]

    if cfg["enable_seamless_loop"] and loop_point is not None:
        final_dur = loop_point
        cmd = [
            "ffmpeg", "-y", "-loglevel", "error",
            "-i", video_in, "-i", audio_in,
            "-t", f"{final_dur:.3f}",
            "-map", "0:v", "-map", "1:a",
            *encode_args, out_path,
        ]
        subprocess.run(cmd, check=True)
        return final_dur, "perceptual_match"

    if cfg["enable_seamless_loop"]:
        # No end frame scored below the similarity threshold -> crossfade the
        # tail back into the opening frames instead (hard cut would still jump).
        xf = cfg["loop_crossfade_s"]
        offset = max(duration - xf, 0.0)
        filter_complex = (
            f"[0:v]trim=0:{xf:.3f},setpts=PTS-STARTPTS[opening];"
            f"[0:v][opening]xfade=transition=fade:duration={xf:.3f}:offset={offset:.3f}[vout]"
        )
        cmd = [
            "ffmpeg", "-y", "-loglevel", "error",
            "-i", video_in, "-i", audio_in,
            "-filter_complex", filter_complex,
            "-map", "[vout]", "-map", "1:a",
            "-t", f"{duration:.3f}",
            *encode_args, out_path,
        ]
        subprocess.run(cmd, check=True)
        return duration, "crossfade_fallback"

    cmd = [
        "ffmpeg", "-y", "-loglevel", "error",
        "-i", video_in, "-i", audio_in,
        "-map", "0:v", "-map", "1:a",
        *encode_args, out_path,
    ]
    subprocess.run(cmd, check=True)
    return duration, "disabled"


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def edit_video(raw_path: str, out_path: str, cfg: dict, video_name: str,
               hooks_by_category: dict[str, list[str]]) -> dict:
    """Run the full T2 edit template on one raw clip. Returns the manifest dict."""
    rng = random.Random(video_name)
    src_info = probe(raw_path)

    with tempfile.TemporaryDirectory() as tmp:
        normalized_path = os.path.join(tmp, "normalized.mkv")
        norm_dur = build_normalize_and_trim(raw_path, normalized_path, cfg, src_info)

        hook_text, hook_category = (None, None)
        if cfg["enable_hook_text"]:
            hook_text, hook_category = next_hook(cfg["hook_category"], hooks_by_category)

        punch_path = os.path.join(tmp, "punch_hook.mkv")
        segments = apply_punch_and_hook(normalized_path, punch_path, norm_dur, cfg, hook_text, rng)

        loop_point, similarity = (None, None)
        loop_method = "disabled"
        if cfg["enable_seamless_loop"]:
            loop_point, similarity = find_loop_point(punch_path, norm_dur, cfg, tmp)

        # Source audio from the already-trimmed/looped normalized clip, not the raw
        # file -- its audio was cut with the exact same -ss/-stream_loop/-t as the
        # video track, so it stays in sync. Extracting from the raw file directly
        # would desync whenever hook_start_offset_s or short-clip looping applies.
        audio_path = os.path.join(tmp, "audio.m4a")
        build_audio_track(normalized_path, audio_path, norm_dur, cfg, src_info)

        final_dur, loop_method = export_final(punch_path, audio_path, out_path, cfg, norm_dur, loop_point)

    manifest = {
        "video_name": video_name,
        "source_duration_s": probe_duration(src_info),
        "working_duration_s": round(norm_dur, 3),
        "final_duration_s": round(final_dur, 3),
        "hook_text": hook_text,
        "hook_category": hook_category,
        "punch_in_segments": [
            {"start": round(s, 3), "end": round(e, 3), "zoom": round(z, 3)} for s, e, z in segments
        ],
        "loop_method": loop_method,
        "loop_point_s": round(loop_point, 3) if loop_point is not None else None,
        "loop_similarity": similarity,
        "config": cfg,
    }
    print(
        f"  loop: {loop_method} "
        f"(point={manifest['loop_point_s']}, similarity={similarity}) "
        f"hook: [{hook_category}] {hook_text!r}"
    )
    return manifest


def edit_day(day: str, channels: list[dict], defaults: dict, hooks_by_category: dict, tmp: str,
             remaining: int | None = None) -> int:
    """Edit pending videos for `day`. Stops early once `remaining` edits have been done.

    Returns how many (video, channel) edits were actually performed.
    """
    videos = get_raw_videos(day)
    print(f"Date: {day} | channels: {[c['folder'] for c in channels]} | raw videos: {len(videos)}")
    if not videos:
        return 0

    done_count = 0
    for vkey in videos:
        if remaining is not None and done_count >= remaining:
            break
        video_name = os.path.splitext(os.path.basename(vkey))[0]
        local_raw = None

        for channel in channels:
            if remaining is not None and done_count >= remaining:
                break
            folder = channel["folder"]
            if already_done(day, folder, video_name):
                print(f"skip  {folder} / {video_name} (already edited)")
                continue
            if local_raw is None:
                local_raw = os.path.join(tmp, os.path.basename(vkey))
                print(f"downloading {vkey} ...")
                s3.download_file(BUCKET, vkey, local_raw)

            cfg = resolve_config(defaults, channel)
            out_local = os.path.join(tmp, f"{folder}__{video_name}.mp4")
            out_key = f"processed/{folder}/{day}/{video_name}.mp4"
            manifest_key = f"processed/{folder}/{day}/{video_name}.json"
            print(f"edit  {folder} / {video_name} ...")
            try:
                manifest = edit_video(local_raw, out_local, cfg, video_name, hooks_by_category)
            except subprocess.CalledProcessError as e:
                print(f"FFMPEG FAILED for {folder}/{video_name}: {e}")
                continue

            s3.upload_file(out_local, BUCKET, out_key)
            touch_key(manifest_key, json.dumps(manifest, indent=2).encode("utf-8"))
            mark_done(day, folder, video_name)
            os.remove(out_local)
            done_count += 1
            print(f"done  -> {out_key}")

        if local_raw and os.path.exists(local_raw):
            os.remove(local_raw)

    return done_count


def load_channels_t2() -> list[dict]:
    env_json = os.environ.get("CHANNELS_T2_JSON")
    if env_json:
        return json.loads(env_json)
    path = os.path.join(BASE_DIR, "channels_t2.json")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def run(day: str | None = None, limit: int | None = None) -> None:
    """Edit not-yet-edited raw videos, for every T2 channel.

    `limit` caps how many (video, channel) edits this call performs. Defaults
    to DEFAULT_EDIT_LIMIT (20) so a single run -- local, scheduled, or a manual
    workflow_dispatch -- can never accidentally chew through the entire raw
    backlog (and its Actions minutes) in one go; the T2_MAX_EDITS env var or
    an explicit `limit` overrides it for a deliberately larger batch.
    """
    channels = load_channels_t2()
    if not channels:
        print("No channels configured in channels_t2.json. Nothing to do.")
        return

    if limit is None:
        env_limit = os.environ.get("T2_MAX_EDITS")
        limit = int(env_limit) if env_limit else DEFAULT_EDIT_LIMIT

    defaults = load_default_config()
    hooks_by_category = load_hooks()

    days = [day] if day else get_all_dates()
    if not days:
        print("No raw videos found. Nothing to do.")
        return

    remaining = limit
    with tempfile.TemporaryDirectory() as tmp:
        for d in days:
            if remaining is not None and remaining <= 0:
                print(f"Edit limit ({limit}) reached, stopping.")
                break
            n = edit_day(d, channels, defaults, hooks_by_category, tmp, remaining)
            if remaining is not None:
                remaining -= n

    print("Edit pass finished.")


def main() -> None:
    day = sys.argv[1] if len(sys.argv) > 1 else None
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else None
    run(day, limit)


if __name__ == "__main__":
    main()
