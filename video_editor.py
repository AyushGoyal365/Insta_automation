"""
Daily Reel Editing Pipeline (R2 -> FFmpeg -> R2)

Bucket layout expected:
    raw/video/{YYYY-MM-DD}/*.mp4                    <- raw videos, date-wise
    channel_info/{channel_name}/<image file>        <- one branding image per channel
Outputs written:
    processed/{channel}/{date}/{video}.mp4          <- edited videos
    video_logs/{channel}/{date}/{video}.done        <- edit log (skip if exists)

Layout of the edited video (1080x1920):
    top 40% (768px)  -> channel image
    bottom 60% (1152px) -> the video (cropped to fill, no black strip)

Usage:
    pip install boto3
    Set the env vars below, then:  python video_editor.py             (processes every date found)
                                   python video_editor.py 2026-07-21  (specific date only)
Requires ffmpeg installed on the machine.
"""

import os
import subprocess
import sys
import tempfile

import boto3

from env_local import load_dotenv

load_dotenv()

# ---------- CONFIG via environment variables (set in .env locally, or real env vars / secrets in CI) ----------
R2_ENDPOINT = os.environ["R2_ENDPOINT"]
R2_ACCESS_KEY = os.environ["R2_ACCESS_KEY"]
R2_SECRET_KEY = os.environ["R2_SECRET_KEY"]
BUCKET = os.environ.get("R2_BUCKET", "reel-dump-b1")  # default bucket name
# -----------------------------------------------------------------------------------------------------------

VIDEO_EXTS = (".mp4", ".mov", ".mkv", ".webm", ".m4v")
IMAGE_EXTS = (".jpg", ".jpeg", ".png", ".webp")

s3 = boto3.client(
    "s3",
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY,
    region_name="auto",
)


def list_keys(prefix: str) -> list[str]:
    """List all object keys under a prefix (handles pagination)."""
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def get_channels() -> dict[str, str]:
    """Return {channel_name: image_key} from channel_info/."""
    channels = {}
    for key in list_keys("channel_info/"):
        parts = key.split("/")
        if len(parts) >= 3 and parts[2] and key.lower().endswith(IMAGE_EXTS):
            channels.setdefault(parts[1], key)  # first image found per channel
    return channels


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


def already_done(day: str, channel: str, video_name: str) -> bool:
    return key_exists(f"video_logs/{channel}/{day}/{video_name}.done")


def mark_done(day: str, channel: str, video_name: str) -> None:
    touch_key(f"video_logs/{channel}/{day}/{video_name}.done")


def edit_video(video_path: str, image_path: str, out_path: str) -> None:
    """Compose 1080x1920: top 40% image, bottom 60% video.

    The source clips have their own baked-in strip (watermark/caption) covering
    the top ~30% of the frame, so that's cropped off before the remaining
    content is scaled to fill its slot -- our channel image replaces it.
    """
    filter_complex = (
        "[1:v]scale=1080:768:force_original_aspect_ratio=increase,"
        "crop=1080:768,setsar=1[img];"
        "[0:v]crop=iw:ih*0.7:0:ih*0.3,"
        "scale=1080:1152:force_original_aspect_ratio=increase,"
        "crop=1080:1152,setsar=1[vid];"
        "[img][vid]vstack=inputs=2,format=yuv420p[out]"
    )
    cmd = [
        "ffmpeg", "-y", "-loglevel", "error",
        "-i", video_path,
        "-loop", "1", "-i", image_path,
        "-filter_complex", filter_complex,
        "-map", "[out]", "-map", "0:a?",
        "-c:v", "libx264", "-preset", "veryfast", "-crf", "23",
        "-c:a", "aac", "-b:a", "128k",
        "-movflags", "+faststart",
        "-r", "30",
        "-shortest",
        out_path,
    ]
    subprocess.run(cmd, check=True)


def edit_day(day: str, channels: dict[str, str], tmp: str, image_paths: dict[str, str]) -> None:
    """Edit any not-yet-edited raw videos for `day`, across all given channels."""
    videos = get_raw_videos(day)
    print(f"Date: {day} | channels: {list(channels)} | raw videos: {len(videos)}")
    if not videos:
        return

    for vkey in videos:
        video_name = os.path.splitext(os.path.basename(vkey))[0]
        local_raw = None  # download lazily, only if some channel needs it

        for channel, img_path in image_paths.items():
            if already_done(day, channel, video_name):
                print(f"skip  {channel} / {video_name} (already edited)")
                continue
            if local_raw is None:
                local_raw = os.path.join(tmp, os.path.basename(vkey))
                print(f"downloading {vkey} ...")
                s3.download_file(BUCKET, vkey, local_raw)

            out_local = os.path.join(tmp, f"{channel}__{video_name}.mp4")
            out_key = f"processed/{channel}/{day}/{video_name}.mp4"
            print(f"edit  {channel} / {video_name} ...")
            try:
                edit_video(local_raw, img_path, out_local)
            except subprocess.CalledProcessError as e:
                print(f"FFMPEG FAILED for {channel}/{video_name}: {e}")
                continue
            s3.upload_file(out_local, BUCKET, out_key)
            mark_done(day, channel, video_name)
            os.remove(out_local)
            print(f"done  -> {out_key}")

        if local_raw and os.path.exists(local_raw):
            os.remove(local_raw)


def run(day: str | None = None) -> None:
    """Edit all not-yet-edited raw videos, for every channel.

    If `day` is given, only that date is scanned. Otherwise every date folder
    under Raw/Video/ is scanned, so nothing from earlier days is ever missed.
    """
    channels = get_channels()
    if not channels:
        print("No channels found (channel_info/ is empty). Nothing to do.")
        return

    days = [day] if day else get_all_dates()
    if not days:
        print("No raw videos found. Nothing to do.")
        return

    with tempfile.TemporaryDirectory() as tmp:
        # download each channel image once, reused across every date
        image_paths = {}
        for channel, img_key in channels.items():
            p = os.path.join(tmp, f"img_{channel}{os.path.splitext(img_key)[1]}")
            s3.download_file(BUCKET, img_key, p)
            image_paths[channel] = p

        for d in days:
            edit_day(d, channels, tmp, image_paths)

    print("Edit pass finished.")


def main() -> None:
    day = sys.argv[1] if len(sys.argv) > 1 else None
    run(day)


if __name__ == "__main__":
    main()