"""
Template-2 pipeline: edit every not-yet-edited raw video in reel-dump-b2 using
the video_edit_t2 template, then post one not-yet-posted processed video per
channel to Instagram.

This mirrors run_pipeline.py but targets the reel-dump-b2 bucket and the T2
edit template (normalize -> trim -> hook overlay -> punch-in -> seamless loop
-> loudness-normalized audio). Kept as a fully separate script/bucket/config
so it can never interfere with the existing reel-dump-b1 pipeline.

Channel config (IG account -> R2 folder mapping): channels_t2.json
    [{"folder": "...", "ig_user_id": "...", "access_token": "...",
      "hook_category": "...", "config_overrides": {...}, "captions": ["...", "..."]}]

Per-video caption override (optional): Raw/Video/{date}/{video_name}.txt
    If present, its contents are used as the caption instead of a random pick
    from the channel's captions list.

Trailing #hashtags in a caption are posted as a follow-up comment rather than
left in the caption itself -- Instagram's Reels API doesn't reliably render
hashtags in API-submitted captions as clickable, but hashtags in comments do.

Upload log: channel_logs/{folder}/{date}/{video_name}.done (skip if exists)

Usage:
    pip install boto3 requests Pillow imagehash numpy pyyaml
    python run_pipeline_t2.py
Requires ffmpeg installed on the machine (see video_edit_t2.py).
"""

import json
import os
import random
import re

from auto_insta_upload import comment_on_media, publish_reel
from video_edit_t2 import BUCKET, VIDEO_EXTS, key_exists, list_keys, run as edit_all_pending, s3, touch_key

TRAILING_HASHTAGS = re.compile(r"(?:\s*#\w+)+\s*$")

R2_PUBLIC_BASE_T2 = os.environ.get("R2_PUBLIC_BASE_T2", "")
CHANNELS_CONFIG = os.path.join(os.path.dirname(os.path.abspath(__file__)), "channels_t2.json")


def load_channels() -> list[dict]:
    """Load channel configs from the CHANNELS_T2_JSON env var (used in CI), else channels_t2.json locally."""
    env_json = os.environ.get("CHANNELS_T2_JSON")
    if env_json:
        return json.loads(env_json)
    with open(CHANNELS_CONFIG, "r", encoding="utf-8") as f:
        return json.load(f)


def find_unposted_video(folder: str) -> tuple[str, str] | None:
    """Return (date, video_name) for the first processed video not yet posted, or None."""
    for key in sorted(list_keys(f"processed/{folder}/")):
        if not key.lower().endswith(VIDEO_EXTS):
            continue
        parts = key.split("/")  # processed/{folder}/{date}/{video}.mp4
        if len(parts) < 4:
            continue
        day, filename = parts[2], parts[3]
        video_name = os.path.splitext(filename)[0]
        # channel_logs (post-done), not video_logs (edit-done, set by video_edit_t2.py) --
        # sharing one prefix would make every video look "already posted" the instant
        # it's edited, since mark_done() sets that same key during the edit pass.
        if not key_exists(f"channel_logs/{folder}/{day}/{video_name}.done"):
            return day, video_name
    return None


def get_caption(day: str, video_name: str, captions: list[str]) -> str:
    """Use Raw/Video/{day}/{video_name}.txt as the caption if present, else a random pick from captions."""
    caption_key = f"Raw/Video/{day}/{video_name}.txt"
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=caption_key)
        return obj["Body"].read().decode("utf-8").strip()
    except s3.exceptions.ClientError:
        return random.choice(captions) if captions else ""


def split_hashtags(caption: str) -> tuple[str, str]:
    """Split a trailing block of #hashtags off the end of a caption.

    Returns (body, hashtags). hashtags is "" if the caption has none at the end.
    """
    match = TRAILING_HASHTAGS.search(caption)
    if not match:
        return caption, ""
    return caption[: match.start()].rstrip(), caption[match.start():].strip()


def post_pending(channels: list[dict]) -> None:
    for ch in channels:
        folder = ch["folder"]
        found = find_unposted_video(folder)
        if not found:
            print(f"{folder}: nothing new to post")
            continue

        day, video_name = found
        video_url = f"{R2_PUBLIC_BASE_T2}/processed/{folder}/{day}/{video_name}.mp4"
        caption = get_caption(day, video_name, ch.get("captions", []))
        caption_body, hashtags = split_hashtags(caption)
        print(f"{folder}: posting {video_name} ({day}) ...")
        try:
            media_id = publish_reel(ch["ig_user_id"], ch["access_token"], video_url, caption_body)
        except RuntimeError as e:
            print(f"{folder}: POST FAILED: {e}")
            continue

        if hashtags:
            try:
                comment_on_media(media_id, ch["access_token"], hashtags)
                print(f"{folder}: hashtags posted as a comment")
            except RuntimeError as e:
                print(f"{folder}: hashtag comment failed (post itself still succeeded): {e}")

        touch_key(f"channel_logs/{folder}/{day}/{video_name}.done")
        print(f"{folder}: posted -> media_id {media_id}")


def run_pipeline() -> None:
    if not R2_PUBLIC_BASE_T2:
        print("R2_PUBLIC_BASE_T2 is not set -- posted video URLs would be broken. Set it in .env.")
        return

    print("=== Editing pending videos (T2 template) ===")
    edit_all_pending()

    print("=== Posting to Instagram ===")
    channels = load_channels()
    if not channels:
        print(f"No channels configured in {CHANNELS_CONFIG}.")
        return
    post_pending(channels)

    print("Pipeline finished.")


if __name__ == "__main__":
    run_pipeline()
