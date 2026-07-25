"""
Publish a Reel to Instagram via the Instagram API with Instagram Login.

Standalone usage:
    1. Fill in ACCESS_TOKEN, IG_USER_ID, VIDEO_URL, CAPTION below
       (or pass them as environment variables of the same names).
    2. python auto_insta_upload.py
Can also be imported: `from auto_insta_upload import publish_reel`.
Requires: pip install requests
"""

import os
import sys
import time

import requests

from env_local import load_dotenv

load_dotenv()

# ---------- CONFIG for standalone use (set env vars, or put them in .env locally) ----------
ACCESS_TOKEN = os.getenv("IG_ACCESS_TOKEN", "PASTE_YOUR_ACCESS_TOKEN")
IG_USER_ID = os.getenv("IG_USER_ID", "PASTE_YOUR_IG_USER_ID")
VIDEO_URL = os.getenv("VIDEO_URL", "")
CAPTION = os.getenv("CAPTION", "")
# ---------------------------------------------------------------------------------------

API = "https://graph.instagram.com/v23.0"


def create_container(ig_user_id: str, access_token: str, video_url: str, caption: str) -> str:
    """Step 1: create a media container. Instagram fetches the video from video_url."""
    r = requests.post(
        f"{API}/{ig_user_id}/media",
        data={
            "media_type": "REELS",
            "video_url": video_url,
            "caption": caption,
            "access_token": access_token,
        },
        timeout=60,
    )
    body = r.json()
    if "id" not in body:
        raise RuntimeError(f"Container creation failed: {body}")
    print(f"Container created: {body['id']}")
    return body["id"]


def wait_until_ready(container_id: str, access_token: str, max_wait_seconds: int = 600) -> None:
    """Step 2: poll the container until Instagram finishes processing the video."""
    waited = 0
    while waited < max_wait_seconds:
        r = requests.get(
            f"{API}/{container_id}",
            params={"fields": "status_code,status", "access_token": access_token},
            timeout=30,
        )
        body = r.json()
        status = body.get("status_code")
        print(f"  status: {status} ({waited}s elapsed)")
        if status == "FINISHED":
            return
        if status == "ERROR":
            raise RuntimeError(f"Processing failed: {body}")
        time.sleep(15)
        waited += 15
    raise RuntimeError("Timed out waiting for Instagram to process the video.")


def publish(container_id: str, ig_user_id: str, access_token: str) -> str:
    """Step 3: publish the finished container. The reel goes live."""
    r = requests.post(
        f"{API}/{ig_user_id}/media_publish",
        data={"creation_id": container_id, "access_token": access_token},
        timeout=60,
    )
    body = r.json()
    if "id" not in body:
        raise RuntimeError(f"Publish failed: {body}")
    return body["id"]


def publish_reel(ig_user_id: str, access_token: str, video_url: str, caption: str) -> str:
    """Run all three steps. Returns the published media id. Raises RuntimeError on failure."""
    container_id = create_container(ig_user_id, access_token, video_url, caption)
    print("Waiting for Instagram to process the video...")
    wait_until_ready(container_id, access_token)
    return publish(container_id, ig_user_id, access_token)


if __name__ == "__main__":
    if "PASTE_YOUR" in ACCESS_TOKEN or "PASTE_YOUR" in IG_USER_ID:
        sys.exit("Fill in ACCESS_TOKEN and IG_USER_ID at the top of the script first.")
    try:
        media_id = publish_reel(IG_USER_ID, ACCESS_TOKEN, VIDEO_URL, CAPTION)
    except RuntimeError as e:
        sys.exit(str(e))
    print(f"\nPublished! Media ID: {media_id}")
    print("Open the Instagram account - your reel should be live.")