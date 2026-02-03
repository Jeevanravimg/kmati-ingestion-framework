import yaml
import subprocess
import time
import json
import os
from datetime import datetime


RCLONE_EXE = r"C:\Users\Administrator\OneDrive\Desktop\rclone\rclone.exe"


# -------------------------------
# Helper: Load or create checkpoint file
# -------------------------------
def load_checkpoint(path):
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return {"last_run": None, "folders": {}}
    try:
        with open(path, "r") as f:
            data = json.load(f)
            if "folders" not in data:
                data["folders"] = {}
            return data
    except Exception:
        return {"last_run": None, "folders": {}}


def save_checkpoint(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=4)


# -------------------------------
# Upload checkpoint: create timestamped copy AND update latest pointer
# -------------------------------
def upload_checkpoint_versions(local_path, landing_zone, rclone_config):
    """
    Uploads:
      - checkpoints/minio_source_<ts>.json  (keeps history)
      - checkpoints/minio_source.json       (latest pointer)
    """
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    try:
        parts = landing_zone.split(":", 1)
        if len(parts) != 2:
            raise ValueError("landing_zone format invalid")
        remote_name = parts[0]
        rest = parts[1]
        bucket_name = rest.split("/", 1)[0]
        remote_base = f"{remote_name}:{bucket_name}"
    except Exception as e:
        print(f"Warning: cannot derive remote checkpoint path from landing_zone ({landing_zone}): {e}")
        return

    remote_ts = f"{remote_base}/checkpoints/minio_source_{ts}.json"
    remote_latest = f"{remote_base}/checkpoints/minio_source.json"

    # timestamped upload (history)
    cmd_ts = [RCLONE_EXE, "--config", rclone_config, "copyto", local_path, remote_ts]
    # latest pointer (overwrite)
    cmd_latest = [RCLONE_EXE, "--config", rclone_config, "copyto", local_path, remote_latest]

    try:
        subprocess.check_call(cmd_ts)
        print(f"Uploaded timestamped checkpoint to {remote_ts}")
    except subprocess.CalledProcessError as e:
        print(f"Warning: failed to upload timestamped checkpoint ({remote_ts}): {e}")

    try:
        subprocess.check_call(cmd_latest)
        print(f"Uploaded latest checkpoint to {remote_latest}")
    except subprocess.CalledProcessError as e:
        print(f"Warning: failed to upload latest checkpoint ({remote_latest}): {e}")


# -------------------------------
# Move landing -> archive with timestamped folder (appends history)
# -------------------------------
def move_landing_to_timestamped_archive(rclone_config, landing_zone, name):
    """
    Moves landing/<name> -> archive/<name>/<YYYYMMDD_HHMMSS>/
    This preserves older archives for the same folder.
    """
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    try:
        parts = landing_zone.split(":", 1)
        if len(parts) != 2:
            raise ValueError("landing_zone format invalid")
        remote_name = parts[0]
        rest = parts[1]
        bucket_name = rest.split("/", 1)[0]
        remote_base = f"{remote_name}:{bucket_name}"
    except Exception as e:
        print(f"Warning: cannot derive archive path from landing_zone ({landing_zone}): {e}")
        return

    landing_remote = f"{remote_base}/landing/{name}"
    archive_remote_ts = f"{remote_base}/archive/{name}/{ts}/"

    # use rclone move to transfer entire folder to timestamped archive folder
    cmd = [
        RCLONE_EXE,
        "--config", rclone_config,
        "copy",
        landing_remote,
        archive_remote_ts,
        "--progress"
    ]
    try:
        subprocess.check_call(cmd)
        print(f"Moved {landing_remote} -> {archive_remote_ts}")
    except subprocess.CalledProcessError as e:
        print(f"Warning: failed to move to archive ({landing_remote} -> {archive_remote_ts}): {e}")


# -------------------------------
# Load YAML
# -------------------------------
CONFIG_FILE = "config.yaml"
config = yaml.safe_load(open(CONFIG_FILE))

rclone_config = config["rclone"]["config_file"]
sources = config["sources"]
landing_zone = config["output"]["landing_zone"]
checkpoint_path_cfg = config["output"]["checkpoint_path"]
metadata_path = config["output"].get("metadata_path")  # not used directly here
settings = config["settings"]
mode = settings.get("mode", "full")
retries = int(settings.get("retries", 3))
retry_delay = int(settings.get("retry_delay", 5))

# -------------------------------
# Normalize checkpoint path: dir -> file
# -------------------------------
def normalize_checkpoint_path(path):
    if path.endswith("/") or path.endswith("\\") or os.path.isdir(path):
        folder = path.rstrip("/\\")
        return os.path.join(folder, "minio_source.json")
    parent = os.path.dirname(path)
    if parent == "":
        parent = "."
    os.makedirs(parent, exist_ok=True)
    return path

checkpoint_path = normalize_checkpoint_path(checkpoint_path_cfg)

# Ensure checkpoint directory exists and load checkpoint
os.makedirs(os.path.dirname(checkpoint_path), exist_ok=True)
checkpoint = load_checkpoint(checkpoint_path)

print("Extractor Started...")
print("------------------------------------------")

for src in sources:
    name = src["name"]
    remote = src["remote"]
    bucket = src["bucket"]

    print(f"\nExtracting: {name}")
    print(f"Source: {remote}:{bucket}")
    print(f"Target: {landing_zone}/{name}")

    cmd = [
        RCLONE_EXE,
        "--config", rclone_config,
        "copy",
        f"{remote}:{bucket}",
        f"{landing_zone}/{name}",
        "--progress"
    ]

    attempt = 0
    success = False

    while attempt < retries:
        try:
            subprocess.check_call(cmd)
            print(f"SUCCESS: {name}")
            success = True
            break
        except subprocess.CalledProcessError as e:
            attempt += 1
            print(f"Retry {attempt}/{retries} for {name} after rclone returned {e.returncode}...")
            time.sleep(retry_delay)
        except Exception as e:
            attempt += 1
            print(f"Retry {attempt}/{retries} for {name} after exception: {e}")
            time.sleep(retry_delay)

    # Update local checkpoint (pipeline-level)
    checkpoint["folders"][name] = {
        "status": "success" if success else "failed",
        "last_copied": datetime.now().strftime("%Y-%m-%d %H:%M:%S") if success else None
    }

    # Save local checkpoint immediately after each folder
    save_checkpoint(checkpoint_path, checkpoint)

    # If success: move landing -> timestamped archive (so archive keeps history)
    if success:
        try:
            move_landing_to_timestamped_archive(rclone_config, landing_zone, name)
        except Exception as e:
            print(f"Warning during archive step for {name}: {e}")

    # Upload checkpoint history + latest pointer to target bucket
    try:
        upload_checkpoint_versions(checkpoint_path, landing_zone, rclone_config)
    except Exception as e:
        print(f"Warning while uploading checkpoint versions to target bucket: {e}")

# Finalize
checkpoint["last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
save_checkpoint(checkpoint_path, checkpoint)

# final upload as well
try:
    upload_checkpoint_versions(checkpoint_path, landing_zone, rclone_config)
except Exception as e:
    print(f"Warning while uploading final checkpoint versions to target bucket: {e}")

print("\nExtraction Completed.")
