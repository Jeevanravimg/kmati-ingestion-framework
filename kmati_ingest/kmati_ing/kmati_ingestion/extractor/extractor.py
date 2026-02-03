import yaml
import subprocess
import time
import json
import os
import shutil
import platform
from datetime import datetime


# =================================================
# RCLONE SETUP (FIX FOR DATABRICKS)
# =================================================
def ensure_rclone_installed():
    """
    Installs rclone on Databricks if not already installed.
    Safe to run multiple times.
    """
    if shutil.which("rclone"):
        print("rclone already available")
        return

    print("rclone not found. Installing rclone...")
    install_cmd = "curl -s https://rclone.org/install.sh | sudo bash"

    subprocess.check_call(["bash", "-c", install_cmd])

    if not shutil.which("rclone"):
        raise RuntimeError("rclone installation failed")

    print("rclone installed successfully")


# Windows → local rclone.exe
# Databricks/Linux → auto-install rclone
if platform.system() == "Windows":
    RCLONE_EXE = r"C:\Users\Administrator\OneDrive\Desktop\rclone\rclone.exe"
else:
    ensure_rclone_installed()
    RCLONE_EXE = "rclone"


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
# Upload checkpoint versions
# -------------------------------
def upload_checkpoint_versions(local_path, landing_zone, rclone_config):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    try:
        remote_name, rest = landing_zone.split(":", 1)
        bucket_name = rest.split("/", 1)[0]
        remote_base = f"{remote_name}:{bucket_name}"
    except Exception as e:
        print(f"Warning: cannot derive remote checkpoint path: {e}")
        return

    remote_ts = f"{remote_base}/checkpoints/minio_source_{ts}.json"
    remote_latest = f"{remote_base}/checkpoints/minio_source.json"

    for remote in [remote_ts, remote_latest]:
        try:
            subprocess.check_call([
                RCLONE_EXE,
                "--config", rclone_config,
                "copyto",
                local_path,
                remote
            ])
            print(f"Uploaded checkpoint → {remote}")
        except subprocess.CalledProcessError as e:
            print(f"Warning uploading checkpoint {remote}: {e}")


# -------------------------------
# Move landing → archive (timestamped)
# -------------------------------
def move_landing_to_timestamped_archive(rclone_config, landing_zone, name):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    try:
        remote_name, rest = landing_zone.split(":", 1)
        bucket_name = rest.split("/", 1)[0]
        remote_base = f"{remote_name}:{bucket_name}"
    except Exception as e:
        print(f"Warning: cannot derive archive path: {e}")
        return

    subprocess.check_call([
        RCLONE_EXE,
        "--config", rclone_config,
        "copy",
        f"{remote_base}/landing/{name}",
        f"{remote_base}/archive/{name}/{ts}/",
        "--progress"
    ])

    print(f"Archived landing/{name} → archive/{name}/{ts}")


# -------------------------------
# Load YAML config
# -------------------------------
CONFIG_FILE = "config.yaml"
config = yaml.safe_load(open(CONFIG_FILE))

rclone_config = config["rclone"]["config_file"]
sources = config["sources"]
landing_zone = config["output"]["landing_zone"]
checkpoint_path_cfg = config["output"]["checkpoint_path"]
settings = config["settings"]

retries = int(settings.get("retries", 3))
retry_delay = int(settings.get("retry_delay", 5))


# -------------------------------
# Normalize checkpoint path
# -------------------------------
def normalize_checkpoint_path(path):
    if path.endswith("/") or path.endswith("\\") or os.path.isdir(path):
        return os.path.join(path.rstrip("/\\"), "minio_source.json")
    parent = os.path.dirname(path) or "."
    os.makedirs(parent, exist_ok=True)
    return path


checkpoint_path = normalize_checkpoint_path(checkpoint_path_cfg)
checkpoint = load_checkpoint(checkpoint_path)

print("Extractor Started...")
print("------------------------------------------")


for src in sources:
    name = src["name"]
    remote = src["remote"]
    bucket = src["bucket"]

    print(f"\nExtracting: {name}")

    cmd = [
        RCLONE_EXE,
        "--config", rclone_config,
        "copy",
        f"{remote}:{bucket}",
        f"{landing_zone}/{name}",
        "--progress"
    ]

    success = False

    for attempt in range(1, retries + 1):
        try:
            subprocess.check_call(cmd)
            print(f"SUCCESS: {name}")
            success = True
            break
        except subprocess.CalledProcessError:
            print(f"Retry {attempt}/{retries} for {name}")
            time.sleep(retry_delay)

    checkpoint["folders"][name] = {
        "status": "success" if success else "failed",
        "last_copied": datetime.now().strftime("%Y-%m-%d %H:%M:%S") if success else None
    }

    save_checkpoint(checkpoint_path, checkpoint)

    if success:
        move_landing_to_timestamped_archive(rclone_config, landing_zone, name)

    upload_checkpoint_versions(checkpoint_path, landing_zone, rclone_config)


checkpoint["last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
save_checkpoint(checkpoint_path, checkpoint)
upload_checkpoint_versions(checkpoint_path, landing_zone, rclone_config)

print("\nExtraction Completed.")
