#%%
import os
import time
import logging
from dotenv import load_dotenv
import paramiko

# Load env
load_dotenv("student.env")

SFTP_HOST = os.getenv("SFTP_HOST")
SFTP_PORT = int(os.getenv("SFTP_PORT"))
SFTP_USER = os.getenv("SFTP_USER")
SFTP_PASSWORD = os.getenv("SFTP_PASSWORD")
SFTP_DIR = os.getenv("SFTP_DIR")
LOCAL_FILE = "example.csv"

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s"
)
logger = logging.getLogger(__name__)

def upload_file():
    try:
        logger.info("Connecting to SFTP…")
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(SFTP_HOST, port=SFTP_PORT, username=SFTP_USER, password=SFTP_PASSWORD)
        logger.info("✅ SSH connection established.")

        sftp = ssh.open_sftp()
        logger.info("✅ SFTP session established.")

        # Check if SFTP_DIR exists
        try:
            sftp.listdir(SFTP_DIR)
            logger.info(f"📂 Remote directory exists: {SFTP_DIR}")
        except IOError:
            logger.error(f"❌ Remote directory does not exist: {SFTP_DIR}")
            return

        # Prepare full remote path
        remote_path = f"{SFTP_DIR}/{os.path.basename(LOCAL_FILE)}"
        logger.info(f"⬆️ Uploading {LOCAL_FILE} → {remote_path}")
        sftp.put(LOCAL_FILE, remote_path)

        # Verify
        local_size = os.path.getsize(LOCAL_FILE)
        remote_size = sftp.stat(remote_path).st_size
        if local_size == remote_size:
            logger.info(f"✅ Upload verified: {local_size} bytes.")
        else:
            logger.warning(f"⚠️ Size mismatch: local {local_size} vs remote {remote_size}")

        sftp.close()
        ssh.close()
        logger.info("🔒 Connection closed.")

    except Exception as e:
        logger.error(f"❌ SFTP session failed: {e}")
        raise

# Retry logic
for attempt in range(2):
    try:
        upload_file()
        break
    except Exception as e:
        if attempt == 0:
            logger.warning("🔄 Retrying in 3 seconds…")
            time.sleep(3)
        else:
            logger.error("❌ Failed to upload after retries.")

#%%