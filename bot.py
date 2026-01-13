import os
import logging
import queue
import threading
import traceback
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    MessageHandler,
    filters,
    CommandHandler,
    ContextTypes,
)
import swiftclient

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
BLOMP_EMAIL = os.getenv("BLOMP_EMAIL")
BLOMP_PASSWORD = os.getenv("BLOMP_PASSWORD")

# Try both common auth URLs - start with the most reported working one
SWIFT_AUTH_URLS = [
    "https://authenticate.blomp.com",          # Preferred in recent reports
    "https://authenticate.blomp.com/v2.0",     # Variant that fixed 404 for some
    "https://authenticate.ain.net"             # Alternative domain used by some tools
]
SWIFT_TENANT_NAME = "storage"
DESTINATION_FOLDER = "Videos"  # Pseudo-folder

swift_conn = None

def init_swift():
    global swift_conn
    for auth_url in SWIFT_AUTH_URLS:
        try:
            logger.info(f"Trying auth URL: {auth_url}")
            swift_conn = swiftclient.client.Connection(
                authurl=auth_url,
                user=BLOMP_EMAIL,
                key=BLOMP_PASSWORD,
                tenant_name=SWIFT_TENANT_NAME,
                auth_version="2"
            )
            swift_conn.get_account()  # Test
            logger.info(f"Success with {auth_url}")
            return
        except Exception as e:
            logger.warning(f"Failed with {auth_url}: {str(e)}")
    raise RuntimeError("All Blomp auth URLs failed - check credentials or try manual test")

upload_queue = queue.Queue()

def upload_worker():
    while True:
        item = upload_queue.get()
        if item is None:
            break
        temp_path, object_name, message = item
        try:
            with open(temp_path, "rb") as f:
                swift_conn.put_object(
                    container=BLOMP_EMAIL,
                    obj=f"{DESTINATION_FOLDER}/{object_name}",
                    contents=f,
                    content_type="video/mp4" if object_name.lower().endswith((".mp4", ".mkv")) else "application/octet-stream"
                )
            logger.info(f"Uploaded: {object_name}")
            message.reply_text(f"Uploaded: {object_name}")
        except Exception as e:
            logger.error(f"Upload fail {object_name}: {str(e)}\n{traceback.format_exc()}")
            message.reply_text(f"Upload failed: {str(e)}")
        finally:
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except:
                    pass
        upload_queue.task_done()

threading.Thread(target=upload_worker, daemon=True).start()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Send videos/files – I'll queue & upload to Blomp.")

async def handle_files(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global swift_conn
    if swift_conn is None:
        try:
            init_swift()
        except Exception as e:
            await update.message.reply_text(f"Blomp connection failed: {str(e)}")
            return

    message = update.message
    files_to_process = []

    if message.video:
        files_to_process.append((message.video, message.video.file_name or f"video_{message.message_id}.mp4"))
    elif message.document:
        files_to_process.append((message.document, message.document.file_name or f"file_{message.message_id}"))

    if not files_to_process:
        await message.reply_text("Send a video or file.")
        return

    await message.reply_text(f"Processing {len(files_to_process)} file(s)...")

    for file_obj, file_name in files_to_process:
        temp_path = f"/tmp/{file_name}"
        try:
            file = await file_obj.get_file()
            await file.download_to_drive(custom_path=temp_path)
            upload_queue.put((temp_path, file_name, message))
        except Exception as e:
            logger.error(f"Download fail for {file_name}: {str(e)}\n{traceback.format_exc()}")
            await message.reply_text(f"Failed to download {file_name}: {str(e)} (check size/network)")

def main():
    if not all([TELEGRAM_TOKEN, BLOMP_EMAIL, BLOMP_PASSWORD]):
        raise ValueError("Missing env vars")

    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.VIDEO | filters.Document.ALL, handle_files))

    logger.info("Bot polling started")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
