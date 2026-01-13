import os
import logging
import queue
import threading
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    MessageHandler,
    filters,
    CommandHandler,
    ContextTypes,
)
from blomp_api import Blomp  # Unofficial client

# Setup logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# Environment variables (set in Railway)
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
BLOMP_EMAIL = os.getenv("BLOMP_EMAIL")
BLOMP_PASSWORD = os.getenv("BLOMP_PASSWORD")
DESTINATION_FOLDER = "/Videos"  # Change to your Blomp folder path

# Initialize Blomp client
blomp = None

def init_blomp():
    global blomp
    try:
        blomp = Blomp(BLOMP_EMAIL, BLOMP_PASSWORD)
        logger.info("Blomp login successful")
    except Exception as e:
        logger.error(f"Blomp login failed: {e}")
        raise

# Upload queue for bulk/sequential processing
upload_queue = queue.Queue()
def upload_worker():
    while True:
        temp_path, file_name, message = upload_queue.get()
        if temp_path is None:
            break
        try:
            blomp.upload_file(temp_path, destination_path=f"{DESTINATION_FOLDER}/{file_name}")
            message.reply_text(f"Upload successful: {file_name}")
        except Exception as e:
            logger.error(f"Upload failed for {file_name}: {e}")
            message.reply_text(f"Upload failed for {file_name}: {str(e)}")
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)
        upload_queue.task_done()

# Start worker thread
threading.Thread(target=upload_worker, daemon=True).start()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Hi! Send me videos/files (single or multiple in an album/group). I'll upload them to Blomp one by one for bulk."
    )

async def handle_files(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not blomp:
        init_blomp()

    message = update.message
    files = []

    # Handle videos/docs (single or in media group/album)
    if message.video:
        files.append((message.video, message.video.file_name or f"video_{message.message_id}.mp4"))
    if message.document:
        files.append((message.document, message.document.file_name or f"file_{message.message_id}"))

    # If part of a media group, Telegram sends separate updates—use context to collect if needed, but for simplicity, process per message

    if not files:
        await message.reply_text("Please send videos or files.")
        return

    await message.reply_text(f"Processing {len(files)} file(s) for upload...")

    for file_obj, file_name in files:
        temp_path = f"/tmp/{file_name}"
        await file_obj.get_file().download_to_drive(custom_path=temp_path)
        upload_queue.put((temp_path, file_name, message))  # Queue for sequential upload

def main():
    if not TELEGRAM_TOKEN:
        raise ValueError("TELEGRAM_TOKEN not set")

    init_blomp()  # Login at startup

    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.VIDEO | filters.Document.ALL, handle_files))

    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
