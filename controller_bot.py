"""
controller_bot.py
"""

import os
import json
import asyncio

from dotenv import load_dotenv
load_dotenv()

from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ConversationHandler,
    ContextTypes,
    filters,
)

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError

from tg_forwarder import main as run_forwarder, load_progress

BOT_TOKEN = os.getenv("BOT_TOKEN")

API_ID   = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")

DATA_DIR  = os.getenv("DATA_DIR", ".")
JOBS_FILE = os.path.join(DATA_DIR, "jobs.json")
SESSION   = os.path.join(DATA_DIR, "sessions/forwarder")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")

os.makedirs(os.path.join(DATA_DIR, "logs"),     exist_ok=True)
os.makedirs(os.path.join(DATA_DIR, "progress"), exist_ok=True)
os.makedirs(os.path.join(DATA_DIR, "sessions"), exist_ok=True)

jobs = {}

GET_PHONE, GET_OTP, GET_PASSWORD, GET_SOURCE, GET_DEST = range(5)


def save_jobs():
    data = {
        jid: {
            "src": j["src"],
            "dst": j["dst"],
            "status": j["status"],
        }
        for jid, j in jobs.items()
    }
    with open(JOBS_FILE, "w") as f:
        json.dump(data, f, indent=2)


def load_jobs():
    if not os.path.exists(JOBS_FILE):
        return
    with open(JOBS_FILE) as f:
        data = json.load(f)
    for jid, job in data.items():
        ev = asyncio.Event()
        ev.set()
        status = job["status"]
        if status == "running":
            status = "stopped"
        jobs[jid] = {
            "src": job["src"],
            "dst": job["dst"],
            "status": status,
            "task": None,
            "pause_event": ev,
        }


# ─────────────────────────────────────────────
# LOGIN FLOW
# ─────────────────────────────────────────────

async def ensure_login(update, context):
    client = TelegramClient(SESSION, API_ID, API_HASH)
    await client.connect()

    if await client.is_user_authorized():
        await client.disconnect()
        return True

    context.user_data["login_client"] = client

    await update.message.reply_text(
        "📱 Send your Telegram phone number\n\n"
        "Example:\n"
        "+919876543210"
    )

    return GET_PHONE


async def get_phone(update, context):
    phone = update.message.text.strip()
    client = context.user_data["login_client"]

    try:
        sent = await client.send_code_request(phone)
        context.user_data["phone"] = phone
        context.user_data["phone_code_hash"] = sent.phone_code_hash

        await update.message.reply_text("📨 OTP sent.\n\nSend OTP code.")
        return GET_OTP

    except Exception as e:
        await update.message.reply_text(f"❌ Login error:\n{e}")
        await client.disconnect()
        return ConversationHandler.END


async def get_otp(update, context):
    code = update.message.text.strip()
    client = context.user_data["login_client"]

    try:
        await client.sign_in(
            phone=context.user_data["phone"],
            code=code,
            phone_code_hash=context.user_data["phone_code_hash"]
        )
        await client.disconnect()

        await update.message.reply_text("✅ Login successful.\n\nSend source ID")
        return GET_SOURCE

    except SessionPasswordNeededError:
        await update.message.reply_text("🔐 2FA enabled.\n\nSend your password.")
        return GET_PASSWORD

    except Exception as e:
        await update.message.reply_text(f"❌ OTP error:\n{e}")
        await client.disconnect()
        return ConversationHandler.END


async def get_password(update, context):
    password = update.message.text.strip()
    client = context.user_data["login_client"]

    try:
        await client.sign_in(password=password)
        await client.disconnect()

        await update.message.reply_text("✅ Login successful.\n\nSend source ID")
        return GET_SOURCE

    except Exception as e:
        await update.message.reply_text(f"❌ Password error:\n{e}")
        await client.disconnect()
        return ConversationHandler.END


# ─────────────────────────────────────────────
# JOB RUNNER
# ─────────────────────────────────────────────

async def _run_job(job_id: str):
    job = jobs[job_id]

    try:
        jobs[job_id]["status"] = "running"
        save_jobs()

        await run_forwarder(
            job["src"],
            job["dst"],
            job["pause_event"]
        )

        jobs[job_id]["status"] = "done"

    except asyncio.CancelledError:
        jobs[job_id]["status"] = "stopped"

    except Exception as e:
        jobs[job_id]["status"] = f"error: {e}"

    save_jobs()


def _start_job(job_id: str):
    task = asyncio.create_task(_run_job(job_id))
    jobs[job_id]["task"] = task
    return task


# ─────────────────────────────────────────────
# START
# ─────────────────────────────────────────────

async def cmd_start(update, context):
    result = await ensure_login(update, context)

    if result is True:
        await update.message.reply_text("Send source ID")
        return GET_SOURCE

    return result


async def get_source(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        context.user_data["src"] = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ Invalid source ID")
        return GET_SOURCE

    await update.message.reply_text("Send destination ID")
    return GET_DEST


async def get_dest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        dst = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ Invalid destination ID")
        return GET_DEST

    src = context.user_data["src"]
    job_id = f"{src}_{dst}"

    if (
        job_id in jobs and
        jobs[job_id].get("task") and
        not jobs[job_id]["task"].done()
    ):
        await update.message.reply_text("⚠️ Job already running")
        return ConversationHandler.END

    ev = asyncio.Event()
    ev.set()

    jobs[job_id] = {
        "src": src,
        "dst": dst,
        "status": "starting",
        "task": None,
        "pause_event": ev,
    }

    _start_job(job_id)
    save_jobs()

    await update.message.reply_text(f"🚀 Started job:\n{job_id}")
    return ConversationHandler.END


# ─────────────────────────────────────────────
# STOP
# ─────────────────────────────────────────────

async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        return await update.message.reply_text("Usage:\n/stop <job_id>")

    job_id = context.args[0]
    job = jobs.get(job_id)

    if not job:
        return await update.message.reply_text("❌ Unknown job")

    task = job.get("task")
    if task and not task.done():
        task.cancel()
        await asyncio.sleep(0.1)

    job["status"] = "stopped"
    save_jobs()

    await update.message.reply_text("🛑 Stopped")


# ─────────────────────────────────────────────
# PAUSE
# ─────────────────────────────────────────────

async def cmd_pause(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        return await update.message.reply_text("Usage:\n/pause <job_id>")

    job_id = context.args[0]
    job = jobs.get(job_id)

    if not job:
        return await update.message.reply_text("❌ Unknown job")

    job["pause_event"].clear()
    job["status"] = "paused"
    save_jobs()

    await update.message.reply_text("⏸ Paused")


# ─────────────────────────────────────────────
# RESUME
# ─────────────────────────────────────────────

async def cmd_resume(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        return await update.message.reply_text("Usage:\n/resume <job_id>")

    job_id = context.args[0]
    job = jobs.get(job_id)

    if not job:
        return await update.message.reply_text("❌ Unknown job")

    job["pause_event"].set()

    if job["status"] != "running":
        job["status"] = "running"
        save_jobs()

    await update.message.reply_text("▶️ Resumed")


# ─────────────────────────────────────────────
# LIST
# ─────────────────────────────────────────────

async def cmd_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not jobs:
        return await update.message.reply_text("No jobs")

    msg = ""
    for jid, j in jobs.items():
        p = load_progress(j["src"], j["dst"])
        msg += (
            f"{jid}\n"
            f"Status: {j['status']}\n"
            f"Sent: {p['forwarded']}\n"
            f"Errors: {p['errors']}\n"
            f"Last ID: {p['last_id']}\n\n"
        )

    await update.message.reply_text(msg)


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    load_jobs()

    app = Application.builder().token(BOT_TOKEN).build()

    conv = ConversationHandler(
        entry_points=[
            CommandHandler("start", cmd_start)
        ],
        states={
            GET_PHONE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, get_phone)
            ],
            GET_OTP: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, get_otp)
            ],
            GET_PASSWORD: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, get_password)
            ],
            GET_SOURCE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, get_source)
            ],
            GET_DEST: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, get_dest)
            ],
        },
        fallbacks=[],
    )

    app.add_handler(conv)
    app.add_handler(CommandHandler("stop",   cmd_stop))
    app.add_handler(CommandHandler("pause",  cmd_pause))
    app.add_handler(CommandHandler("resume", cmd_resume))
    app.add_handler(CommandHandler("list",   cmd_list))

    print("✅ Bot running...")
    app.run_polling()


if __name__ == "__main__":
    main()