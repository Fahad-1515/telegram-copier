"""
tg_forwarder.py  —  core copy engine
Supports: pause/resume via an asyncio.Event, per-job progress tracking
"""

import asyncio
import json
import os
from datetime import datetime

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.errors import FloodWaitError, ChatWriteForbiddenError

load_dotenv()

API_ID   = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")

DATA_DIR = os.getenv("DATA_DIR", ".")
SESSION  = os.path.join(DATA_DIR, "sessions/forwarder")
LOG_FILE = os.path.join(DATA_DIR, "logs/forwarder.log")

DELAY      = 3
ALBUM_WAIT = 1

os.makedirs(os.path.join(DATA_DIR, "logs"),     exist_ok=True)
os.makedirs(os.path.join(DATA_DIR, "progress"), exist_ok=True)
os.makedirs(os.path.join(DATA_DIR, "sessions"), exist_ok=True)


def log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def _pfile(src, dst) -> str:
    return os.path.join(DATA_DIR, f"progress/{src}_{dst}.json")


def load_progress(src, dst) -> dict:
    path = _pfile(src, dst)
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {"last_id": 0, "forwarded": 0, "errors": 0}


def save_progress(src, dst, last_id: int, forwarded: int, errors: int):
    try:
        with open(_pfile(src, dst), "w") as f:
            json.dump(
                {"last_id": last_id, "forwarded": forwarded, "errors": errors},
                f
            )
    except Exception as e:
        log(f"⚠️ Progress save failed: {e}")


async def safe_send(coro):
    while True:
        try:
            await coro
            return True
        except FloodWaitError as e:
            wait = e.seconds + 10
            log(f"⏳ FloodWait — sleeping {wait}s")
            await asyncio.sleep(wait)
        except ChatWriteForbiddenError:
            log("❌ No write permission in destination.")
            return False
        except Exception as e:
            log(f"✗ Send error: {type(e).__name__}: {e}")
            return False


async def flush_album(client, dst, gid, msgs, forwarded, errors, src):
    if not msgs:
        return forwarded, errors

    msgs.sort(key=lambda m: m.id)
    files = [m.media for m in msgs if m.media]
    caption = next((m.text for m in msgs if m.text), "") or ""

    ok = True
    if files:
        ok = await safe_send(
            client.send_file(dst, files, caption=caption)
        )

    if ok:
        forwarded += len(msgs)
        log(f"✓ Album ({len(msgs)}) sent | last ID {msgs[-1].id}")
    else:
        errors += 1

    save_progress(src, dst, msgs[-1].id, forwarded, errors)
    await asyncio.sleep(DELAY)

    return forwarded, errors


async def main(src: int, dst: int, pause_event: asyncio.Event = None):

    if not API_ID or not API_HASH:
        log("❌ Missing API credentials in .env")
        return

    if pause_event is None:
        pause_event = asyncio.Event()
        pause_event.set()

    p = load_progress(src, dst)
    last_id   = p["last_id"]
    forwarded = p["forwarded"]
    errors    = p["errors"]

    log(f"▶ Job {src}→{dst} | Resume from msg ID: {last_id or 'beginning'}")

    album_buffer: dict = {}

    async with TelegramClient(SESSION, API_ID, API_HASH) as client:

        me = await client.get_me()
        log(f"Logged in as {me.first_name}")

        async for message in client.iter_messages(src, reverse=True, min_id=last_id):

            if not pause_event.is_set():
                log(f"⏸ Paused at msg {message.id}")
                await pause_event.wait()
                log(f"▶ Resumed at msg {message.id}")

            try:
                if not message.text and not message.media:
                    continue

                if message.grouped_id:
                    album_buffer.setdefault(message.grouped_id, []).append(message)
                    await asyncio.sleep(0.3)
                    continue

                for gid in list(album_buffer.keys()):
                    forwarded, errors = await flush_album(
                        client, dst,
                        gid,
                        album_buffer.pop(gid),
                        forwarded,
                        errors,
                        src
                    )

                if message.media:
                    ok = await safe_send(
                        client.send_file(dst, message.media, caption=message.text or "")
                    )
                elif message.text:
                    ok = await safe_send(
                        client.send_message(dst, message.text)
                    )
                else:
                    ok = True

                if ok:
                    forwarded += 1
                    save_progress(src, dst, message.id, forwarded, errors)
                else:
                    errors += 1

                await asyncio.sleep(DELAY)

            except ChatWriteForbiddenError:
                log("❌ Forbidden in destination, stopping job")
                return

            except asyncio.CancelledError:
                save_progress(src, dst, message.id, forwarded, errors)
                log(f"🛑 Cancelled at {message.id}")
                raise

            except Exception as e:
                errors += 1
                log(f"✗ Skip {message.id}: {e}")
                await asyncio.sleep(3)

        for gid in list(album_buffer.keys()):
            forwarded, errors = await flush_album(
                client, dst,
                gid,
                album_buffer.pop(gid),
                forwarded,
                errors,
                src
            )

    log(f"✅ Done | Sent: {forwarded} | Errors: {errors}")