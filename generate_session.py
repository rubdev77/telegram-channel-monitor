"""One-time helper: log in with your phone number and print the Pyrogram
session string to paste into SESSION_STRING. Run locally, never on the server."""

import os

from dotenv import load_dotenv
from pyrogram import Client

load_dotenv()

with Client(
    "session_gen",
    api_id=int(os.environ["API_ID"]),
    api_hash=os.environ["API_HASH"],
    in_memory=True,
) as app:
    print("\nYour SESSION_STRING (keep it secret!):\n")
    print(app.export_session_string())
