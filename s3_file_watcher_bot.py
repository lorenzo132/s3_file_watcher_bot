import os
import json
from datetime import datetime, timezone

import discord
import boto3
from discord.ext import tasks, commands
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Environment variables
DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
DISCORD_CHANNEL_ID = int(os.getenv("DISCORD_CHANNEL_ID"))

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_ENDPOINT = os.getenv("S3_ENDPOINT")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

# JSON file to persist sent files
SENT_FILES_PATH = "sent_files.json"

def load_sent_files():
    """Load the list of already-sent files from disk."""
    if os.path.exists(SENT_FILES_PATH):
        with open(SENT_FILES_PATH, "r") as f:
            return set(json.load(f))
    return set()

def save_sent_files(files):
    """Save the list of sent files to disk."""
    with open(SENT_FILES_PATH, "w") as f:
        json.dump(list(files), f)

# Track files that have already triggered notifications
sent_files = load_sent_files()

# Initialize S3 client
s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    endpoint_url=S3_ENDPOINT,
)

# Set up Discord bot
intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)

def get_file_metadata(obj):
    """Extract file size (MB) and creation timestamp from S3 object metadata."""
    size_mb = round(obj["Size"] / (1024 * 1024), 2)
    created_time = obj["LastModified"].replace(tzinfo=timezone.utc).timestamp()
    return size_mb, int(created_time)

def build_download_url(filename):
    """Construct the public Contabo download URL."""
    return f"{S3_ENDPOINT}/{AWS_ACCESS_KEY_ID}:{S3_BUCKET_NAME}/{filename}"

async def send_file_embed(channel, filename, size_mb, timestamp, download_url):
    """Send a rich embed to the Discord channel."""
    embed = discord.Embed(
        title=f"🆕 New File Uploaded: `{filename}`",
        color=0x00ff99,
        timestamp=datetime.utcnow()
    )
    embed.add_field(name="File Size", value=f"{size_mb} MB", inline=True)
    embed.add_field(name="Created At", value=f"<t:{timestamp}:F>", inline=True)
    embed.add_field(name="Download", value=f"[Click here]({download_url})", inline=False)

    await channel.send(embed=embed)

@tasks.loop(seconds=8)  # Check every 8 seconds
async def monitor_bucket():
    global sent_files
    try:
        response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME)
        contents = response.get("Contents", [])
        new_files = []

        for obj in contents:
            key = obj["Key"]
            if key not in sent_files:
                sent_files.add(key)
                new_files.append(obj)

        if new_files:
            save_sent_files(sent_files)
            channel = bot.get_channel(DISCORD_CHANNEL_ID)
            for obj in new_files:
                filename = obj["Key"]
                size_mb, timestamp = get_file_metadata(obj)
                download_url = build_download_url(filename)
                await send_file_embed(channel, filename, size_mb, timestamp, download_url)

    except Exception as e:
        print(f"[ERROR] Failed to monitor bucket: {e}")

@bot.event
async def on_ready():
    print(f"[INFO] Logged in as {bot.user}!")
    monitor_bucket.start()

if __name__ == "__main__":
    bot.run(DISCORD_BOT_TOKEN)
