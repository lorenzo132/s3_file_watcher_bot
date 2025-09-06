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
BUCKET_ID = os.getenv("BUCKET_ID")  # NEW: used in download URL

# JSON files to persist data
SENT_FILES_PATH = "sent_files.json"
FILE_MESSAGES_PATH = "file_messages.json"


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


def load_file_messages():
    """Load the mapping of files to Discord message IDs."""
    if os.path.exists(FILE_MESSAGES_PATH):
        with open(FILE_MESSAGES_PATH, "r") as f:
            return json.load(f)
    return {}


def save_file_messages(file_messages):
    """Save the mapping of files to Discord message IDs."""
    with open(FILE_MESSAGES_PATH, "w") as f:
        json.dump(file_messages, f)


# Track files that have already triggered notifications
sent_files = load_sent_files()
file_messages = load_file_messages()

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
    """Construct the public Contabo download URL using BUCKET_ID."""
    return f"{S3_ENDPOINT}/{BUCKET_ID}:{S3_BUCKET_NAME}/{filename}"


async def send_file_embed(channel, filename, size_mb, timestamp, download_url):
    """Send a rich embed to the Discord channel."""
    embed = discord.Embed(
        title=f"🆕 New File Uploaded: `{filename}`",
        color=0x00FF99,
        timestamp=datetime.utcnow(),
    )
    embed.add_field(name="File Size", value=f"{size_mb} MB", inline=True)
    embed.add_field(name="Created At", value=f"<t:{timestamp}:F>", inline=True)
    embed.add_field(
        name="Download", value=f"[Click here]({download_url})", inline=False
    )

    message = await channel.send(embed=embed)
    return message.id


@tasks.loop(seconds=8)  # Check every 8 seconds
async def monitor_bucket():
    global sent_files, file_messages
    try:
        response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME)
        contents = response.get("Contents", [])
        current_files = {obj["Key"] for obj in contents}

        # Check for new files
        new_files = []
        for obj in contents:
            key = obj["Key"]
            if key not in sent_files:
                sent_files.add(key)
                new_files.append(obj)

        # Check for deleted files
        deleted_files = sent_files - current_files

        channel = bot.get_channel(DISCORD_CHANNEL_ID)

        # Handle new files
        if new_files:
            for obj in new_files:
                filename = obj["Key"]
                size_mb, timestamp = get_file_metadata(obj)
                download_url = build_download_url(filename)
                message_id = await send_file_embed(
                    channel, filename, size_mb, timestamp, download_url
                )
                file_messages[filename] = message_id

        # Handle deleted files
        if deleted_files:
            for filename in deleted_files:
                if filename in file_messages:
                    try:
                        message = await channel.fetch_message(file_messages[filename])
                        await message.delete()
                    except discord.NotFound:
                        pass  # Message already deleted
                    except Exception as e:
                        print(f"[ERROR] Failed to delete message for {filename}: {e}")
                    del file_messages[filename]
                sent_files.discard(filename)

        # Save changes if any occurred
        if new_files or deleted_files:
            save_sent_files(sent_files)
            save_file_messages(file_messages)

    except Exception as e:
        print(f"[ERROR] Failed to monitor bucket: {e}")


@bot.event
async def on_ready():
    print(f"[INFO] Logged in as {bot.user}!")
    monitor_bucket.start()


if __name__ == "__main__":
    bot.run(DISCORD_BOT_TOKEN)
