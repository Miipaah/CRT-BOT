import discord
from discord.ext import commands
import requests
import pandas as pd
import sqlite3
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Configuration
CLOSED_CATEGORY_ID = int(os.getenv('CLOSED_CATEGORY_ID'))
OPEN_CATEGORY_ID = int(os.getenv('OPEN_CATEGORY_ID'))
BOT_TOKEN = os.getenv('BOT_TOKEN')
GUILD_ID = int(os.getenv('GUILD_ID')) # replace with your actual Guild ID

# Initialize the bot
intents = discord.Intents.all()
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents=intents)

# Database functions
def fetch_data_from_google_sheets_csv(url):
    response = requests.get(url)
    if response.status_code == 200:
        df = pd.read_csv(url)
        return df
    else:
        print("Failed to fetch data")
        return None

def sync_database():
    google_sheets_csv_url = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vSrYWEdNTlrNq3mszXr2dU8TJMkPNVLjTSo9ym3MoHoceTuBJuFoGR-GUfcnWqeyMp5jfTVX8GS1AfD/pub?gid=0&single=true&output=csv'
    dataframe = fetch_data_from_google_sheets_csv(google_sheets_csv_url)
    if dataframe is not None:
        return update_database(dataframe)

def update_database(dataframe):
    conn = sqlite3.connect('example.db')
    cur = conn.cursor()

    # Create the table if it doesn't exist
    cur.execute('''
        CREATE TABLE IF NOT EXISTS users (
            username TEXT DEFAULT NULL,
            user_ID INTEGER DEFAULT NULL,
            channel_ID INTEGER DEFAULT NULL,
            Open BOOLEAN DEFAULT FALSE,
            Active BOOLEAN DEFAULT NULL
        )
    ''')

    # Fetch current data from the database
    cur.execute('SELECT * FROM users')
    existing_data = cur.fetchall()
    existing_data_dict = {row[1]: row for row in existing_data}  # Mapping user_ID to row data

    changes = []

    # Iterate through the dataframe and update the database
    for index, row in dataframe.iterrows():
        try:
            username = row['Username']
            user_ID = row['Discord ID']
            Active = bool(row['ACTIVE'])

            print(f"Processing row: Username={username}, User_ID={user_ID}, Active={Active}")

            if user_ID in existing_data_dict:
                # Check for changes
                existing_row = existing_data_dict[user_ID]
                if existing_row[0] != username or existing_row[4] != Active:
                    # Update the existing record
                    cur.execute('''
                        UPDATE users 
                        SET username = ?, Active = ? 
                        WHERE user_ID = ?
                    ''', (username, Active, user_ID))
                    changes.append((username, user_ID, 'Updated'))
                    print(f"Updated: Username={username}, User_ID={user_ID}")
            else:
                # Insert new record with default values for channel_ID and Open
                cur.execute('''
                    INSERT INTO users (username, user_ID, channel_ID, Open, Active)
                    VALUES (?, ?, NULL, FALSE, ?)
                ''', (username, user_ID, Active))
                changes.append((username, user_ID, 'Inserted'))
                print(f"Inserted: Username={username}, User_ID={user_ID}")

        except Exception as e:
            print(f"Error processing row: {row}, Error: {e}")

    # Fetch users with Active = True and channel_ID = Null
    cur.execute('SELECT username, user_ID FROM users WHERE Active = 1 AND channel_ID IS NULL')
    users_to_update = cur.fetchall()

    # Commit the changes and close the connection
    conn.commit()
    conn.close()

    return users_to_update, changes

def update_channel_id(user_ID, channel_id):
    conn = sqlite3.connect('example.db')
    cur = conn.cursor()
    cur.execute('''
        UPDATE users 
        SET channel_ID = ? 
        WHERE user_ID = ?
    ''', (channel_id, user_ID))
    conn.commit()
    conn.close()

def get_channel_id(user_id):
    conn = sqlite3.connect('example.db')
    cur = conn.cursor()
    cur.execute('SELECT channel_ID FROM users WHERE user_ID = ?', (user_id,))
    result = cur.fetchone()
    conn.close()
    return result[0] if result else None

def get_user_id(channel_id):
    conn = sqlite3.connect('example.db')
    cur = conn.cursor()
    cur.execute('SELECT user_ID FROM users WHERE channel_ID = ?', (channel_id,))
    result = cur.fetchone()
    conn.close()
    return result[0] if result else None

def set_channel_open(user_ID, open_status):
    conn = sqlite3.connect('example.db')
    cur = conn.cursor()
    cur.execute('''
        UPDATE users 
        SET Open = ? 
        WHERE user_ID = ?
    ''', (open_status, user_ID))
    conn.commit()
    conn.close()

# Helper function to get all channel IDs
def get_all_channels():
    conn = sqlite3.connect('example.db')
    cur = conn.cursor()
    cur.execute('SELECT channel_ID FROM users WHERE channel_ID IS NOT NULL')
    result = cur.fetchall()
    conn.close()
    return result

# DM Relay functionality
@bot.event
async def on_message(message):
    # Ignore bot's own messages
    if message.author == bot.user:
        return

    # Process commands first
    if message.content.startswith(bot.command_prefix):
        await bot.process_commands(message)
        return
    
    if isinstance(message.channel, discord.DMChannel):
        # Message is a DM to the bot
        user_id = message.author.id
        channel_id = get_channel_id(user_id)
        
        if channel_id:
            channel = bot.get_channel(channel_id)
            if channel:
                # Move channel to Open category and set Open status to True
                category = bot.get_channel(OPEN_CATEGORY_ID)
                await channel.edit(category=category)
                set_channel_open(user_id, True)

                # Create embed with user message
                embed = discord.Embed(description=message.content, color=0x00ff00)
                embed.set_author(name=message.author.display_name, icon_url=message.author.avatar.url)

                # Attach any attachments to the embed
                if message.attachments:
                    for attachment in message.attachments:
                        file = await attachment.to_file()
                        await channel.send(embed=embed, file=file)
                else:
                    await channel.send(embed=embed)
        else:
            # Send a response to non-database users or users without a channel ID
            embed = discord.Embed(
                description="We don't currently have you listed as a Scheduled streamer in the Discord for this Charity Raid Train. If you need assistance from the Admin team or another staff member, please use the various staff chat channels available in the main Discord.",
                color=0xff0000  # Red color
            )
            await message.author.send(embed=embed)

    elif message.channel.id in [ch_id[0] for ch_id in get_all_channels()]:
        # Message is in a channel associated with a user
        user_id = get_user_id(message.channel.id)
        user = bot.get_user(user_id)

        if user:
            # Move channel to Open category and set Open status to True
            category = bot.get_channel(OPEN_CATEGORY_ID)
            await message.channel.edit(category=category)
            set_channel_open(user_id, True)

            # Create embed with channel message
            embed = discord.Embed(description=message.content, color=0x00ff00)
            embed.set_author(name=message.author.display_name, icon_url=message.author.avatar.url)

            # Attach any attachments to the embed
            if message.attachments:
                for attachment in message.attachments:
                    file = await attachment.to_file()
                    await user.send(embed=embed, file=file)
            else:
                await user.send(embed=embed)

@bot.command(name='close')
@commands.has_role('staff')  # Require the 'staff' role
async def close(ctx):
    channel_id = ctx.channel.id
    user_id = get_user_id(channel_id)
    if user_id:
        user = bot.get_user(user_id)
        if user:
            # Move channel to Closed category and set Open status to False
            category = bot.get_channel(CLOSED_CATEGORY_ID)
            await ctx.channel.edit(category=category)
            set_channel_open(user_id, False)

            await user.send("Your ticket has been closed by staff.")

    await ctx.send("This channel has been closed.")

@bot.command(name='sync')
@commands.has_role('staff')  # Require the 'staff' role
async def sync(ctx):
    users_to_update, changes = sync_database()

    # Create a message with the changes
    if changes:
        changes_msg = '\n'.join([f"{change[2]}: Username={change[0]}, User_ID={change[1]}" for change in changes])
        await ctx.send(f"Database synced. Changes made:\n{changes_msg}")
    else:
        await ctx.send("Database synced. No changes were necessary.")

    # Update channel ID for users with Active = 1 and channel_ID = NULL
    for username, user_id in users_to_update:
        guild = bot.get_guild(GUILD_ID)
        member = guild.get_member(user_id)
        if member:
            overwrites = {
                guild.default_role: discord.PermissionOverwrite(read_messages=False),
                member: discord.PermissionOverwrite(read_messages=True, send_messages=True),
                guild.me: discord.PermissionOverwrite(read_messages=True, send_messages=True)
            }
            category = bot.get_channel(CLOSED_CATEGORY_ID)
            channel = await guild.create_text_channel(name=f"ticket-{username}", category=category, overwrites=overwrites)
            update_channel_id(user_id, channel.id)

@bot.command(name='dmall')
@commands.has_role('staff')  # Require the 'staff' role
async def dmall(ctx, *, message: str):
    conn = sqlite3.connect('example.db')
    cur = conn.cursor()
    
    # Fetch all users with a channel ID
    cur.execute('SELECT user_ID FROM users WHERE channel_ID IS NOT NULL')
    users = cur.fetchall()
    conn.close()

    if users:
        for user_id in users:
            user = bot.get_user(user_id[0])
            if user:
                try:
                    await user.send(message)
                except discord.Forbidden:
                    await ctx.send(f"Cannot send message to {user.display_name}.")
        await ctx.send("Message sent to all users with a channel ID.")
    else:
        await ctx.send("No users found with a channel ID.")

# Error handling
@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send("You do not have the required role to use this command.")
    else:
        raise error

bot.run(BOT_TOKEN)
