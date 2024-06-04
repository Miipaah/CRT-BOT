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
@commands.has_role('Staff')  # Require the 'Staff' role
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

            # Notify the user
            embed = discord.Embed(
                description=f"{ctx.author.display_name} has closed this conversation. To open again, please send a DM.",
                color=0xff0000
            )
            await user.send(embed=embed)
            await ctx.send("Channel has been closed and user notified.")

@bot.command(name='dm')
@commands.has_role('Staff')  # Require the 'Staff' role
async def dm(ctx, *, args: str):
    mentions = ctx.message.mentions
    message_content = args.replace(' '.join([mention.mention for mention in mentions]), '').strip()

    if mentions:
        for user in mentions:
            try:
                await user.send(message_content)
                await ctx.send(f"Message sent to {user.display_name}.")
            except discord.Forbidden:
                await ctx.send(f"Cannot send message to {user.display_name}.")
    else:
        await ctx.send("You need to mention at least one user.")

@bot.command(name='dmall')
@commands.has_role('Staff')  # Require the 'Staff' role
async def dmall(ctx, *, message: str):
    guild = bot.get_guild(GUILD_ID)
    if guild:
        for member in guild.members:
            if not member.bot:
                try:
                    await member.send(message)
                except discord.Forbidden:
                    await ctx.send(f"Cannot send message to {member.display_name}.")
        await ctx.send("Message sent to all members.")
    else:
        await ctx.send("Guild not found.")

@bot.command(name='sync')
@commands.has_role('Staff')  # Require the 'Staff' role
async def sync(ctx):
    await ctx.send('Syncing database...')
    users_to_update, changes = sync_database()

    if not users_to_update and not changes:
        await ctx.send('Database is already up-to-date.')
        return

    # Create channels for users who are active and have no channel_ID
    category = bot.get_channel(CLOSED_CATEGORY_ID)
    if category is None:
        await ctx.send('Closed category not found.')
        return

    for username, user_ID in users_to_update:
        channel = await category.create_text_channel(name=username)
        update_channel_id(user_ID, channel.id)
        await ctx.send(f'Created channel {channel.name} for user {username}.')

    await ctx.send('Database synced successfully! Changes made:')
    for change in changes:
        await ctx.send(f'User: {change[0]}, Discord ID: {change[1]}, Change: {change[2]}')

# Error handling
@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send("You do not have the required role to use this command.")
    else:
        raise error

bot.run(BOT_TOKEN)
