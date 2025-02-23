from telethon import TelegramClient

client = TelegramClient('session_name', API_ID, API_HASH)

async def get_chat_id():
    await client.start()
    channel = await client.get_entity('https://t.me/your_channel_username')
    print(channel.id)

with client:
    client.loop.run_until_complete(get_chat_id())
