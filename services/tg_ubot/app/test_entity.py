# File location: services/tg_ubot/app/test_entity.py
import asyncio
from telethon import TelegramClient
from app.config import settings

async def main():
    client = TelegramClient(settings.SESSION_FILE, settings.TELEGRAM_API_ID, settings.TELEGRAM_API_HASH)
    await client.start()
    entity = await client.get_entity(7079551)
    print("entity:", entity)
    print("broadcast =", getattr(entity, 'broadcast', False))
    print("megagroup =", getattr(entity, 'megagroup', False))
    print("entity.id =", entity.id)
    await client.disconnect()

asyncio.run(main())
