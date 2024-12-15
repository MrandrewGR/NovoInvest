import asyncio
import os
from telethon import TelegramClient

# Получение переменных окружения
api_id = int(os.getenv('TELEGRAM_API_ID'))
api_hash = os.getenv('TELEGRAM_API_HASH')
phone = os.getenv('TELEGRAM_PHONE')

# Имя файла сессии
session_name = 'session_name'

client = TelegramClient(session_name, api_id, api_hash)

async def main():
    await client.connect()
    if not await client.is_user_authorized():
        print("Необходимо авторизоваться.")
        await client.send_code_request(phone)
        code = input('Введите код, полученный в Telegram: ')
        try:
            await client.sign_in(phone, code)
        except Exception as e:
            print(f"Ошибка авторизации: {e}")
            return
    else:
        print("Клиент уже авторизован.")
    print("Сессия создана успешно.")
    await client.disconnect()

if __name__ == '__main__':
    asyncio.run(main())
