from telethon import TelegramClient
import asyncio

api_id = 19890265
api_hash = '1a1fb40bc6fb9e6475f443742f4c4fe5'
phone = '+381631899159'
password = 'sact0cugh0lick!GRAW'  # Ваш пароль для 2FA

client = TelegramClient('session_name123', api_id, api_hash)

async def main():
    await client.start(phone)
    if await client.is_user_authorized():
        print("Уже авторизован")
    else:
        await client.send_code_request(phone)
        code = input('Введите код: ')
        try:
            await client.sign_in(phone, code)
        except Exception as e:
            if 'PASSWORD_HASH_INVALID' in str(e):
                await client.sign_in(password=password)
    print("Авторизация успешна")

asyncio.run(main())
