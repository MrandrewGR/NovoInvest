from telethon import TelegramClient
import base64
import asyncio

api_id = 23718355
api_hash = 'cf1637bd7d4517d2f1db3b6e6d628f45'
phone = '+917778937094'
password = 'sact0cugh0lick!GRAW'  # Ваш пароль для 2FA


client = TelegramClient('session_name', api_id, api_hash)


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
            else:
                print(f"Ошибка авторизации: {e}")
                return  # Прерываем выполнение, если ошибка другая

    print("Авторизация успешна")

    # Путь к файлу сессии
    session_file = 'session_name.session'

    try:
        # Чтение файла сессии в бинарном режиме
        with open(session_file, 'rb') as f:
            session_data = f.read()

        # Кодирование данных в Base64
        session_b64 = base64.b64encode(session_data).decode('utf-8')

        # Сохранение закодированных данных в новый файл
        with open('session_b64.txt', 'w') as f:
            f.write(session_b64)

        print("Файл сессии успешно закодирован в Base64 и сохранён в 'session_b64.txt'")
    except FileNotFoundError:
        print(f"Файл сессии '{session_file}' не найден.")
    except Exception as e:
        print(f"Произошла ошибка при кодировании файла сессии: {e}")


# Запуск асинхронной функции
asyncio.run(main())