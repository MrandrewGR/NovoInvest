# app/handlers/__init__.py

from .channel_handler import register_channel_handler
from .chat_handler import register_chat_handler

def register_handlers(client, kafka_producer, counter):
    register_channel_handler(client, kafka_producer, counter)
    register_chat_handler(client, kafka_producer, counter)
