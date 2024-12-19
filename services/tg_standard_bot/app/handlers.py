# File location: services/tg_standard_bot/app/handlers.py

from telegram import Update
from telegram.ext import CallbackContext
import logging
from .kafka_producer import KafkaMessageProducer

logger = logging.getLogger("handlers")

class InstructionHandler:
    def __init__(self, kafka_producer: KafkaMessageProducer):
        self.kafka_producer = kafka_producer

    def start_instruction(self, update: Update, context: CallbackContext):
        user = update.effective_user
        logger.info(f"Получена команда /start_instruction от пользователя {user.username} (ID: {user.id})")

        instruction = {
            "action": "start",
            "user_id": user.id,
            "username": user.username,
            "timestamp": update.message.date.isoformat()
        }
        self.kafka_producer.send_instruction(instruction)
        update.message.reply_text("Инструкция на начало выполнения отправлена.")

    def stop_instruction(self, update: Update, context: CallbackContext):
        user = update.effective_user
        logger.info(f"Получена команда /stop_instruction от пользователя {user.username} (ID: {user.id})")

        instruction = {
            "action": "stop",
            "user_id": user.id,
            "username": user.username,
            "timestamp": update.message.date.isoformat()
        }
        self.kafka_producer.send_instruction(instruction)
        update.message.reply_text("Инструкция на завершение выполнения отправлена.")

    def add_instruction_command(self, update: Update, context: CallbackContext):
        user = update.effective_user
        logger.info(f"Получена команда /add_instruction от пользователя {user.username} (ID: {user.id})")

        if len(context.args) < 1:
            update.message.reply_text("Пожалуйста, укажите название инструкции. Пример: /add_instruction new_task")
            return

        instruction_name = context.args[0]

        instruction = {
            "action": "add_instruction",
            "instruction_name": instruction_name,
            "user_id": user.id,
            "username": user.username,
            "timestamp": update.message.date.isoformat()
        }
        self.kafka_producer.send_instruction(instruction)
        update.message.reply_text(f"Инструкция '{instruction_name}' добавлена.")
