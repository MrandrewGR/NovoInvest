# app/models.py
from sqlalchemy import Column, Integer, String, Text, DateTime
from services.data_processor.app.database import Base
import datetime

class TelegramMessage(Base):
    __tablename__ = "telegram_messages"

    id = Column(Integer, primary_key=True, index=True)
    message_id = Column(Integer, unique=True, index=True, nullable=False)
    chat_id = Column(String, index=True, nullable=False)
    date = Column(DateTime, default=datetime.datetime.utcnow)
    content = Column(Text, nullable=True)
    media_path = Column(String, nullable=True)
