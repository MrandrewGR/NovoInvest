# File: services/data_processor/app/models.py
import datetime
from sqlalchemy import Column, Integer, String, Text, DateTime
from .database import Base

# Пример таблицы tg_ubot (как в нашей миграции)
class TgUbot(Base):
    __tablename__ = "tg_ubot"

    id = Column(Integer, primary_key=True, index=True)
    message_id = Column(Integer, unique=True, index=True, nullable=False)
    chat_id = Column(String, index=True, nullable=False)
    date = Column(DateTime, default=datetime.datetime.utcnow)
    content = Column(Text, nullable=True)
    media_path = Column(String, nullable=True)
