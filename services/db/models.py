from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, DateTime, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import func

Base = declarative_base()

class Messages(Base):
    __tablename__ = "messages"

    id = Column(Integer, primary_key=True, autoincrement=True)
    data = Column(JSONB, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
