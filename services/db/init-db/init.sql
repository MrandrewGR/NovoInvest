-- Создание таблицы messages
CREATE TABLE IF NOT EXISTS messages (
    id SERIAL PRIMARY KEY,
    data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Создание GIN-индекса по полю data
CREATE INDEX IF NOT EXISTS idx_messages_data_gin
ON messages
USING GIN (data);
