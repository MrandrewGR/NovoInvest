# services/data_processor/app/utils.py

import os

def ensure_dir(directory):
    """
    Создаёт директорию, если она не существует.
    """
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
