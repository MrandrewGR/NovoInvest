
# utils/libs/mirco_services_data_management/setup.py

from setuptools import setup, find_packages

setup(
    name="mirco_services_data_management",
    version="0.1.0",
    author="Your Name",
    author_email="you@example.com",
    description="Внутренняя библиотека для микросервисов (Kafka, PostgreSQL, дубли, партиционирование).",
    packages=find_packages(),  # автоматически найдёт mirco_services_data_management/
    install_requires=[
        "kafka-python",
        "psycopg2-binary",
        # etc... но обычно они в вашем проекте в любом случае
    ],
    python_requires=">=3.8"
)
