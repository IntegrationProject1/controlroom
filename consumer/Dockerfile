# Makes a container for the publisher
FROM python:3.10-slim

WORKDIR /app

COPY consumer.py /app/
COPY requirements.txt /app/

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "consumer.py"]