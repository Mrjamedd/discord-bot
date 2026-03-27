FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN useradd --create-home appuser

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /app/runtime /app/credentials /home/ubuntu/discord-bot/assets \
    && chown -R appuser:appuser /app /home/ubuntu

USER appuser

CMD ["python", "run_bot.py"]
