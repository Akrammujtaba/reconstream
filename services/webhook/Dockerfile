FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py replay_dlq.py ./
EXPOSE 8000
CMD ["uvicorn", "--factory", "app:app_from_env", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
