FROM python:3.11-slim
ENV TZ=UTC PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY simulator.py run.py ./
ENTRYPOINT ["python", "run.py"]
