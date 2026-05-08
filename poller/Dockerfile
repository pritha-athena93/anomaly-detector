FROM python:3.11-slim

WORKDIR /app

COPY poller/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY poller/poller.py ./poller/poller.py

CMD ["python", "poller/poller.py"]
