FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    langchain==0.2.12 \
    langchain-aws==0.1.16 \
    langchain-core==0.2.28 \
    langgraph==0.1.19 \
    fastapi==0.111.1 \
    uvicorn==0.30.3 \
    kubernetes==30.1.0 \
    mlflow==2.13.0 \
    kfp==2.9.0 \
    boto3==1.34.144 \
    faiss-cpu==1.8.0 \
    numpy==1.26.4 \
    requests==2.32.3 \
    kafka-python==2.0.2 \
    psycopg2-binary==2.9.9 \
    tenacity==8.3.0

COPY agent/ ./agent/
COPY k8s/manifests/katib/ ./k8s/manifests/katib/

CMD ["uvicorn", "agent.server:server", "--host", "0.0.0.0", "--port", "8000"]
