FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    flwr==1.8.0 \
    scikit-learn==1.4.2 \
    pandas==2.2.2 \
    pyarrow==15.0.2 \
    mlflow==2.13.0 \
    boto3==1.34.0

COPY fl/ ./fl/

# SERVER: docker run -e FL_ROUNDS=10 <image> python fl/fl_server.py
# CLIENT: docker run -e DATA_PATH=/data/node1/features.parquet -e NODE_ID=1 <image> python fl/fl_client.py
