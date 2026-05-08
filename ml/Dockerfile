FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    scikit-learn==1.4.2 \
    pandas==2.2.2 \
    pyarrow==15.0.2 \
    mlflow==2.13.0 \
    boto3==1.34.0

COPY ml/trainer.py ./ml/

CMD ["python", "ml/trainer.py"]
