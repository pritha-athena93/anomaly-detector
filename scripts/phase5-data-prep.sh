#!/bin/bash
# Run on control-plane (or any node with enough disk)
# Phase 5: download datasets, run feature engineering
set -e

DATA_DIR=~/anomaly-detector/data

mkdir -p ${DATA_DIR}/raw/flaws
mkdir -p ${DATA_DIR}/raw/otrf
mkdir -p ${DATA_DIR}/raw/invictus
mkdir -p ${DATA_DIR}/processed
mkdir -p ${DATA_DIR}/federated/node1
mkdir -p ${DATA_DIR}/federated/node2

# flaws.cloud CloudTrail logs (~240MB compressed)
echo "Downloading flaws.cloud dataset..."
wget -q -O ${DATA_DIR}/raw/flaws/flaws_cloudtrail_logs.tar.gz \
  https://summitroute.com/downloads/flaws_cloudtrail_logs.tar.gz
tar -xzf ${DATA_DIR}/raw/flaws/flaws_cloudtrail_logs.tar.gz -C ${DATA_DIR}/raw/flaws/
# each log file is gzip — decompress all
find ${DATA_DIR}/raw/flaws -name "*.gz" -exec gunzip -f {} \;

# OTRF Security-Datasets (attack scenarios mapped to MITRE ATT&CK)
echo "Cloning OTRF Security-Datasets..."
git clone --depth=1 https://github.com/OTRF/Security-Datasets.git ${DATA_DIR}/raw/otrf

# invictus-ir Stratus Red Team scenarios
echo "Cloning invictus-ir aws_dataset..."
git clone --depth=1 https://github.com/invictus-ir/aws_dataset.git ${DATA_DIR}/raw/invictus

# install python deps
pip install -q pandas pyarrow scikit-learn tqdm

# run feature engineering
echo "Building feature dataset..."
cd ~/anomaly-detector
python data/build_dataset.py

# verify
python data/verify_dataset.py
