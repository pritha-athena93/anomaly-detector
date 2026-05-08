# katib/experiment.yaml

## What it does
Katib HPO experiment for IsolationForest hyperparameter tuning. Submitted by the ml-agent when it determines re-training is needed (`trigger_katib_hpo` tool).

## How it works
- **Algorithm**: Bayesian optimization (efficient for this 3-parameter space)
- **Parameters**: `n_estimators` (50–400), `contamination` (0.01–0.20), `max_samples` (0.5–1.0)
- **Objective**: maximize `roc_auc` (target: 0.95)
- **Trials**: up to 20, 3 in parallel
- Each trial runs `training-pipeline` image with `--katib_mode true` flag, which writes `roc_auc=<value>` to stdout (Katib StdOut collector)

## How to use
Applied automatically by the agent via:
```bash
kubectl apply -f /app/k8s/manifests/katib/experiment.yaml
```
Or manually from bastion:
```bash
kubectl apply -f k8s/manifests/katib/experiment.yaml
```
Monitor: `kubectl get experiment -n kubeflow` 

## Dependencies
- Katib controller deployed in kubeflow namespace
- `984445750473.dkr.ecr.us-east-1.amazonaws.com/anomaly-detector/training-pipeline:latest` image built and pushed
- `training-sa` service account in `training-pipeline` namespace (IRSA for S3)
- `ml/trainer.py` supports `--katib_mode` flag (writes ROC-AUC to stdout)
