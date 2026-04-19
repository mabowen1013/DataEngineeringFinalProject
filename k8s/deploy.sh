#!/bin/bash
# Author: Bowen Ma
# One-click deployment script for the trade analysis pipeline on K8s.
# Usage: ./deploy.sh [--local]
#   --local    Load trade-ingestion image into k3d (for local testing)

set -e

NAMESPACE="trade-pipeline"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "============================================"
echo "  Trade Pipeline - K8s Deployment"
echo "============================================"

# ----------------------------------------------------------
# Step 1: Create namespace
# ----------------------------------------------------------
echo ""
echo "[1/8] Creating namespace..."
kubectl apply -f "$SCRIPT_DIR/namespace.yaml"

# ----------------------------------------------------------
# Step 2: Apply secrets
# ----------------------------------------------------------
echo "[2/8] Applying secrets..."
kubectl apply -f "$SCRIPT_DIR/secrets.yaml"

# ----------------------------------------------------------
# Step 3: Create ConfigMaps from source files
# ----------------------------------------------------------
echo "[3/8] Creating ConfigMaps..."

# Pipeline config
kubectl apply -f "$SCRIPT_DIR/configmaps/pipeline-config.yaml"

# Postgres init SQL
kubectl create configmap postgres-init \
  --from-file=init.sql="$PROJECT_DIR/sql/init.sql" \
  -n "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

# Airflow DAGs (from the dags/ directory)
kubectl create configmap airflow-dags \
  --from-file="$PROJECT_DIR/dags/news_ingestion_dag.py" \
  --from-file="$PROJECT_DIR/dags/data_curation_dag.py" \
  --from-file="$PROJECT_DIR/dags/trade_health_check_dag.py" \
  -n "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

# ----------------------------------------------------------
# Step 4: Build and load trade-ingestion image (local k3d)
# ----------------------------------------------------------
if [ "$1" = "--local" ]; then
  echo "[4/8] Building trade-ingestion image for k3d..."
  docker build -t trade-ingestion:latest "$PROJECT_DIR/services/trade_ingestion"
  k3d image import trade-ingestion:latest -c trade-pipeline
else
  echo "[4/8] Skipping local image build (use --local for k3d testing)"
fi

# ----------------------------------------------------------
# Step 5: Deploy Postgres
# ----------------------------------------------------------
echo "[5/8] Deploying Postgres..."
kubectl apply -f "$SCRIPT_DIR/postgres/service.yaml"
kubectl apply -f "$SCRIPT_DIR/postgres/statefulset.yaml"

echo "  Waiting for Postgres to be ready..."
kubectl rollout status statefulset/postgres -n "$NAMESPACE" --timeout=120s

# ----------------------------------------------------------
# Step 6: Deploy Minio
# ----------------------------------------------------------
echo "[6/8] Deploying Minio..."
kubectl apply -f "$SCRIPT_DIR/minio/service.yaml"
kubectl apply -f "$SCRIPT_DIR/minio/statefulset.yaml"

echo "  Waiting for Minio to be ready..."
kubectl rollout status statefulset/minio -n "$NAMESPACE" --timeout=120s

# Create buckets
echo "  Creating Minio buckets..."
kubectl delete job minio-init -n "$NAMESPACE" --ignore-not-found
kubectl apply -f "$SCRIPT_DIR/minio/init-job.yaml"
kubectl wait --for=condition=complete job/minio-init -n "$NAMESPACE" --timeout=60s

# ----------------------------------------------------------
# Step 7: Deploy Airflow
# ----------------------------------------------------------
echo "[7/8] Deploying Airflow..."

# Initialize Airflow DB
echo "  Initializing Airflow database..."
kubectl delete job airflow-init -n "$NAMESPACE" --ignore-not-found
kubectl apply -f "$SCRIPT_DIR/airflow/init-job.yaml"
kubectl wait --for=condition=complete job/airflow-init -n "$NAMESPACE" --timeout=180s

# Deploy webserver + scheduler
kubectl apply -f "$SCRIPT_DIR/airflow/service.yaml"
kubectl apply -f "$SCRIPT_DIR/airflow/webserver-deployment.yaml"
kubectl apply -f "$SCRIPT_DIR/airflow/scheduler-deployment.yaml"

echo "  Waiting for Airflow webserver..."
kubectl rollout status deployment/airflow-webserver -n "$NAMESPACE" --timeout=180s
echo "  Waiting for Airflow scheduler..."
kubectl rollout status deployment/airflow-scheduler -n "$NAMESPACE" --timeout=180s

# ----------------------------------------------------------
# Step 8: Deploy Trade Ingestion
# ----------------------------------------------------------
echo "[8/8] Deploying Trade Ingestion service..."
kubectl apply -f "$SCRIPT_DIR/trade-ingestion/deployment.yaml"
kubectl rollout status deployment/trade-ingestion -n "$NAMESPACE" --timeout=60s

# ----------------------------------------------------------
# Summary
# ----------------------------------------------------------
echo ""
echo "============================================"
echo "  Deployment complete!"
echo "============================================"
echo ""
echo "Pods:"
kubectl get pods -n "$NAMESPACE"
echo ""
echo "Services:"
kubectl get svc -n "$NAMESPACE"
echo ""
echo "Access:"
echo "  Airflow UI:    kubectl port-forward svc/airflow-webserver 8080:8080 -n $NAMESPACE"
echo "  Minio Console: kubectl port-forward svc/minio 9001:9001 -n $NAMESPACE"
echo ""
echo "Verify data:"
echo "  kubectl exec -it statefulset/postgres -n $NAMESPACE -- psql -U pipeline -d trade_pipeline -c \"SELECT symbol, COUNT(*) FROM raw_trades GROUP BY symbol;\""
