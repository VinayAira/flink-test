#!/bin/bash
set -euo pipefail

# ---------------------------------------------------------------------------
# deploy-flink.sh
# Applies Flink session cluster and example job manifests.
# Uses 'az aks command invoke' (private cluster, no direct kubectl access).
# Injects ACR image name and Azure storage credentials via envsubst.
# ---------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MANIFEST_DIR="${MANIFEST_DIR:-$SCRIPT_DIR/../k8s}"
ENV_FILE="${ENV_FILE:-$SCRIPT_DIR/../.env.azure}"
FLINK_NAMESPACE="${FLINK_NAMESPACE:-flink}"
TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

if [ ! -f "$ENV_FILE" ]; then
  echo "ERROR: $ENV_FILE not found. Run deploy-azure-infra.sh first."
  exit 1
fi
# shellcheck source=/dev/null
source "$ENV_FILE"

invoke() {
  local cmd="$1"; shift
  local extra_args=("$@")
  az aks command invoke \
    --resource-group "$RG" \
    --name "$AKS_NAME" \
    --command "$cmd" \
    "${extra_args[@]}"
}

echo "=============================================="
echo " Flink Deployment"
echo "=============================================="
echo "ACR:             $ACR_LOGIN_SERVER"
echo "Storage account: $STORAGE_ACCOUNT"
echo "Container:       $CONTAINER_NAME"
echo ""

# --- Step 1: Namespace ---
echo "[1/5] Applying namespace..."
invoke "kubectl apply -f flink-namespace.yaml" \
  --file "$MANIFEST_DIR/flink-namespace.yaml"

# --- Step 2: RBAC ---
echo "[2/5] Applying RBAC..."
invoke "kubectl apply -f flink-rbac.yaml" \
  --file "$MANIFEST_DIR/flink-rbac.yaml"

# --- Step 3: Verify secret ---
echo "[3/5] Verifying azure-storage-secret..."
invoke "kubectl get secret azure-storage-secret -n $FLINK_NAMESPACE"

# --- Step 4: Session cluster (substitute ACR image) ---
echo "[4/5] Applying Flink session cluster..."
export ACR_LOGIN_SERVER STORAGE_ACCOUNT STORAGE_KEY CONTAINER_NAME
envsubst '${ACR_LOGIN_SERVER}' \
  < "$MANIFEST_DIR/flink-session-cluster.yaml" \
  > "$TMP_DIR/flink-session-cluster.yaml"

invoke "kubectl apply -f flink-session-cluster.yaml" \
  --file "$TMP_DIR/flink-session-cluster.yaml"

# --- Step 5: Example job (substitute ACR image + storage credentials) ---
echo "[5/5] Applying Flink example job..."
envsubst '${ACR_LOGIN_SERVER} ${STORAGE_ACCOUNT} ${STORAGE_KEY} ${CONTAINER_NAME}' \
  < "$MANIFEST_DIR/flink-example-job.yaml" \
  > "$TMP_DIR/flink-example-job.yaml"

invoke "kubectl apply -f flink-example-job.yaml" \
  --file "$TMP_DIR/flink-example-job.yaml"

echo ""
echo "=============================================="
echo " Flink deployment submitted!"
echo "=============================================="
echo ""
echo "Check deployment status:"
echo "  az aks command invoke --resource-group $RG --name $AKS_NAME \\"
echo "    --command 'kubectl get flinkdeployment -n $FLINK_NAMESPACE'"
echo ""
echo "Check pods:"
echo "  az aks command invoke --resource-group $RG --name $AKS_NAME \\"
echo "    --command 'kubectl get pods -n $FLINK_NAMESPACE'"
echo ""
echo "Watch pod startup:"
echo "  az aks command invoke --resource-group $RG --name $AKS_NAME \\"
echo "    --command 'kubectl get pods -n $FLINK_NAMESPACE -w'"
