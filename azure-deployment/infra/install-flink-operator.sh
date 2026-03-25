#!/bin/bash
set -euo pipefail

# ---------------------------------------------------------------------------
# install-flink-operator.sh
# Installs cert-manager and the Flink Kubernetes Operator using images from ACR.
# Uses 'helm template' + 'az aks command invoke' (private cluster, no direct kubectl).
# ---------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${ENV_FILE:-$SCRIPT_DIR/../.env.azure}"

if [ ! -f "$ENV_FILE" ]; then
  echo "ERROR: $ENV_FILE not found. Run deploy-azure-infra.sh first."
  exit 1
fi
# shellcheck source=/dev/null
source "$ENV_FILE"

FLINK_NAMESPACE="${FLINK_NAMESPACE:-flink}"
OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE:-flink-operator}"
TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

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
echo " Flink Kubernetes Operator Installation"
echo "=============================================="
echo "cert-manager:       $CERT_MANAGER_VERSION"
echo "Flink Operator:     $FLINK_OPERATOR_VERSION"
echo "ACR:                $ACR_LOGIN_SERVER"
echo "Operator namespace: $OPERATOR_NAMESPACE"
echo "Watches namespace:  $FLINK_NAMESPACE"
echo ""

# --- Step 1: Add Helm repos ---
echo "[1/5] Adding Helm repositories..."
helm repo add jetstack https://charts.jetstack.io
helm repo add flink-operator-repo \
  "https://downloads.apache.org/flink/flink-kubernetes-operator-${FLINK_OPERATOR_VERSION}/"
helm repo update
echo "[1/5] Done."

# --- Step 2: Render cert-manager manifests and apply ---
echo "[2/5] Rendering cert-manager $CERT_MANAGER_VERSION manifests..."
helm template cert-manager jetstack/cert-manager \
  --namespace cert-manager \
  --version "$CERT_MANAGER_VERSION" \
  --set installCRDs=true \
  --set image.registry="${ACR_LOGIN_SERVER}" \
  --set cainjector.image.registry="${ACR_LOGIN_SERVER}" \
  --set webhook.image.registry="${ACR_LOGIN_SERVER}" \
  --set startupapicheck.image.registry="${ACR_LOGIN_SERVER}" \
  > "$TMP_DIR/cert-manager.yaml"

echo "     Applying cert-manager manifests..."
invoke "kubectl create namespace cert-manager --dry-run=client -o yaml | kubectl apply -f -"
invoke "kubectl apply -f cert-manager.yaml" --file "$TMP_DIR/cert-manager.yaml"
echo "[2/5] Done."

# --- Step 3: Wait for cert-manager to be ready ---
echo "[3/5] Waiting for cert-manager deployments to be ready (up to 3 min)..."
invoke "kubectl rollout status deployment/cert-manager -n cert-manager --timeout=180s"
invoke "kubectl rollout status deployment/cert-manager-cainjector -n cert-manager --timeout=180s"
invoke "kubectl rollout status deployment/cert-manager-webhook -n cert-manager --timeout=180s"
echo "[3/5] Done."

# --- Step 4: Render Flink operator manifests and apply ---
echo "[4/5] Rendering Flink Kubernetes Operator $FLINK_OPERATOR_VERSION manifests..."
helm template flink-kubernetes-operator \
  flink-operator-repo/flink-kubernetes-operator \
  --namespace "$OPERATOR_NAMESPACE" \
  --version "$FLINK_OPERATOR_VERSION" \
  --set webhook.create=true \
  --set "watchNamespaces={$FLINK_NAMESPACE}" \
  --set image.repository="${ACR_LOGIN_SERVER}/apache/flink-kubernetes-operator" \
  > "$TMP_DIR/flink-operator.yaml"

echo "     Applying Flink operator manifests..."
invoke "kubectl create namespace $OPERATOR_NAMESPACE --dry-run=client -o yaml | kubectl apply -f -"
invoke "kubectl apply -f flink-operator.yaml" --file "$TMP_DIR/flink-operator.yaml"
echo "[4/5] Done."

# --- Step 5: Wait for operator to be ready ---
echo "[5/5] Waiting for Flink operator to be ready (up to 3 min)..."
invoke "kubectl rollout status deployment/flink-kubernetes-operator -n $OPERATOR_NAMESPACE --timeout=180s"
echo "[5/5] Done."

echo ""
echo "=============================================="
echo " Flink Operator installation complete!"
echo "=============================================="
echo ""
echo "Check operator pods:"
echo "  az aks command invoke --resource-group $RG --name $AKS_NAME --command 'kubectl get pods -n $OPERATOR_NAMESPACE'"
echo ""
echo "Check Flink CRDs:"
echo "  az aks command invoke --resource-group $RG --name $AKS_NAME --command \"kubectl get crd | grep flink.apache.org\""
echo ""
echo "Next step: run deploy-flink.sh"
