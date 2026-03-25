#!/bin/bash
set -euo pipefail

# ---------------------------------------------------------------------------
# install-nginx-ingress.sh
# Installs NGINX Ingress Controller with an Azure internal Load Balancer.
# Uses 'helm template' + 'az aks command invoke' (private cluster).
#
# Prerequisites:
#   - AKS cluster must have Network Contributor role on the VNet:
#     az role assignment create --assignee-object-id <aks-identity-principal-id> \
#       --role "Network Contributor" --scope <vnet-id>
# ---------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${ENV_FILE:-$SCRIPT_DIR/../.env.azure}"

if [ ! -f "$ENV_FILE" ]; then
  echo "ERROR: $ENV_FILE not found. Run deploy-azure-infra.sh first."
  exit 1
fi
# shellcheck source=/dev/null
source "$ENV_FILE"

NGINX_VERSION="${NGINX_VERSION:-4.12.1}"
CONTROLLER_TAG="${CONTROLLER_TAG:-v1.13.7}"
CERTGEN_TAG="${CERTGEN_TAG:-v1.4.4}"
INGRESS_NAMESPACE="${INGRESS_NAMESPACE:-ingress-nginx}"
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
echo " NGINX Ingress Controller Installation"
echo "=============================================="
echo "Chart version:    $NGINX_VERSION"
echo "Controller tag:   $CONTROLLER_TAG"
echo "ACR:              $ACR_LOGIN_SERVER"
echo "Namespace:        $INGRESS_NAMESPACE"
echo "Load Balancer:    Internal (Azure)"
echo ""

# --- Step 1: Add Helm repo ---
echo "[1/4] Adding ingress-nginx Helm repo..."
helm repo add ingress-nginx https://kubernetes.github.io/ingress-nginx
helm repo update
echo "[1/4] Done."

# --- Step 2: Import kube-webhook-certgen into ACR ---
echo "[2/4] Importing kube-webhook-certgen into ACR..."
az acr import \
  --name "$ACR_NAME" \
  --source "registry.k8s.io/ingress-nginx/kube-webhook-certgen:${CERTGEN_TAG}" \
  --image "ingress-nginx/kube-webhook-certgen:${CERTGEN_TAG}" \
  --force
echo "[2/4] Done."

# --- Step 3: Render and apply ---
echo "[3/4] Rendering NGINX ingress manifests..."
helm template ingress-nginx ingress-nginx/ingress-nginx \
  --namespace "$INGRESS_NAMESPACE" \
  --version "$NGINX_VERSION" \
  --set controller.image.registry="${ACR_LOGIN_SERVER}" \
  --set controller.image.image="aks-managed-repository/oss/kubernetes/ingress/nginx-ingress-controller" \
  --set controller.image.tag="${CONTROLLER_TAG}" \
  --set controller.image.digest="" \
  --set controller.admissionWebhooks.patch.image.registry="${ACR_LOGIN_SERVER}" \
  --set controller.admissionWebhooks.patch.image.image="ingress-nginx/kube-webhook-certgen" \
  --set controller.admissionWebhooks.patch.image.tag="${CERTGEN_TAG}" \
  --set controller.admissionWebhooks.patch.image.digest="" \
  --set controller.service.annotations."service\.beta\.kubernetes\.io/azure-load-balancer-internal"=true \
  > "$TMP_DIR/ingress-nginx.yaml"

invoke "kubectl create namespace $INGRESS_NAMESPACE --dry-run=client -o yaml | kubectl apply -f -"
invoke "kubectl apply -f ingress-nginx.yaml" --file "$TMP_DIR/ingress-nginx.yaml"
echo "[3/4] Done."

# --- Step 4: Wait for rollout ---
echo "[4/4] Waiting for ingress controller to be ready (up to 3 min)..."
invoke "kubectl rollout status deployment/ingress-nginx-controller -n $INGRESS_NAMESPACE --timeout=180s"
echo "[4/4] Done."

echo ""
echo "=============================================="
echo " NGINX Ingress installation complete!"
echo "=============================================="
echo ""
echo "Get internal LB IP:"
echo "  az aks command invoke --resource-group $RG --name $AKS_NAME \\"
echo "    --command 'kubectl get svc ingress-nginx-controller -n $INGRESS_NAMESPACE'"
echo ""
echo "Next step: apply k8s/flink-ingress.yaml"
echo "  az aks command invoke --resource-group $RG --name $AKS_NAME \\"
echo "    --command 'kubectl apply -f flink-ingress.yaml' --file k8s/flink-ingress.yaml"
