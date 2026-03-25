#!/bin/bash
set -euo pipefail

# ---------------------------------------------------------------------------
# deploy-azure-infra.sh
# Provisions a fully private Flink-on-AKS setup (no public IPs).
#
# One ACR handles both AKS bootstrap and app images:
#   - Bootstrap cache rule: mcr.microsoft.com/* → aks-managed-repository/*
#   - App images: flink:1.20, cert-manager, operator (no prefix conflict)
#
# Steps:
#   1. Verify resource group
#   2. VNet + subnet (Microsoft.ContainerRegistry service endpoint)
#   3. ACR + aks-managed-mcr cache rule (target: aks-managed-repository/*)
#   4. Import app images into ACR
#   5. Private AKS cluster (--enable-private-cluster --outbound-type none)
#   6. ADLS Gen2 storage account + blob container
#   7. Save .env.azure
#   8. Bootstrap k8s namespace + secret
#
# Prerequisites: az CLI, kubectl, helm, envsubst installed; 'az login' completed.
# ---------------------------------------------------------------------------

RG="${RG:-VinayAiran_FlinkTests}"
AKS_NAME="${AKS_NAME:-flink-aks}"
NODE_COUNT="${NODE_COUNT:-3}"
NODE_VM="${NODE_VM:-Standard_D4s_v3}"
STORAGE_ACCOUNT="${STORAGE_ACCOUNT:-flinkstate$RANDOM}"
CONTAINER_NAME="${CONTAINER_NAME:-flink-state}"
VNET_NAME="${VNET_NAME:-flink-vnet}"
SUBNET_NAME="${SUBNET_NAME:-aks-subnet}"
ACR_NAME="${ACR_NAME:-flinkacr$RANDOM}"

CERT_MANAGER_VERSION="${CERT_MANAGER_VERSION:-v1.16.2}"
FLINK_OPERATOR_VERSION="${FLINK_OPERATOR_VERSION:-1.10.0}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/../.env.azure"

echo "=============================================="
echo " Flink on AKS - Private Infrastructure Setup"
echo "=============================================="
echo "Resource Group:   $RG"
echo "AKS Cluster:      $AKS_NAME ($NODE_COUNT x $NODE_VM)"
echo "VNet:             $VNET_NAME / $SUBNET_NAME"
echo "ACR:              $ACR_NAME"
echo "Storage Account:  $STORAGE_ACCOUNT"
echo ""

# --- Step 1: Verify resource group ---
echo "[1/8] Verifying resource group '$RG'..."
if ! az group show --name "$RG" --output none 2>/dev/null; then
  echo "ERROR: Resource group '$RG' not found."
  exit 1
fi
REGION=$(az group show --name "$RG" --query location --output tsv)
echo "       Found. Region: $REGION"
echo "[1/8] Done."

# --- Step 2: VNet + subnet with ACR service endpoint ---
echo "[2/8] Creating VNet '$VNET_NAME' and subnet '$SUBNET_NAME'..."
if az network vnet show --resource-group "$RG" --name "$VNET_NAME" --output none 2>/dev/null; then
  echo "       VNet already exists."
else
  az network vnet create \
    --resource-group "$RG" \
    --name "$VNET_NAME" \
    --address-prefix "10.0.0.0/8" \
    --subnet-name "$SUBNET_NAME" \
    --subnet-prefix "10.240.0.0/16" \
    --output table
fi

az network vnet subnet update \
  --resource-group "$RG" \
  --vnet-name "$VNET_NAME" \
  --name "$SUBNET_NAME" \
  --service-endpoints "Microsoft.ContainerRegistry" \
  --output table

SUBNET_ID=$(az network vnet subnet show \
  --resource-group "$RG" \
  --vnet-name "$VNET_NAME" \
  --name "$SUBNET_NAME" \
  --query id --output tsv)
echo "[2/8] Done."

# --- Step 3: ACR + aks-managed-mcr cache rule ---
echo "[3/8] Setting up ACR '$ACR_NAME'..."
if ! az acr show --name "$ACR_NAME" --resource-group "$RG" --output none 2>/dev/null; then
  az acr create \
    --resource-group "$RG" \
    --name "$ACR_NAME" \
    --sku Standard \
    --location "$REGION" \
    --output table
fi
ACR_ID=$(az acr show --name "$ACR_NAME" --resource-group "$RG" --query id --output tsv)
ACR_LOGIN_SERVER="${ACR_NAME}.azurecr.io"

# AKS bootstrap cache rule: target must be 'aks-managed-repository/*'
# (app images use different prefixes so no conflict)
if ! az acr cache show --registry "$ACR_NAME" --name "aks-managed-mcr" --output none 2>/dev/null; then
  echo "       Creating aks-managed-mcr cache rule..."
  az acr cache create \
    --registry "$ACR_NAME" \
    --name "aks-managed-mcr" \
    --source-repo "mcr.microsoft.com/*" \
    --target-repo "aks-managed-repository/*" \
    --output table
else
  echo "       Cache rule 'aks-managed-mcr' already exists."
fi
echo "[3/8] Done. ACR: $ACR_LOGIN_SERVER"

# --- Step 4: Import app images (server-side, no local internet needed) ---
echo "[4/8] Importing container images into ACR..."
echo "      Importing flink:1.20..."
az acr import \
  --name "$ACR_NAME" \
  --source "docker.io/library/flink:1.20" \
  --image "flink:1.20" \
  --force

for component in cert-manager-controller cert-manager-cainjector cert-manager-webhook cert-manager-startupapicheck; do
  echo "      Importing jetstack/$component:$CERT_MANAGER_VERSION..."
  az acr import \
    --name "$ACR_NAME" \
    --source "quay.io/jetstack/$component:$CERT_MANAGER_VERSION" \
    --image "jetstack/$component:$CERT_MANAGER_VERSION" \
    --force
done

echo "      Importing apache/flink-kubernetes-operator:$FLINK_OPERATOR_VERSION..."
az acr import \
  --name "$ACR_NAME" \
  --source "docker.io/apache/flink-kubernetes-operator:$FLINK_OPERATOR_VERSION" \
  --image "apache/flink-kubernetes-operator:$FLINK_OPERATOR_VERSION" \
  --force

echo "[4/8] Done. All images in $ACR_LOGIN_SERVER"

# --- Step 5: Private AKS cluster ---
echo "[5/8] Creating private AKS cluster '$AKS_NAME' (takes ~5 minutes)..."
az aks create \
  --resource-group "$RG" \
  --name "$AKS_NAME" \
  --node-count "$NODE_COUNT" \
  --node-vm-size "$NODE_VM" \
  --enable-managed-identity \
  --enable-oidc-issuer \
  --enable-workload-identity \
  --generate-ssh-keys \
  --network-plugin azure \
  --vnet-subnet-id "$SUBNET_ID" \
  --enable-private-cluster \
  --outbound-type none \
  --attach-acr "$ACR_NAME" \
  --bootstrap-artifact-source Cache \
  --bootstrap-container-registry-resource-id "$ACR_ID" \
  --output table
echo "[5/8] Done."

# --- Step 6: ADLS Gen2 storage account ---
echo "[6/8] Creating ADLS Gen2 storage account '$STORAGE_ACCOUNT'..."
az storage account create \
  --name "$STORAGE_ACCOUNT" \
  --resource-group "$RG" \
  --location "$REGION" \
  --sku Standard_LRS \
  --kind StorageV2 \
  --enable-hierarchical-namespace true \
  --output table

az storage container create \
  --name "$CONTAINER_NAME" \
  --account-name "$STORAGE_ACCOUNT" \
  --auth-mode login \
  --output table

STORAGE_KEY=$(az storage account keys list \
  --resource-group "$RG" \
  --account-name "$STORAGE_ACCOUNT" \
  --query "[0].value" \
  --output tsv)
echo "[6/8] Done."

# --- Step 7: Save env file ---
echo "[7/8] Saving .env.azure..."
cat > "$ENV_FILE" <<EOF
STORAGE_ACCOUNT=$STORAGE_ACCOUNT
STORAGE_KEY=$STORAGE_KEY
CONTAINER_NAME=$CONTAINER_NAME
RG=$RG
AKS_NAME=$AKS_NAME
REGION=$REGION
ACR_NAME=$ACR_NAME
ACR_LOGIN_SERVER=$ACR_LOGIN_SERVER
VNET_NAME=$VNET_NAME
SUBNET_NAME=$SUBNET_NAME
CERT_MANAGER_VERSION=$CERT_MANAGER_VERSION
FLINK_OPERATOR_VERSION=$FLINK_OPERATOR_VERSION
EOF
chmod 600 "$ENV_FILE"
echo "[7/8] Done."

# --- Step 8: Bootstrap k8s namespace + secret ---
echo "[8/8] Creating 'flink' namespace and storage secret..."
az aks command invoke \
  --resource-group "$RG" \
  --name "$AKS_NAME" \
  --command "kubectl create namespace flink --dry-run=client -o yaml | kubectl apply -f -"

az aks command invoke \
  --resource-group "$RG" \
  --name "$AKS_NAME" \
  --command "kubectl create secret generic azure-storage-secret \
    --namespace flink \
    --from-literal=storage-account-name='$STORAGE_ACCOUNT' \
    --from-literal=storage-account-key='$STORAGE_KEY' \
    --dry-run=client -o yaml | kubectl apply -f -"
echo "[8/8] Done."

echo ""
echo "=============================================="
echo " Infrastructure provisioning complete!"
echo "=============================================="
echo "AKS cluster:      $AKS_NAME (private)"
echo "ACR:              $ACR_LOGIN_SERVER"
echo "Storage account:  $STORAGE_ACCOUNT"
echo "abfss:// path:    abfss://$CONTAINER_NAME@${STORAGE_ACCOUNT}.dfs.core.windows.net/"
echo ""
echo "kubectl (private cluster):"
echo "  az aks command invoke --resource-group $RG --name $AKS_NAME --command 'kubectl get nodes'"
echo ""
echo "Next step: run install-flink-operator.sh"
