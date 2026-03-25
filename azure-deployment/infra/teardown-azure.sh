#!/bin/bash
set -euo pipefail

# ---------------------------------------------------------------------------
# teardown-azure.sh
# Deletes the AKS cluster, VNet, ACR, and storage account created by
# deploy-azure-infra.sh. The resource group itself is NOT deleted (pre-existing).
# ---------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${ENV_FILE:-$SCRIPT_DIR/../.env.azure}"

if [ -f "$ENV_FILE" ]; then
  # shellcheck source=/dev/null
  source "$ENV_FILE"
fi

RG="${RG:-VinayAiran_FlinkTests}"
AKS_NAME="${AKS_NAME:-flink-aks}"

echo "=============================================="
echo " WARNING: Flink Teardown"
echo "=============================================="
echo "This will permanently delete:"
echo "  - AKS cluster:     $AKS_NAME"
echo "  - ACR:             ${ACR_NAME:-<from .env.azure>}"
echo "  - Storage account: ${STORAGE_ACCOUNT:-<from .env.azure>}"
echo "  - VNet:            ${VNET_NAME:-flink-vnet}"
echo ""
echo "NOTE: Resource group '$RG' will NOT be deleted."
echo ""
read -r -p "Type the AKS cluster name to confirm: " CONFIRM

if [ "$CONFIRM" != "$AKS_NAME" ]; then
  echo "Confirmation did not match '$AKS_NAME'. Aborting."
  exit 1
fi

# --- Step 1: Remove Flink deployments gracefully ---
echo "[1/4] Removing Flink deployments..."
az aks command invoke \
  --resource-group "$RG" \
  --name "$AKS_NAME" \
  --command "kubectl delete flinkdeployment --all -n flink --timeout=60s || true; kubectl delete namespace flink --timeout=30s || true" \
  2>/dev/null || true
echo "[1/4] Done."

# --- Step 2: Delete AKS cluster ---
echo "[2/4] Deleting AKS cluster '$AKS_NAME' (async)..."
az aks delete \
  --resource-group "$RG" \
  --name "$AKS_NAME" \
  --yes \
  --no-wait
echo "[2/4] Deletion initiated."

# --- Step 3: Delete ACR and storage account ---
if [ -n "${ACR_NAME:-}" ]; then
  echo "[3/4] Deleting ACR '$ACR_NAME'..."
  az acr delete --resource-group "$RG" --name "$ACR_NAME" --yes 2>/dev/null || true
fi

if [ -n "${STORAGE_ACCOUNT:-}" ]; then
  echo "      Deleting storage account '$STORAGE_ACCOUNT'..."
  az storage account delete --resource-group "$RG" --name "$STORAGE_ACCOUNT" --yes 2>/dev/null || true
fi
echo "[3/4] Done."

# --- Step 4: Delete VNet ---
if [ -n "${VNET_NAME:-}" ]; then
  echo "[4/4] Deleting VNet '$VNET_NAME' (after AKS deletion completes)..."
  echo "      Waiting for AKS deletion before removing VNet..."
  az aks wait --resource-group "$RG" --name "$AKS_NAME" --deleted --timeout=300 2>/dev/null || true
  az network vnet delete --resource-group "$RG" --name "$VNET_NAME" --yes 2>/dev/null || true
fi
echo "[4/4] Done."

# --- Cleanup local state ---
rm -f "$ENV_FILE"
echo ""
echo "Teardown complete. Removed .env.azure."
