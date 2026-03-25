# Apache Flink on Azure AKS

Deploys the Flink session cluster and example job from this repo onto Azure Kubernetes Service using the [Flink Kubernetes Operator](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/).

## Prerequisites

- [az CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli) — `az login` completed
- [kubectl](https://kubernetes.io/docs/tasks/tools/)
- [helm](https://helm.sh/docs/intro/install/)
- [envsubst](https://www.gnu.org/software/gettext/) — usually pre-installed on macOS/Linux

## Quick Start

```bash
cd azure-deployment/infra

# 1. Provision AKS, storage account, namespace, and secret
./deploy-azure-infra.sh

# 2. Install cert-manager and Flink Kubernetes Operator
./install-flink-operator.sh

# 3. Deploy Flink session cluster and example job
./deploy-flink.sh
```

That's it. The Flink UI will be accessible via:
```bash
kubectl port-forward -n flink svc/basic-session-cluster-rest 8081:8081
open http://localhost:8081
```

## Configuration

All defaults can be overridden via environment variables before running the scripts:

| Variable | Default | Description |
|---|---|---|
| `REGION` | `eastus` | Azure region |
| `RG` | `VinayAiran_FlinkTests` | Resource group name (pre-existing) |
| `AKS_NAME` | `flink-aks` | AKS cluster name |
| `NODE_COUNT` | `3` | Number of AKS nodes |
| `NODE_VM` | `Standard_D4s_v3` | VM size per node |
| `STORAGE_ACCOUNT` | `flinkstate<random>` | Storage account name (must be globally unique) |
| `CONTAINER_NAME` | `flink-state` | Blob container for checkpoints/savepoints |

Example with overrides:
```bash
REGION=westus2 RG=my-flink-rg NODE_COUNT=5 ./deploy-azure-infra.sh
```

## Architecture

```
Azure
└── Resource Group (flink-rg)
    ├── AKS Cluster (3x Standard_D4s_v3)
    │   └── Kubernetes
    │       ├── namespace: cert-manager   ← cert-manager
    │       ├── namespace: flink-operator ← Flink K8s Operator
    │       └── namespace: flink
    │           ├── ServiceAccount: flink
    │           ├── Secret: azure-storage-secret
    │           ├── FlinkDeployment: basic-session-cluster
    │           └── FlinkDeployment: flink-example-job (StateMachineExample)
    └── Storage Account (ADLS Gen2)
        └── Container: flink-state
            ├── checkpoints/  ← periodic checkpoints every 60s
            └── savepoints/   ← triggered on job upgrades
```

## What Changed vs the Minikube Setup

| Setting | Minikube | Azure AKS |
|---|---|---|
| Namespace | `default` | `flink` |
| Checkpoint dir | `file:///tmp/flink-checkpoints` | `abfss://flink-state@<account>.dfs.core.windows.net/checkpoints` |
| Savepoint dir | `file:///tmp/flink-savepoints` | `abfss://flink-state@<account>.dfs.core.windows.net/savepoints` |
| State backend | `filesystem` | `rocksdb` |
| Upgrade mode | `stateless` | `savepoint` |
| TM slots | 2 | 4 |
| JM/TM memory | 1024m | 2048m |
| TM CPU | 1 | 2 |
| Parallelism | 2 | 4 |

## Verification

```bash
# Check nodes
kubectl get nodes

# Check operator is running
kubectl get pods -n flink-operator

# Check Flink deployments
kubectl get flinkdeployment -n flink

# Check all pods
kubectl get pods -n flink

# Verify checkpoints are landing in Azure
az storage blob list \
  --container-name flink-state \
  --account-name <your-storage-account> \
  --prefix "checkpoints/" \
  --auth-mode login \
  --output table

# Trigger a manual savepoint
kubectl patch flinkdeployment flink-example-job -n flink \
  --type merge \
  -p '{"spec":{"job":{"savepointTriggerNonce":1}}}'
```

## Teardown

```bash
cd azure-deployment/infra
./teardown-azure.sh
```

This deletes the entire resource group (AKS + storage). All data will be lost.
