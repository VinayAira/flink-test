# Apache Flink on Azure — Evaluation Plan

**Author:** Vinay Airan
**Last Updated:** March 2026
**Status:** In Progress
**Repo:** [VinayAira/flink-test](https://github.com/VinayAira/flink-test) — branch `azure-deployment`

---

## Goal

Evaluate Apache Flink as a stream processing platform on Azure Kubernetes Service (AKS) to determine its suitability for production workloads. The evaluation covers:

1. **Infrastructure setup** — Private AKS cluster with Flink Kubernetes Operator
2. **Functional validation** — Core Flink capabilities (stateful streaming, checkpointing, fault tolerance)
3. **Stress testing** — Performance, scalability, and resilience under load
4. **Operability** — Monitoring, UI access, job lifecycle management

The outcome will inform a decision on whether Flink meets the team's requirements for real-time data processing at scale on Azure.

---

## Architecture Overview

```
Azure Subscription (westus2)
└── Resource Group: VinayAiran_FlinkTests
    ├── VNet: flink-vnet (10.0.0.0/8)
    │   └── Subnet: aks-subnet (10.240.0.0/16)
    ├── ACR: flinkacr20106 (image registry, no internet pull)
    ├── AKS: flink-aks (private cluster, 3x Standard_D4s_v3)
    │   └── Kubernetes
    │       ├── cert-manager          ← TLS for operator webhook
    │       ├── flink-operator        ← Flink Kubernetes Operator v1.10.0
    │       ├── ingress-nginx         ← Internal LB ingress controller
    │       └── flink-test (managed)  ← Flink workloads
    │           ├── FlinkDeployment: basic-session-cluster
    │           ├── Service: basic-session-cluster-rest (Internal LB: 10.240.0.92)
    │           └── Ingress: flink-ui (flink.test → 10.240.0.93)
    ├── Storage Account: flinkstate20106 (ADLS Gen2)
    │   └── Container: flink-state
    │       ├── checkpoints/
    │       └── savepoints/
    └── DNS Zone: flink.test (A record → 10.240.0.93)
```

**Key constraints on this subscription:**
- `SDOStdPolicyNetwork` (ISRM policy) blocks all public IPs and user-defined routes
- Flink UI accessible only from within the VNet (via Azure Cloud Shell, Bastion, or VPN)

---

## Milestones

| # | Milestone | Status |
|---|---|---|
| M1 | Azure infrastructure provisioned | Done |
| M2 | Flink Operator installed and running | Done |
| M3 | Flink Session Cluster deployed | Done |
| M4 | Networking — Ingress + DNS configured | Done |
| M5 | Functional testing — core Flink features | In Progress |
| M6 | Stress testing — performance & resilience | Not Started |
| M7 | Evaluation report | Not Started |

---

## Milestone 1 — Azure Infrastructure

**Goal:** Private AKS cluster with ACR, VNet, and ADLS Gen2 storage.

**Status:** Done

### Tasks

| # | Task | Command / Script | Done |
|---|---|---|---|
| 1.1 | Create VNet + subnet | `infra/deploy-azure-infra.sh` | ✅ |
| 1.2 | Create ACR + MCR cache rule | `infra/deploy-azure-infra.sh` | ✅ |
| 1.3 | Import Flink + cert-manager + operator images into ACR | `infra/deploy-azure-infra.sh` | ✅ |
| 1.4 | Create private AKS cluster (outboundType: none) | `infra/deploy-azure-infra.sh` | ✅ |
| 1.5 | Create ADLS Gen2 storage account + container | `infra/deploy-azure-infra.sh` | ✅ |
| 1.6 | Grant AKS identity Network Contributor on VNet | Manual (`az role assignment create`) | ✅ |
| 1.7 | Save `.env.azure` with all infrastructure values | `infra/deploy-azure-infra.sh` | ✅ |

### Verification
```bash
az aks show --name flink-aks --resource-group VinayAiran_FlinkTests \
  --query "{state:provisioningState, nodes:agentPoolProfiles[0].count}" --output table
```

---

## Milestone 2 — Flink Operator Installation

**Goal:** cert-manager and Flink Kubernetes Operator running in the cluster.

**Status:** Done

### Tasks

| # | Task | Script | Done |
|---|---|---|---|
| 2.1 | Add Helm repos (jetstack, flink-operator archive) | `infra/install-flink-operator.sh` | ✅ |
| 2.2 | Install cert-manager v1.16.2 from ACR images | `infra/install-flink-operator.sh` | ✅ |
| 2.3 | Install Flink Kubernetes Operator v1.10.0 with CRDs | `infra/install-flink-operator.sh` | ✅ |
| 2.4 | Verify operator pod is 2/2 Running | Manual check | ✅ |

### Verification
```bash
az aks command invoke --name flink-aks --resource-group VinayAiran_FlinkTests \
  --command "kubectl get pods -n flink-operator && kubectl get crd | grep flink.apache.org"
```

---

## Milestone 3 — Flink Session Cluster

**Goal:** Flink session cluster running in the `flink-test` managed namespace with Azure storage integration.

**Status:** Done

### Tasks

| # | Task | Script | Done |
|---|---|---|---|
| 3.1 | Create `flink-test` managed namespace (ARM) | Azure Portal / ARM | ✅ |
| 3.2 | Create RBAC (ServiceAccount, Role, RoleBinding) | `k8s/flink-rbac.yaml` | ✅ |
| 3.3 | Create azure-storage-secret | `infra/deploy-flink.sh` | ✅ |
| 3.4 | Deploy FlinkDeployment (1 JM + 2 TM replicas) | `k8s/flink-session-cluster.yaml` | ✅ |
| 3.5 | Verify JobManager pod Running + REST API responding | Manual check | ✅ |

### Verification
```bash
az aks command invoke --name flink-aks --resource-group VinayAiran_FlinkTests \
  --command "kubectl exec -n flink-test \$(kubectl get pod -n flink-test -l component=jobmanager \
  -o jsonpath={.items[0].metadata.name}) -- curl -s http://localhost:8081/overview"
```

Expected response:
```json
{"flink-version":"1.20.3","taskmanagers":0,"slots-total":0,"jobs-running":0}
```

---

## Milestone 4 — Networking & UI Access

**Goal:** Flink UI accessible via `http://flink.test` from within the VNet.

**Status:** Done

### Tasks

| # | Task | Script | Done |
|---|---|---|---|
| 4.1 | Import NGINX kube-webhook-certgen image into ACR | `infra/install-nginx-ingress.sh` | ✅ |
| 4.2 | Deploy NGINX Ingress Controller with internal Azure LB | `infra/install-nginx-ingress.sh` | ✅ |
| 4.3 | Patch Flink REST service to internal LoadBalancer | Manual | ✅ |
| 4.4 | Create Ingress resource for `flink.test` | `k8s/flink-ingress.yaml` | ✅ |
| 4.5 | Add A record in Azure DNS zone `flink.test` | Manual | ✅ |
| 4.6 | Verify UI reachable from Cloud Shell / VPN | Manual | ✅ |

### Access Methods
| Method | Steps |
|---|---|
| **Azure Cloud Shell** | Open portal.azure.com → Cloud Shell → `curl http://10.240.0.93 -H "Host: flink.test"` |
| **Azure Portal** | AKS → Kubernetes resources → Services → `flink-test` → `basic-session-cluster-rest` |
| **SAW + VPN** | Connect to MSFT-AzVPN → open `http://flink.test` (requires VNet peering to corpnet hub) |

### Known Blocker
`SDOStdPolicyNetwork` blocks all public IPs. Request exemption via ISRM portal (`aka.ms/isrm`) to enable external access.

---

## Milestone 5 — Functional Testing

**Goal:** Validate core Flink capabilities work correctly end-to-end on Azure.

**Status:** In Progress

### Tasks

| # | Task | Description | Done |
|---|---|---|---|
| 5.1 | Submit a stateless batch job | Run built-in WordCount example | ⬜ |
| 5.2 | Submit a stateful streaming job | Run StateMachineExample from `k8s/flink-example-job.yaml` | ⬜ |
| 5.3 | Verify TaskManagers scale up on job submission | Check TM pods appear in `flink-test` namespace | ⬜ |
| 5.4 | Verify checkpoints written to ADLS Gen2 | Check `flink-state/checkpoints/` in storage account | ⬜ |
| 5.5 | Trigger a manual savepoint | Patch FlinkDeployment with `savepointTriggerNonce` | ⬜ |
| 5.6 | Restore job from savepoint | Redeploy with `initialSavepointPath` set | ⬜ |
| 5.7 | Test job failure recovery | Kill a TaskManager pod, verify job restarts from last checkpoint | ⬜ |
| 5.8 | Test operator-managed upgrade | Change job parallelism, verify rolling upgrade via savepoint | ⬜ |
| 5.9 | Verify Flink UI — job graph, metrics, logs | Access via `http://flink.test` from Cloud Shell | ⬜ |
| 5.10 | Test cancel and resubmit job | Verify clean lifecycle management | ⬜ |

### Test Commands

**5.1 — Submit WordCount job:**
```bash
az aks command invoke --name flink-aks --resource-group VinayAiran_FlinkTests \
  --command "kubectl exec -n flink-test \$(kubectl get pod -n flink-test \
  -l component=jobmanager -o jsonpath={.items[0].metadata.name}) -- \
  flink run /opt/flink/examples/batch/WordCount.jar"
```

**5.4 — Verify checkpoints in ADLS:**
```bash
az storage blob list \
  --container-name flink-state \
  --account-name flinkstate20106 \
  --prefix "checkpoints/" \
  --auth-mode login \
  --output table
```

**5.5 — Trigger savepoint:**
```bash
az aks command invoke --name flink-aks --resource-group VinayAiran_FlinkTests \
  --command "kubectl patch flinkdeployment basic-session-cluster -n flink-test \
  --type merge -p '{\"spec\":{\"job\":{\"savepointTriggerNonce\":1}}}'"
```

**5.7 — Simulate TaskManager failure:**
```bash
az aks command invoke --name flink-aks --resource-group VinayAiran_FlinkTests \
  --command "kubectl delete pod -n flink-test -l component=taskmanager"
```

---

## Milestone 6 — Stress Testing

**Goal:** Measure Flink's performance, scalability, and resilience under sustained high-throughput workloads on Azure.

**Status:** Not Started

### 6.1 Throughput Benchmark

| # | Task | Description | Done |
|---|---|---|---|
| 6.1.1 | Deploy Kafka on AKS (or use Azure Event Hubs) | High-throughput source for streaming tests | ⬜ |
| 6.1.2 | Build a high-throughput Flink job | Kafka → stateful aggregations → ADLS sink | ⬜ |
| 6.1.3 | Baseline throughput at current config (3 nodes, 2 TMs) | Measure events/sec, latency p50/p99 | ⬜ |
| 6.1.4 | Scale TMs to 5, 10 — measure throughput scaling | Verify near-linear scaling | ⬜ |
| 6.1.5 | Measure checkpoint duration at scale | Target: checkpoints complete in < 30s under load | ⬜ |

### 6.2 Scalability Testing

| # | Task | Description | Done |
|---|---|---|---|
| 6.2.1 | Scale AKS node pool from 3 → 6 nodes | `az aks scale --node-count 6` | ⬜ |
| 6.2.2 | Enable AKS cluster autoscaler | Auto scale nodes 3–10 based on load | ⬜ |
| 6.2.3 | Test TaskManager autoscaling with Reactive Mode | Enable `scheduler-mode: reactive` in Flink config | ⬜ |
| 6.2.4 | Measure job restart time after node failure | Cordon a node, verify job recovers within SLA | ⬜ |

### 6.3 Resilience Testing

| # | Task | Description | Done |
|---|---|---|---|
| 6.3.1 | Kill JobManager pod — verify HA failover | Requires HA config with ZooKeeper or Kubernetes HA | ⬜ |
| 6.3.2 | Kill all TaskManagers — verify recovery from checkpoint | Measure RTO (Recovery Time Objective) | ⬜ |
| 6.3.3 | Drain an AKS node — verify pod rescheduling | `kubectl drain <node>` during active job | ⬜ |
| 6.3.4 | Simulate network partition — verify exactly-once guarantees | Isolate a TM pod via NetworkPolicy | ⬜ |
| 6.3.5 | ADLS storage latency injection — verify checkpoint timeouts | Throttle storage account, observe behavior | ⬜ |

### 6.4 Metrics & Observability

| # | Task | Description | Done |
|---|---|---|---|
| 6.4.1 | Deploy Azure Managed Prometheus + Grafana | AKS monitoring add-on | ⬜ |
| 6.4.2 | Configure Flink metrics reporter → Prometheus | Set `metrics.reporter.prom.class` in Flink config | ⬜ |
| 6.4.3 | Build Grafana dashboard — throughput, latency, checkpoint duration | Key Flink operational metrics | ⬜ |
| 6.4.4 | Set up alerts — checkpoint failure, job failure, high latency | Azure Monitor alert rules | ⬜ |

### Target Benchmarks

| Metric | Target | Notes |
|---|---|---|
| Throughput | ≥ 1M events/sec | Across 2 TaskManagers, 4 slots each |
| End-to-end latency (p99) | ≤ 500ms | Source → sink |
| Checkpoint duration | ≤ 30s | Under full load |
| Job recovery time | ≤ 60s | From TaskManager failure |
| JobManager failover | ≤ 120s | Requires HA configuration |
| Throughput scaling efficiency | ≥ 80% | When doubling TM count |

---

## Milestone 7 — Evaluation Report

**Goal:** Summarize findings and make a go/no-go recommendation.

**Status:** Not Started

### Tasks

| # | Task | Done |
|---|---|---|
| 7.1 | Document functional test results | ⬜ |
| 7.2 | Document stress test benchmark results vs targets | ⬜ |
| 7.3 | Document operational findings (complexity, debuggability, upgrade story) | ⬜ |
| 7.4 | Cost analysis (AKS node pool, storage, networking) | ⬜ |
| 7.5 | Comparison vs alternatives (Spark Structured Streaming, Azure Stream Analytics) | ⬜ |
| 7.6 | Go / No-Go recommendation with justification | ⬜ |

---

## Cluster Management

### Start / Stop (cost saving)
```bash
# Stop (pause VM billing — all configs preserved)
az aks stop --name flink-aks --resource-group VinayAiran_FlinkTests

# Start
az aks start --name flink-aks --resource-group VinayAiran_FlinkTests
```

### Check cluster health
```bash
az aks command invoke --name flink-aks --resource-group VinayAiran_FlinkTests \
  --command "kubectl get nodes && kubectl get pods -A --field-selector=status.phase=Running \
  | grep -v kube-system"
```

### Deploy / redeploy scripts
```bash
cd azure-deployment/infra
./deploy-azure-infra.sh        # Provision infrastructure (first time only)
./install-flink-operator.sh    # Install cert-manager + Flink operator
./install-nginx-ingress.sh     # Install NGINX ingress with internal LB
./deploy-flink.sh              # Deploy Flink session cluster + example job
./teardown-azure.sh            # Destroy everything
```

---

## Known Issues & Workarounds

| Issue | Cause | Workaround |
|---|---|---|
| Flink UI not publicly accessible | `SDOStdPolicyNetwork` blocks public IPs | Access via Azure Cloud Shell or SAW + VPN |
| `kubectl` from laptop doesn't work | Private cluster, no authorized IP ranges supported | Use `az aks command invoke` |
| NGINX app-routing LB pending | Same policy blocks public IP for AKS app routing add-on | Use our custom `ingress-nginx` with internal LB annotation |
| Helm repo 404 for Flink operator | Released versions move from `downloads.apache.org` to `archive.apache.org` | Use `archive.apache.org` URL |

---

## References

- [Flink Kubernetes Operator Docs](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/)
- [Flink 1.20 Release Notes](https://nightlies.apache.org/flink/flink-docs-release-1.20/)
- [AKS Private Cluster Docs](https://learn.microsoft.com/en-us/azure/aks/private-cluster)
- [ADLS Gen2 with Flink](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/filesystems/azure/)
- [Flink Kubernetes Operator — Session Clusters](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/custom-resource/session-job/)
- [ISRM Policy Exemption Request](https://aka.ms/isrm)
