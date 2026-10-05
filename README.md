# RabbitMQ Distributed File Processing & Auto-Scaling with Kubernetes + KEDA

This project demonstrates a production-grade **Competing Consumers Architecture** solving two major distributed systems challenges:
1. **Idle Containers / Work Imbalance**: Solved using RabbitMQ QoS Fair Dispatch (`prefetch = 1`). Fast workers immediately pull new work instead of sitting idle while other workers are busy.
2. **Dynamic Auto-Scaling**: Solved using Kubernetes + **KEDA (Kubernetes Event-driven Autoscaler)** based on real-time RabbitMQ queue backlog.
3. **Cross-Platform Shared Storage**: The Windows/Sender component generates relative storage keys pointing to a shared volume (`/shared-data`), ensuring cross-platform compatibility between host files and Linux Kubernetes pods.

---

## Architecture Overview

```
[Windows / Sender Pod] 
       │ 
       ├── (1) Writes file to Shared Volume (/shared-data)
       └── (2) Publishes relative file key to RabbitMQ
                     │
               [RabbitMQ Queue] 
                     │
       ┌─────────────┴─────────────┐
       │ (KEDA monitors queue depth│
       │  and scales Pods 1 -> 10) │
       ▼                           ▼
[K8s Worker Pod 1]         [K8s Worker Pod N]
 - Prefetch = 1             - Prefetch = 1
 - Pull on-demand           - Pull on-demand
 - Mounts Shared Storage    - Mounts Shared Storage
```

---

## Project Structure

```
├── sender/
│   ├── index.js              # Publishes file path every 2s
│   ├── package.json
│   └── Dockerfile
├── receiver/
│   ├── index.js              # Processes files (5s per file) with prefetch(1)
│   ├── package.json
│   └── Dockerfile
└── k8s/                      # Kubernetes manifests
    ├── 00-namespace.yaml     # Dedicated file-demo namespace
    ├── 01-rabbitmq.yaml      # RabbitMQ broker + Management Web UI
    ├── 02-storage.yaml       # Shared volume PV & PVC (hostPath for local cluster)
    ├── 03-receiver.yaml      # Receiver Deployment (with graceful shutdown)
    ├── 04-sender.yaml        # Sender Deployment
    └── 05-keda-autoscaler.yaml # KEDA ScaledObject & TriggerAuthentication
```

---

## How to Run This Project

### Prerequisites
Make sure your terminal is navigated to the project folder:
```powershell
cd d:\sender-reciever-demo
```
Ensure Docker and your Kubernetes cluster are running (`kubectl get nodes`).

---

### Step 1: Build the Container Images
Build the Sender and Receiver Docker images locally:
```powershell
docker build -t sender-service:latest ./sender
docker build -t receiver-service:latest ./receiver
```

---

### Step 2: Install KEDA in Your Cluster (Run Once)
If KEDA is not already installed in your cluster, install it using Helm:
```powershell
helm repo add kedacore https://kedacore.github.io/charts
helm repo update
helm install keda kedacore/keda --namespace keda --create-namespace
```

---

### Step 3: Deploy to Kubernetes
Apply all manifests in the `k8s/` folder. This starts RabbitMQ, creates the shared storage, launches the Sender and Receiver pods, and enables KEDA autoscaling:
```powershell
kubectl apply -f k8s/
```

Check that all pods are up and running:
```powershell
kubectl get pods -n file-demo
```

---

### Step 4: Watch the Demo in Action

Open separate terminal windows to observe the system working in real time:

#### Terminal 1: Watch the Receivers Balance the Workload (Fair Dispatch)
Watch how the receivers pick up files dynamically on demand (`prefetch = 1`) without idle time:
```powershell
kubectl logs -n file-demo -l app=receiver -f --prefix=true
```
* **Notice:** Fast workers process tasks and immediately send `ACK` to grab the next message. No container sits idle waiting on other containers.

#### Terminal 2: Watch the Sender Creating Files
See the sender write a file and publish its key every 2 seconds:
```powershell
kubectl logs -n file-demo -l app=sender -f
```

#### Terminal 3: Watch KEDA Dynamic Autoscaling
Watch Kubernetes scale the receiver pods up and down based on RabbitMQ queue depth:
```powershell
kubectl get hpa,scaledobject -n file-demo -w
```

---

### Step 5: (Optional) Open the RabbitMQ Dashboard
Port-forward the management interface to view message queues and consumer graphs in your browser:
```powershell
kubectl port-forward -n file-demo svc/rabbitmq-service 15672:15672
```
* **URL:** `http://localhost:15672`
* **Username:** `guest`
* **Password:** `guest`

---

### Step 6: Stop & Clean Up Everything
When you want to stop the demo and remove all resources from your cluster:
```powershell
# Delete the file-demo namespace and storage PV
kubectl delete namespace file-demo
kubectl delete pv shared-file-pv

# (Optional) Uninstall KEDA and delete its namespace
helm uninstall keda -n keda
kubectl delete namespace keda
```

---

## How It Works in Simple Words (Restaurant Analogy)

Imagine a busy **Restaurant Kitchen**:
* **The Waiter (Sender):** Takes food orders from customers every 2 seconds.
* **The Order Board (RabbitMQ):** A central board where orders are posted.
* **The Chefs (Receiver Pods in Kubernetes):** Each chef takes an order, cooks it, and finishes. Some dishes take 2 seconds (a salad), while others take 9 seconds (a steak).

### What Was Broken in the Original Design?

1. **Unfair Work Distribution (Chefs Sitting Idle):**
   * *Old behavior:* RabbitMQ handed out 5 orders to Chef A, 5 to Chef B, and 5 to Chef C all at once in round-robin batches.
   * *The problem:* Chef B got quick salads and finished in 10 seconds. Chef A got heavy steaks and was stuck for an hour. Chef B was **standing around idle doing nothing**, even though Chef A was overloaded!
2. **Fixed Number of Containers:**
   * During a sudden rush of 1,000 orders, you only had a fixed number of chefs.
   * When no customers were there at night, chefs were still idling, consuming resources.
3. **Cross-Platform File Path Trap:**
   * A Windows program sending `C:\files\data.txt` fails in Linux containers because Linux has no `C:\` drive.

### How This Project Fixes Everything

1. **Take One Order at a Time (`prefetch = 1`):**
   * RabbitMQ is instructed: *"Only give each chef 1 order at a time."*
   * As soon as Chef B finishes a quick 2-second dish, they send an `ACK` and immediately grab the next ticket from the board.
   * Fast workers automatically do more work; no worker sits idle waiting on others.
2. **Hire and Release Chefs Automatically (Kubernetes + KEDA):**
   * **KEDA** acts like an intelligent kitchen manager watching the order board every 5 seconds.
   * If orders start piling up in the queue, KEDA tells Kubernetes to spin up more worker pods (up to 10) to clear the rush.
   * When the queue is cleared, KEDA scales them back down to save resources.
3. **Shared Storage Shelf (`/shared-data`):**
   * The Sender places the actual file onto a shared storage volume (`/shared-data`).
   * The message only contains the relative key (e.g. `document_123.txt`).
   * Any Linux worker pod opens `/shared-data/document_123.txt` directly without OS path conflicts.
