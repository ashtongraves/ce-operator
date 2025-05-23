# OSG Hosted CE Kubernetes Operator

A Kubernetes operator that manages Open Science Grid (OSG) Hosted Compute Entrypoints (CEs) using custom resources. This operator automates the deployment and lifecycle management of HTCondor-CE instances that bridge OSG workloads to local compute clusters.

## Overview

The OSG Hosted CE Operator creates and manages HTCondor-CE deployments in Kubernetes that act as gateways between the OSG and local batch systems (Slurm, PBS, etc.). Each Compute Entrypoint is defined as a custom Kubernetes resource and automatically provisions all necessary components including services, network policies, and configurations.

## Features

- **Custom Resource Definition**: Declarative management of Compute Entrypoints through Kubernetes CRDs
- **Health Monitoring**: Continuous health checks with pool-aware safety mechanisms
- **Multi-Pool Support**: Track and manage resources across different OSG pools (ospool, cms, atlas, etc.)
- **Automated Configuration**: Generate HTCondor-CE, OSG-Configure, and SciTokens configurations
- **Network Security**: Automatic NetworkPolicy creation for secure communication
- **Load Balancing**: Service exposure with MetalLB integration
- **BOSCO Integration**: Support for SSH-based job submission to remote clusters

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│ ComputeEntrypoint│───▶│ CE Operator      │───▶│ Kubernetes      │
│ (Custom Resource)│    │ (Controller)     │    │ Resources       │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                              │                          │
                              ▼                          ▼
                       ┌──────────────┐         ┌────────────────┐
                       │ Health       │         │ • Deployment   │
                       │ Monitoring   │         │ • Service      │
                       │ & Pool       │         │ • ConfigMaps   │
                       │ Management   │         │ • NetworkPolicy│
                       └──────────────┘         └────────────────┘
```
## ComputeEntrypoint Resource Specification

### Basic Structure

```yaml
apiVersion: osg-htc.org/v1
kind: ComputeEntrypoint
metadata:
  name: my-ce
  namespace: osg-ce-dev
  annotation:
    topology/facility: MyFacility
    topology/resource: MyResource-CE
    topology/contact-email: admin@example.org
  labels:
    pool/ospool: true
spec:
  kubernetes:
    image: hub.opensciencegrid.org/osg-htc/hosted-ce:24-release
    hostname: my-ce.example.org
  cluster:
    host: login.example.org
    batch: slurm
  pilot:
  - name: standard
    queue: normal
    limit: 10
    walltime: 4320
    resources:
      cpu: 8
      ram: 16384
    vo:
    - osg
```

### Configuration Sections

#### Kubernetes Configuration (`spec.kubernetes`)
- `image`: Container image for the HTCondor-CE
- `hostname`: External hostname for the CE
- `node.labels`: Node selector for pod placement
- `service.annotations`: Service annotations (e.g., MetalLB pool)

#### Cluster Configuration (`spec.cluster`)
- `host`: Remote cluster login node
- `batch`: Batch system type (slurm, pbs, condor, etc.)
- `squid`: Squid proxy server (optional)
- `scratch`: Scratch directory path
- `ssh`: SSH configuration for BOSCO

#### Pilot Configuration (`spec.pilot`)
Array of pilot job configurations:
- `name`: Pilot configuration name
- `queue`: Target batch queue
- `limit`: Maximum concurrent jobs
- `walltime`: Maximum wall time (minutes)
- `resources`: Resource requirements (cpu, ram, gpu)
- `vo`: Supported Virtual Organizations

#### Pool Labels
Use labels to assign CEs to specific OSG pools:
- `pool/ospool: true` - OSG Connect pool
- `pool/cms: true` - CMS pool  
- `pool/atlas: true` - ATLAS pool
- `pool/dune: true` - DUNE pool
- `pool/icecube: true` - IceCube pool

## Health Monitoring and Safety Features

The operator implements sophisticated health monitoring to ensure pool stability:

### Pool Health Tracking
- Monitors health percentage of resources in each pool
- Prevents updates that would violate minimum health thresholds
- Default minimum healthy percentage: 80%

### Safety Mechanisms
- Resources are only updated if pool health remains above threshold
- Temporary delays applied when health constraints would be violated
- Continuous monitoring with 60-second health check intervals

### Health States
- **Healthy**: Deployment ready and functioning
- **Degraded**: Partial availability
- **Unhealthy**: No available replicas
- **Unknown**: Health check failed
- **Missing**: Resource not found

## Advanced Configuration

## Environment Variables

The operator supports these environment variables:

- `MIN_HEALTHY_PERCENTAGE`: Minimum pool health threshold (default: 0.8)
- `LOG_LEVEL`: Logging level (default: INFO)

### SSH Key Management

For clusters requiring SSH access, create a secret with your private key:

```bash
kubectl create secret generic my-ssh-key \
  --from-file=bosco.key=/path/to/private/key \
  -n osg-ce-dev
```

Reference it in your ComputeEntrypoint:

```yaml
spec:
  cluster:
    ssh:
      key: my-ssh-key
```

### Custom HTCondor Configuration

Add custom HTCondor-CE configuration:

```yaml
spec:
  config: |
    # Custom HTCondor-CE configuration
    SCHEDD_INTERVAL = 300
    MAX_JOBS_PER_OWNER = 100
```

### BOSCO Overrides

Configure BOSCO-specific settings:

```yaml
spec:
  bosco:
    dir: $HOME/bosco-osg-wn-client
    overrides:
    - name: gridmanager.log.debug
      path: /path/to/custom/config
```

## Monitoring and Troubleshooting

### Check Operator Status
```bash
kubectl logs -n osg-ce-dev deployment/osg-hosted-ce-controller
```

### Monitor ComputeEntrypoint Status
```bash
kubectl describe computeentrypoint my-ce -n osg-ce-dev
```

### Debug CE Container
```bash
kubectl exec -it deployment/osg-hosted-ce-my-ce -n osg-ce-dev -- bash
```
