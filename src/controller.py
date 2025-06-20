import kopf
import logging
import asyncio
from typing import Dict
from datetime import datetime, timezone

from kubernetes import client, config
from kubernetes.client.rest import ApiException

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global state for health tracking
pool_health_tracker = {}
min_healthy_percentage = 0.8

# Initialize Kubernetes clients
try:
    config.load_incluster_config()
except config.ConfigException:
    config.load_kube_config()

apps_v1 = client.AppsV1Api()
core_v1 = client.CoreV1Api()
networking_v1 = client.NetworkingV1Api()

@kopf.on.startup()
def configure(settings: kopf.OperatorSettings, **_):
    """Configure the operator"""
    settings.peering.priority = 100
    settings.peering.name = "osg-hosted-ce-controller"
    settings.watching.server_timeout = 600
    logger.info(f"OSG Hosted CE Controller starting (min healthy: {min_healthy_percentage*100}%)")

@kopf.on.create('osg-htc.org', 'v1', 'computeentrypoints')
@kopf.on.update('osg-htc.org', 'v1', 'computeentrypoints')
async def reconcile_compute_entrypoint(spec, meta, status, logger, **kwargs):
    """Handle ComputeEntrypoint creation and updates"""
    name = meta['name']
    namespace = meta['namespace']
    resource_key = f"{namespace}/{name}"
    
    logger.info(f"Reconciling ComputeEntrypoint {resource_key}")
    
    # Check if we can safely update this resource
    if not await can_safely_update_resource(resource_key, meta, logger):
        logger.warning(f"Skipping update for {resource_key} - would violate health threshold")
        raise kopf.TemporaryError("Health threshold would be violated", delay=300)
    
    try:
        # Track this resource in pools
        await track_resource_pools(resource_key, meta, logger)
        
        # Create all required Kubernetes resources
        await ensure_ce_resources(name, namespace, spec, meta, logger)
        
        # Update health tracking
        await update_resource_health(resource_key, 'Reconciling', logger)
        
        return {"message": "Successfully reconciled", "phase": "Running"}
        
    except Exception as e:
        logger.error(f"Failed to reconcile {resource_key}: {e}")
        await update_resource_health(resource_key, 'Failed', logger)
        raise kopf.PermanentError(f"Reconciliation failed: {e}")

@kopf.on.delete('osg-htc.org', 'v1', 'computeentrypoints')
async def cleanup_compute_entrypoint(meta, logger, **kwargs):
    """Handle ComputeEntrypoint deletion"""
    name = meta['name']
    namespace = meta['namespace']
    resource_key = f"{namespace}/{name}"
    
    logger.info(f"Cleaning up ComputeEntrypoint {resource_key}")
    
    # Remove from health tracking
    await remove_from_health_tracking(resource_key, meta, logger)
    
    # Kubernetes will handle cleanup of owned resources via owner references
    logger.info(f"Cleanup completed for {resource_key}")

@kopf.timer('osg-htc.org', 'v1', 'computeentrypoints', interval=60)
async def health_check_timer(spec, meta, status, logger, **kwargs):
    """Periodic health check for ComputeEntrypoint resources"""
    name = meta['name']
    namespace = meta['namespace']
    resource_key = f"{namespace}/{name}"
    
    try:
        # Check deployment health
        health_status = await check_resource_health(name, namespace, logger)
        
        # Update health tracking
        await update_resource_health(resource_key, health_status, logger)
        
        # Return status update
        return {
            "lastHealthCheck": datetime.now(timezone.utc).isoformat(),
            "healthStatus": health_status,
            "phase": "Running" if health_status == "Healthy" else "Degraded"
        }
        
    except Exception as e:
        logger.error(f"Health check failed for {resource_key}: {e}")
        await update_resource_health(resource_key, 'Unknown', logger)
        return {
            "lastHealthCheck": datetime.now(timezone.utc).isoformat(),
            "healthStatus": "Unknown",
            "phase": "Unknown"
        }

@kopf.daemon('osg-htc.org', 'v1', 'computeentrypoints')
async def pool_health_monitor(stopped, logger, **kwargs):
    """Background daemon to monitor pool health"""
    while not stopped:
        try:
            # Log pool health status every 5 minutes
            for pool_name, pool_data in pool_health_tracker.items():
                health_pct = await get_pool_health_percentage(pool_name)
                resource_count = len(pool_data.get('resources', set()))
                logger.info(f"Pool {pool_name}: {health_pct*100:.1f}% healthy ({resource_count} resources)")
            
            await asyncio.sleep(300)  # 5 minutes
            
        except Exception as e:
            logger.error(f"Error in pool health monitoring: {e}")
            await asyncio.sleep(60)

# Helper functions

async def track_resource_pools(resource_key: str, meta: Dict, logger):
    """Track which pools a resource belongs to"""
    labels = meta.get('labels', {})
    
    # Find all pool/ labels that are true
    pools = []
    for label_key, label_value in labels.items():
        if label_key.startswith('pool/') and str(label_value).lower() == 'true':
            pools.append(label_key)
    
    # Update pool tracking
    for pool in pools:
        if pool not in pool_health_tracker:
            pool_health_tracker[pool] = {'resources': set(), 'health': {}}
        pool_health_tracker[pool]['resources'].add(resource_key)
    
    logger.info(f"Tracking {resource_key} in pools: {pools}")

async def remove_from_health_tracking(resource_key: str, meta: Dict, logger):
    """Remove resource from health tracking"""
    labels = meta.get('labels', {})
    
    for label_key, label_value in labels.items():
        if label_key.startswith('pool/') and str(label_value).lower() == 'true':
            pool = label_key
            if pool in pool_health_tracker:
                pool_health_tracker[pool]['resources'].discard(resource_key)
                pool_health_tracker[pool]['health'].pop(resource_key, None)
    
    logger.info(f"Removed {resource_key} from health tracking")

async def can_safely_update_resource(resource_key: str, meta: Dict, logger) -> bool:
    """Check if we can safely update a resource without violating health thresholds"""
    labels = meta.get('labels', {})
    
    for label_key, label_value in labels.items():
        if label_key.startswith('pool/') and str(label_value).lower() == 'true':
            pool = label_key
            pool_health = await get_pool_health_percentage(pool)
            
            # Check if this resource is currently healthy
            current_health = pool_health_tracker.get(pool, {}).get('health', {}).get(resource_key, 'Unknown')
            
            # If this resource is healthy and the pool is at threshold, don't update
            if current_health == 'Healthy' and pool_health <= min_healthy_percentage:
                logger.warning(f"Pool {pool} health at {pool_health*100:.1f}%, threshold is {min_healthy_percentage*100}%")
                return False
    
    return True

async def get_pool_health_percentage(pool_name: str) -> float:
    """Get the percentage of healthy resources in a pool"""
    if pool_name not in pool_health_tracker:
        return 1.0
    
    pool_data = pool_health_tracker[pool_name]
    resources = pool_data['resources']
    health_data = pool_data['health']
    
    if not resources:
        return 1.0
    
    healthy_count = sum(1 for resource in resources if health_data.get(resource) == 'Healthy')
    return healthy_count / len(resources)

async def update_resource_health(resource_key: str, health_status: str, logger):
    """Update health status for a resource"""
    # Find which pools this resource belongs to and update health
    for pool_name, pool_data in pool_health_tracker.items():
        if resource_key in pool_data['resources']:
            pool_data['health'][resource_key] = health_status

async def ensure_ce_resources(name: str, namespace: str, spec: Dict, meta: Dict, logger):
    """Ensure all necessary Kubernetes resources exist for the Hosted CE"""
    
    # 1. Create ConfigMaps
    await ensure_configmaps(name, namespace, spec, meta, logger)
    
    # 2. Create PVCs
    await ensure_pvcs(name, namespace, spec, meta, logger)
    
    # 3. Create Certificate
    await ensure_certificate(name, namespace, spec, meta, logger)
    
    # 4. Create Service
    await ensure_service(name, namespace, spec, meta, logger)
    
    # 5. Create NetworkPolicy
    await ensure_network_policy(name, namespace, meta, logger)
    
    # 6. Create Deployment
    await ensure_deployment(name, namespace, spec, meta, logger)

async def ensure_configmaps(name: str, namespace: str, spec: Dict, meta: Dict, logger):
    """Create ConfigMaps for CE configuration"""
    
    # Main OSG configuration
    osg_config = build_osg_configure_config(spec, meta)
    
    # HTCondor-CE configuration
    htcondor_config = build_htcondor_ce_config(spec, meta)
    
    # SciTokens configuration
    scitokens_config = build_scitokens_config(spec)
    
    configmaps = [
        {
            "name": f"osg-hosted-ce-{name}-configuration",
            "data": {"90-local.ini": osg_config}
        },
        {
            "name": f"osg-hosted-ce-{name}-htcondor-ce-configuration", 
            "data": {"90-instance.conf": htcondor_config}
        },
        {
            "name": f"osg-hosted-ce-{name}-scitokens",
            "data": {"50-scitokens.conf": scitokens_config}
        }
    ]
    
    for cm_data in configmaps:
        configmap = client.V1ConfigMap(
            metadata=client.V1ObjectMeta(
                name=cm_data["name"],
                namespace=namespace,
                labels={"compute-entrypoint": name, "app": "osg-hosted-ce"},
                owner_references=[create_owner_reference(meta)]
            ),
            data=cm_data["data"]
        )
        
        await create_or_update_configmap(configmap, namespace, logger)

async def ensure_pvcs(name: str, namespace: str, spec: Dict, meta: Dict, logger):
    """Create PVCs for the ComputeEntrypoint"""
    k8s_config = spec.get('kubernetes', {})
    storage_config = k8s_config.get('storage', {})
    
    # Create PVCs for each storage configuration
    for storage_name, storage_spec in storage_config.items():
        size = storage_spec.get('size', '5Gi')
        storage_class = storage_spec.get('class')
        access_modes = storage_spec.get('accessModes', ['ReadWriteOnce'])
        
        # Build PVC spec
        pvc_spec = client.V1PersistentVolumeClaimSpec(
            access_modes=access_modes,
            resources=client.V1ResourceRequirements(
                requests={'storage': size}
            )
        )
        
        if storage_class:
            pvc_spec.storage_class_name = storage_class
        
        # Create PVC with finalizer to prevent deletion
        pvc = client.V1PersistentVolumeClaim(
            metadata=client.V1ObjectMeta(
                name=f"osg-hosted-ce-{name}-{storage_name}",
                namespace=namespace,
                labels={"compute-entrypoint": name, "app": "osg-hosted-ce", "storage": storage_name},
                finalizers=["kubernetes.io/pvc-protection"],
                owner_references=[create_owner_reference(meta)]
            ),
            spec=pvc_spec
        )
        
        try:
            core_v1.create_namespaced_persistent_volume_claim(namespace=namespace, body=pvc)
            logger.info(f"Created PVC osg-hosted-ce-{name}-{storage_name}")
        except ApiException as e:
            if e.status == 409:
                logger.info(f"PVC osg-hosted-ce-{name}-{storage_name} already exists")
            else:
                raise

async def ensure_certificate(name: str, namespace: str, spec: Dict, meta: Dict, logger):
    """Create Certificate for the ComputeEntrypoint"""
    certificate_config = spec.get('certificate', {})
    k8s_config = spec.get('kubernetes', {})
    
    # Skip certificate creation if not configured
    if not certificate_config:
        logger.info(f"No certificate configuration found for {name}")
        return
    
    issuer_ref = certificate_config.get('issuerRef')
    if not issuer_ref:
        logger.warning(f"Certificate configuration missing issuerRef for {name}")
        return
    
    hostname = k8s_config.get('hostname', f"{name}.svc.opensciencegrid.org")
    
    # Create Certificate resource using dynamic client for CRDs
    certificate_body = {
        "apiVersion": "cert-manager.io/v1",
        "kind": "Certificate",
        "metadata": {
            "name": f"{name}-cert",
            "namespace": namespace,
            "labels": {"compute-entrypoint": name, "app": "osg-hosted-ce"},
            "ownerReferences": [create_owner_reference_dict(meta)]
        },
        "spec": {
            "secretName": f"{name}-cert",
            "commonName": hostname,
            "dnsNames": [hostname],
            "usages": ["server auth", "client auth"],
            "issuerRef": {
                "name": issuer_ref,
                "kind": "ClusterIssuer"
            }
        }
    }
    
    try:
        # Use dynamic client to create cert-manager Certificate
        from kubernetes import dynamic
        from kubernetes.client import api_client
        
        dyn_client = dynamic.DynamicClient(api_client.ApiClient())
        cert_api = dyn_client.resources.get(api_version="cert-manager.io/v1", kind="Certificate")
        
        try:
            cert_api.create(namespace=namespace, body=certificate_body)
            logger.info(f"Created Certificate {name}-cert")
        except Exception as e:
            if "already exists" in str(e):
                cert_api.patch(namespace=namespace, name=f"{name}-cert", body=certificate_body)
                logger.info(f"Updated Certificate {name}-cert")
            else:
                raise
                
    except ImportError:
        logger.warning("cert-manager not available, skipping certificate creation")
    except Exception as e:
        logger.error(f"Failed to create certificate for {name}: {e}")
        raise

def create_owner_reference_dict(meta: Dict) -> Dict:
    """Create owner reference dictionary for cert-manager resources"""
    return {
        "apiVersion": "osg-htc.org/v1",
        "kind": "ComputeEntrypoint",
        "name": meta['name'],
        "uid": meta['uid'],
        "controller": True,
        "blockOwnerDeletion": True
    }

async def ensure_service(name: str, namespace: str, spec: Dict, meta: Dict, logger):
    """Create Service for the ComputeEntrypoint"""
    k8s_config = spec.get('kubernetes', {})
    hostname = k8s_config.get('hostname', f"{name}.svc.opensciencegrid.org")
    
    # Get service annotations
    service_annotations = k8s_config.get('service', {}).get('annotations', {})
    service_annotations['external-dns.alpha.kubernetes.io/hostname'] = hostname
    
    service = client.V1Service(
        metadata=client.V1ObjectMeta(
            name=f"osg-hosted-ce-{name}",
            namespace=namespace,
            labels={"compute-entrypoint": name, "app": "osg-hosted-ce"},
            annotations=service_annotations,
            owner_references=[create_owner_reference(meta)]
        ),
        spec=client.V1ServiceSpec(
            type="LoadBalancer",
            external_traffic_policy="Local",
            selector={"compute-entrypoint": name},
            ports=[
                client.V1ServicePort(
                    name="htcondor-ce",
                    port=9619,
                    target_port=9619,
                    protocol="TCP"
                )
            ]
        )
    )
    
    try:
        core_v1.create_namespaced_service(namespace=namespace, body=service)
        logger.info(f"Created Service osg-hosted-ce-{name}")
    except ApiException as e:
        if e.status == 409:
            core_v1.patch_namespaced_service(
                name=f"osg-hosted-ce-{name}", namespace=namespace, body=service
            )
            logger.info(f"Updated Service osg-hosted-ce-{name}")
        else:
            raise

async def ensure_network_policy(name: str, namespace: str, meta: Dict, logger):
    """Create NetworkPolicy for the ComputeEntrypoint"""
    network_policy = client.V1NetworkPolicy(
        metadata=client.V1ObjectMeta(
            name=f"allow-osg-hosted-ce-{name}",
            namespace=namespace,
            labels={"compute-entrypoint": name},
            owner_references=[create_owner_reference(meta)]
        ),
        spec=client.V1NetworkPolicySpec(
            pod_selector=client.V1LabelSelector(
                match_labels={"compute-entrypoint": name}
            ),
            policy_types=["Ingress", "Egress"],
            ingress=[
                client.V1NetworkPolicyIngressRule(
                    ports=[
                        client.V1NetworkPolicyPort(protocol="TCP", port=9619)
                    ]
                )
            ],
            egress=[client.V1NetworkPolicyEgressRule()]
        )
    )
    
    try:
        networking_v1.create_namespaced_network_policy(namespace=namespace, body=network_policy)
        logger.info(f"Created NetworkPolicy allow-osg-hosted-ce-{name}")
    except ApiException as e:
        if e.status == 409:
            logger.info(f"NetworkPolicy allow-osg-hosted-ce-{name} already exists")
        else:
            raise

async def ensure_deployment(name: str, namespace: str, spec: Dict, meta: Dict, logger):
    """Create Deployment for the ComputeEntrypoint"""
    k8s_config = spec.get('kubernetes', {})
    cluster_config = spec.get('cluster', {})
    
    # Build container environment
    env_vars = [
        client.V1EnvVar(name="CE_HOSTNAME", value=k8s_config.get('hostname', f"{name}.svc.opensciencegrid.org")),
        client.V1EnvVar(name="CE_CONTACT", value=meta.get('annotation', {}).get('topology/contact-email', 'unknown@example.org')),
        client.V1EnvVar(name="RESOURCE_NAME", value=meta.get('annotation', {}).get('topology/resource', name)),
        client.V1EnvVar(name="REMOTE_HOST", value=cluster_config.get('host', 'localhost')),
        client.V1EnvVar(name="REMOTE_BATCH", value=cluster_config.get('batch', 'slurm').lower()),
        client.V1EnvVar(name="REMOTE_BOSCO_DIR", value=spec.get('bosco', {}).get('dir', 'bosco')),
        client.V1EnvVar(name="DEVELOPER", value="false"),
        client.V1EnvVar(name="LE_STAGING", value="false"),
    ]
    
    # Add BOSCO_TARBALL_URL if specified
    bosco_config = spec.get('bosco', {})
    tarball_url = bosco_config.get('tarball')
    if tarball_url:
        env_vars.append(client.V1EnvVar(name="BOSCO_TARBALL_URL", value=tarball_url))
    
    # Build volume mounts
    volume_mounts = [
        client.V1VolumeMount(
            name="osg-config",
            mount_path="/tmp/90-local.ini",
            sub_path="90-local.ini"
        ),
        client.V1VolumeMount(
            name="htcondor-config",
            mount_path="/etc/condor-ce/config.d/90-instance.conf",
            sub_path="90-instance.conf"
        ),
        client.V1VolumeMount(
            name="scitokens-config",
            mount_path="/etc/condor-ce/mapfiles.d/50-scitokens.conf",
            sub_path="50-scitokens.conf"
        )
    ]
    
    # Add storage volume mounts based on osg-hosted-ce reference
    storage_config = k8s_config.get('storage', {})
    storage_mount_paths = {
        'lib': '/var/lib/condor-ce',
        'log': '/var/log/condor-ce', 
        'key': '/etc/condor-ce/passwords.d/'
    }
    
    for storage_name in storage_config.keys():
        if storage_name not in storage_mount_paths:
            raise ValueError(f"Unknown storage type '{storage_name}'. Supported types: {list(storage_mount_paths.keys())}")
        
        volume_mounts.append(
            client.V1VolumeMount(
                name=f"storage-{storage_name}",
                mount_path=storage_mount_paths[storage_name]
            )
        )
    
    # Build volumes
    volumes = [
        client.V1Volume(
            name="osg-config",
            config_map=client.V1ConfigMapVolumeSource(
                name=f"osg-hosted-ce-{name}-configuration"
            )
        ),
        client.V1Volume(
            name="htcondor-config",
            config_map=client.V1ConfigMapVolumeSource(
                name=f"osg-hosted-ce-{name}-htcondor-ce-configuration"
            )
        ),
        client.V1Volume(
            name="scitokens-config",
            config_map=client.V1ConfigMapVolumeSource(
                name=f"osg-hosted-ce-{name}-scitokens"
            )
        )
    ]
    
    # Add storage volumes
    for storage_name in storage_config.keys():
        volumes.append(
            client.V1Volume(
                name=f"storage-{storage_name}",
                persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                    claim_name=f"osg-hosted-ce-{name}-{storage_name}"
                )
            )
        )
    
    # Add certificate volume if configured
    certificate_config = spec.get('certificate', {})
    if certificate_config:
        volumes.append(
            client.V1Volume(
                name="host-cert-key",
                secret=client.V1SecretVolumeSource(
                    secret_name=f"{name}-cert",
                    items=[
                        client.V1KeyToPath(key="tls.crt", path="hostcert.pem", mode=0o644),
                        client.V1KeyToPath(key="tls.key", path="hostkey.pem", mode=0o400)
                    ]
                )
            )
        )
        volume_mounts.append(
            client.V1VolumeMount(
                name="host-cert-key",
                mount_path="/etc/grid-security-orig.d"
            )
        )

    # Add SSH key volume if specified
    ssh_key_secret = cluster_config.get('ssh', {}).get('key')
    if ssh_key_secret:
        volumes.append(
            client.V1Volume(
                name="ssh-key",
                secret=client.V1SecretVolumeSource(
                    secret_name=ssh_key_secret,
                    items=[
                        client.V1KeyToPath(key="bosco.key", path="bosco.key", mode=0o400)
                    ]
                )
            )
        )
        volume_mounts.append(
            client.V1VolumeMount(
                name="ssh-key",
                mount_path="/etc/osg/bosco.key",
                sub_path="bosco.key"
            )
        )
    
    # Build container
    container_args = {
        "name": "osg-hosted-ce",
        "image": k8s_config.get('image', 'hub.opensciencegrid.org/osg-htc/hosted-ce:24-release'),
        "image_pull_policy": "Always",
        "ports": [
            client.V1ContainerPort(container_port=9619, name="htcondor-ce", protocol="TCP")
        ],
        "env": env_vars,
        "volume_mounts": volume_mounts,
        "resources": client.V1ResourceRequirements(
            requests={"cpu": "500m", "memory": "1Gi"},
            limits={"cpu": "2", "memory": "4Gi"}
        )
    }
    
    # Override command for debugging if sleep is enabled
    if k8s_config.get('sleep', False):
        container_args["command"] = ["/bin/sleep"]
        container_args["args"] = ["infinity"]
    
    container = client.V1Container(**container_args)
    
    # Build deployment
    deployment = client.V1Deployment(
        metadata=client.V1ObjectMeta(
            name=f"osg-hosted-ce-{name}",
            namespace=namespace,
            labels={"compute-entrypoint": name, "app": "osg-hosted-ce"},
            owner_references=[create_owner_reference(meta)]
        ),
        spec=client.V1DeploymentSpec(
            replicas=k8s_config.get('replicas', 1),
            selector=client.V1LabelSelector(
                match_labels={"compute-entrypoint": name}
            ),
            template=client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(
                    labels={"compute-entrypoint": name, "app": "osg-hosted-ce"}
                ),
                spec=client.V1PodSpec(
                    containers=[container],
                    volumes=volumes,
                    enable_service_links=False,
                    node_selector=k8s_config.get('node', {}).get('labels', {})
                )
            ),
            strategy=client.V1DeploymentStrategy(type="Recreate")
        )
    )
    
    try:
        apps_v1.create_namespaced_deployment(namespace=namespace, body=deployment)
        logger.info(f"Created Deployment osg-hosted-ce-{name}")
    except ApiException as e:
        if e.status == 409:
            apps_v1.patch_namespaced_deployment(
                name=f"osg-hosted-ce-{name}", namespace=namespace, body=deployment
            )
            logger.info(f"Updated Deployment osg-hosted-ce-{name}")
        else:
            raise

def build_osg_configure_config(spec: Dict, meta: Dict) -> str:
    """Build OSG-Configure configuration (90-local.ini)"""
    cluster_config = spec.get('cluster', {})
    annotations = meta.get('annotation', {})
    
    # Extract topology information from annotations
    resource_name = annotations.get('topology/resource', 'UNKNOWN')
    resource_group = annotations.get('topology/resource-group', 'UNKNOWN')
    sponsor = annotations.get('topology/sponsor', 'osg:100')
    contact = annotations.get('topology/contact', 'Unknown')
    contact_email = annotations.get('topology/contact-email', 'unknown@example.org')
    city = annotations.get('topology/site-city', 'Unknown')
    country = annotations.get('topology/site-country', 'US')
    latitude = annotations.get('topology/site-latitude', '0.00')
    longitude = annotations.get('topology/site-longitude', '0.00')
    
    # Determine if this is production or ITB based on topology annotation
    is_production = annotations.get('topology/production', 'false').lower() == 'true'
    group = "OSG" if is_production else "OSG-ITB"
    
    # Get GRACC site for accounting
    gracc_site = annotations.get('gracc/site', resource_name)
    
    config_lines = [
        "[Gateway]",
        "htcondor_gateway_enabled = True",
        "job_envvar_path=$PATH",
        "",
        "[Site Information]",
        f"group = {group}",
        f"host_name = localhost",
        f"resource = {resource_name}",
        f"resource_group = {resource_group}",
        f"sponsor = {sponsor}",
        f"contact = {contact}",
        f"email = {contact_email}",
        f"city = {city}",
        f"country = {country}",
        f"latitude = {latitude}",
        f"longitude = {longitude}",
        f"batch_systems = {cluster_config.get('batch', 'slurm')}",
        "",
        "[BOSCO]",
        "enabled = False",
        "",
        "[Storage]",
        f"grid_dir = {cluster_config.get('scratch', '/tmp')}",
        f"worker_node_temp = {cluster_config.get('scratch', '/tmp')}",
        f"app_dir = {cluster_config.get('scratch', '/tmp')}",
        f"data_dir = {cluster_config.get('scratch', '/tmp')}",
        f"site_read = {cluster_config.get('scratch', '/tmp')}",
        f"site_write = {cluster_config.get('scratch', '/tmp')}",
        "",
        "[Gratia]",
        "enabled = True",
        f"resource = {gracc_site}",
        "probes = jobmanager:condor",
        "",
        "[Info Services]",
        "enabled = True",
        "ce_collectors = collector1.opensciencegrid.org:9619,collector2.opensciencegrid.org:9619",
        "",
        f"[Subcluster {resource_name}]",
        f"name = {resource_name}",
        "ram_mb = 4000",
        "cores_per_node = 1", 
        "max_wall_time = 1440",
        "allowed_vos = *",
        "",
        "[Squid]",
        f"enabled = {'True' if cluster_config.get('squid') else 'False'}",
        f"location = {cluster_config.get('squid', '')}",
    ]
    
    # Add pilot-specific configuration sections
    pilot_configs = spec.get('pilot', [])
    for pilot in pilot_configs:
        pilot_name = pilot.get('name', 'default')
        queue = pilot.get('queue', 'default')
        max_pilots = pilot.get('limit', 2)
        walltime = pilot.get('walltime', 1440)
        resources = pilot.get('resources', {})
        vos = pilot.get('vo', [])
        whole_node = pilot.get('wholeNode', False)
        apptainer = pilot.get('apptainer', False)
        
        # Get OS field - required when require_singularity is False
        pilot_os = pilot.get('os', 'rhel8')  # Default to rhel8
        
        config_lines.extend([
            "",
            f"[Pilot {pilot_name}]",
            f"allowed_vos = {','.join(vos) if vos else '*'}",
            f"queue = {queue}",
            f"cpucount = {resources.get('cpu', 1)}",
            f"ram_mb = {resources.get('ram', 2500)}",  # Default to 2500 per OSG docs
            f"max_wall_time = {walltime}",
            f"max_pilots = {max_pilots}",
            f"whole_node = {str(whole_node).lower()}",
            f"gpucount = {resources.get('gpu', 0)}",
            f"require_singularity = {str(apptainer).lower()}"
        ])
        
        # Add OS field only if require_singularity is False
        if not apptainer:
            config_lines.append(f"os = {pilot_os}")
    
    return "\n".join(config_lines)

def build_htcondor_ce_config(spec: Dict, meta: Dict) -> str:
    """Build HTCondor-CE configuration"""
    cluster_config = spec.get('cluster', {})
    k8s_config = spec.get('kubernetes', {})
    bosco_config = spec.get('bosco', {})
    pilot_configs = spec.get('pilot', [])
    custom_config = spec.get('config', '')
    
    config_lines = [
        "# Generated HTCondor-CE Configuration",
        f"SLATE_HOSTED_CE = True",
        f"OSG_HOSTED_CE = True", 
        f"SCHEDD_ATTRS = $(SCHEDD_ATTRS) SLATE_HOSTED_CE OSG_HOSTED_CE",
        "",
    ]
    
    # Add hostname configuration if specified
    if k8s_config.get('hostname'):
        config_lines.extend([
            f"TCP_FORWARDING_HOST = {k8s_config['hostname']}",
            ""
        ])
    
    # Build job router entries
    config_lines.extend([
        "# Job Router Configuration",
        "JOB_ROUTER_ENTRIES = [Name = \"Hosted_CE_default_route\"]",
        "JOB_ROUTER_ROUTE_NAMES = Hosted_CE_default_route",
        ""
    ])
    
    
    # Add GridResource transform
    config_lines.extend([
        "JOB_ROUTER_TRANSFORM_GridResource @=jrt",
        f"    hosted_grid_resource = batch {cluster_config.get('batch', 'slurm')} $(MY.Owner)@{cluster_config.get('host', 'localhost')} --rgahp-glite ~/{bosco_config.get('dir', 'bosco')}/glite",
        "    SET GridResource \"$(hosted_grid_resource)\"",
        "@jrt",
        "JOB_ROUTER_PRE_ROUTE_TRANSFORM_NAMES = $(JOB_ROUTER_PRE_ROUTE_TRANSFORM_NAMES) GridResource",
        ""
    ])
    
    # Add ID token generation for glidein -> CE collector advertising (from users section)
    users = spec.get('users', [])
    idtoken_names = []
    for user_config in users:
        username = user_config.get('user')
        if username:
            # Create safe name for HTCondor config (replace non-alphanumeric with underscore)
            safe_name = ''.join(c if c.isalnum() else '_' for c in username)
            idtoken_names.append(safe_name)
            
            config_lines.extend([
                f"JOB_ROUTER_CREATE_IDTOKEN_{safe_name} @=end",
                f"  sub = \"{username}@users.htcondor.org\"",
                f"  kid = \"POOL\"",
                f"  lifetime = 604800",
                f"  scope = \"ADVERTISE_STARTD, ADVERTISE_MASTER, READ\"",
                f"  dir = \"/usr/share/condor-ce/glidein-tokens/{username}\"",
                f"  filename = \"ce_{username}.idtoken\"",
                f"  owner = \"{username}\"",
                "@end",
                ""
            ])
    
    if idtoken_names:
        config_lines.extend([
            f"JOB_ROUTER_CREATE_IDTOKEN_NAMES = $(JOB_ROUTER_CREATE_IDTOKEN_NAMES) {' '.join(idtoken_names)}",
            ""
        ])
    
    # Add custom configuration
    if custom_config:
        config_lines.extend([
            "# Custom Configuration", 
            custom_config
        ])
    
    return "\n".join(config_lines)

def build_scitokens_config(spec: Dict) -> str:
    """Build SciTokens configuration"""
    config_lines = []
    
    # Add user mappings based on users section
    users = spec.get('users', [])
    for user_config in users:
        username = user_config.get('user')
        scitoken = user_config.get('scitoken')
        
        if username and scitoken:
            # Escape special regex characters for URL matching (following helm template pattern)
            escaped_token = scitoken.replace('/', '\\/').replace('.', '\\.').replace('-', '\\-')
            # Add comma if not already present (following helm template logic)
            if ',' not in scitoken:
                escaped_token += ','
            config_lines.append(f"SCITOKENS /^{escaped_token}/ {username}")
    
    return "\n".join(config_lines) if config_lines else "# No SciTokens mappings configured"

async def create_or_update_configmap(configmap: client.V1ConfigMap, namespace: str, logger):
    """Create or update a ConfigMap"""
    try:
        core_v1.create_namespaced_config_map(namespace=namespace, body=configmap)
        logger.info(f"Created ConfigMap {configmap.metadata.name}")
    except ApiException as e:
        if e.status == 409:
            core_v1.patch_namespaced_config_map(
                name=configmap.metadata.name, namespace=namespace, body=configmap
            )
            logger.info(f"Updated ConfigMap {configmap.metadata.name}")
        else:
            raise

async def check_resource_health(name: str, namespace: str, logger) -> str:
    """Check health of a specific ComputeEntrypoint resource"""
    try:
        # Check deployment status
        deployment = apps_v1.read_namespaced_deployment(
            name=f"osg-hosted-ce-{name}",
            namespace=namespace
        )
        
        if not deployment.status:
            return "Unknown"
        
        desired_replicas = deployment.spec.replicas or 1
        ready_replicas = deployment.status.ready_replicas or 0
        
        if ready_replicas >= desired_replicas:
            return "Healthy"
        elif ready_replicas > 0:
            return "Degraded"
        else:
            return "Unhealthy"
            
    except ApiException as e:
        if e.status == 404:
            return "Missing"
        logger.error(f"Error checking health for {name}: {e}")
        return "Unknown"

def create_owner_reference(meta: Dict) -> client.V1OwnerReference:
    """Create owner reference for child resources"""
    return client.V1OwnerReference(
        api_version="osg-htc.org/v1",
        kind="ComputeEntrypoint", 
        name=meta['name'],
        uid=meta['uid'],
        controller=True,
        block_owner_deletion=True
    )

if __name__ == '__main__':
    kopf.run()