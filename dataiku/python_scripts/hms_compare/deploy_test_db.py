from kubernetes import client
import time
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from util import get_param, get_credential, get_zone_name, get_s3_location_list, replace_vars_in_string

# === Configuration ===
K8S_HOST = get_param('K8S_HOST', 'lcoalhost:8443')
NAMESPACE = get_param('NAMESPACE', 'dpr-uat-infrapz')
SERVICE_NAME = get_param('SERVICE_NAME', 'postgresql-hl-test')
SERVICE_PORT = get_param('SERVICE_PORT', '5442')
STS_NAME = get_param('STS_NAME', 'postgresql-test')
CRONJOB_NAME = get_param('CRONJOB_NAME', 'postgres-recovery-v2')
PSQL_IMAGE = get_param('PSQL_IMAGE','postgresql:16.6.0-debian-12-r2')
API_TOKEN = get_credential('K8S_API_TOKEN', '')
TIMESTAMP = get_param('TIMESTAMP','1')


# === Configure Kubernetes client ===
config = client.Configuration()
config.host = K8S_HOST
config.verify_ssl = False
config.api_key = {"authorization": f"Bearer {API_TOKEN}"}
client.Configuration.set_default(config)

# === Kubernetes API Clients ===
core_v1 = client.CoreV1Api()
apps_v1 = client.AppsV1Api()
batch_v1 = client.BatchV1Api()

# === Service Definition ===
service_manifest = client.V1Service(
    api_version="v1",
    kind="Service",
    metadata=client.V1ObjectMeta(name=SERVICE_NAME),
    spec=client.V1ServiceSpec(
        cluster_ip="None",
        internal_traffic_policy="Cluster",
        ip_families=["IPv4"],
        ip_family_policy="SingleStack",
        ports=[
            client.V1ServicePort(
                name="tcp-postgresql",
                port=int(SERVICE_PORT),
                protocol="TCP",
                target_port="tcp-postgresql"
            )
        ],
        publish_not_ready_addresses=True,
        selector={
            "app.kubernetes.io/component": "primary",
            "app.kubernetes.io/instance": "dpr-infra",
            "app.kubernetes.io/name": STS_NAME
        },
        session_affinity="None",
        type="ClusterIP"
    )
)

# === Environment Variables ===
env_vars = [
    client.V1EnvVar(name="BITNAMI_DEBUG", value="false"),
    client.V1EnvVar(name="POSTGRESQL_PORT_NUMBER", value=SERVICE_PORT),
    client.V1EnvVar(name="POSTGRESQL_VOLUME_DIR", value="/bitnami/postgresql"),
    client.V1EnvVar(name="PGDATA", value="/bitnami/postgresql/data"),
    client.V1EnvVar(
        name="POSTGRES_PASSWORD",
        value_from=client.V1EnvVarSource(
            secret_key_ref=client.V1SecretKeySelector(name="postgresql", key="postgres-password")
        )
    ),
    client.V1EnvVar(name="POSTGRES_INITSCRIPTS_USERNAME", value="postgres"),
    client.V1EnvVar(name="POSTGRES_INITSCRIPTS_PASSWORD", value="postgres"),
    client.V1EnvVar(name="POSTGRESQL_ENABLE_LDAP", value="no"),
    client.V1EnvVar(name="POSTGRESQL_ENABLE_TLS", value="no"),
    client.V1EnvVar(name="POSTGRESQL_LOG_HOSTNAME", value="false"),
    client.V1EnvVar(name="POSTGRESQL_LOG_CONNECTIONS", value="false"),
    client.V1EnvVar(name="POSTGRESQL_LOG_DISCONNECTIONS", value="false"),
    client.V1EnvVar(name="POSTGRESQL_PGAUDIT_LOG_CATALOG", value="off"),
    client.V1EnvVar(name="POSTGRESQL_CLIENT_MIN_MESSAGES", value="error"),
    client.V1EnvVar(name="POSTGRESQL_SHARED_PRELOAD_LIBRARIES", value="pgaudit"),
]

# === Security and Resource Config ===
security_context = client.V1SecurityContext(
    allow_privilege_escalation=False,
    capabilities=client.V1Capabilities(drop=["ALL"]),
    privileged=False,
    read_only_root_filesystem=False,
    run_as_non_root=True,
    run_as_user=1001,
    seccomp_profile=client.V1SeccompProfile(type="RuntimeDefault")
)

resources = client.V1ResourceRequirements(
    limits={"cpu": "6", "memory": "32Gi", "ephemeral-storage": "8Gi"},
    requests={"cpu": "6", "memory": "32Gi", "ephemeral-storage": "8Gi"}
)

# === Liveness and Readiness Probes ===
liveness_probe = client.V1Probe(
    _exec=client.V1ExecAction(command=["/bin/sh", "-c", f'exec pg_isready -U "postgres" -h 127.0.0.1 -p {SERVICE_PORT}']),
    initial_delay_seconds=30,
    period_seconds=10,
    failure_threshold=6,
    timeout_seconds=5
)

readiness_probe = client.V1Probe(
    _exec=client.V1ExecAction(command=["/bin/sh", "-c", f'exec pg_isready -U "postgres" -h 127.0.0.1 -p {SERVICE_PORT}']),
    initial_delay_seconds=5,
    period_seconds=10,
    failure_threshold=6,
    timeout_seconds=5
)

# === PostgreSQL Container ===
postgres_container = client.V1Container(
    name="postgresql",
    image=PSQL_IMAGE,
    image_pull_policy="Always",
    env=env_vars,
    ports=[client.V1ContainerPort(container_port=int(SERVICE_PORT), name="tcp-postgresql", protocol="TCP")],
    liveness_probe=liveness_probe,
    readiness_probe=readiness_probe,
    resources=resources,
    security_context=security_context,
    volume_mounts=[
        client.V1VolumeMount(mount_path="/dev/shm", name="dshm"),
        client.V1VolumeMount(mount_path="/bitnami/postgresql", name="data")
    ]
)

# === Pod Template ===
pod_template = client.V1PodTemplateSpec(
    metadata=client.V1ObjectMeta(
        name=STS_NAME,
        labels={
            "app.kubernetes.io/component": "primary",
            "app.kubernetes.io/instance": "dpr-infra",
            "app.kubernetes.io/name": STS_NAME
        }
    ),
    spec=client.V1PodSpec(
        containers=[postgres_container],
        image_pull_secrets=[client.V1LocalObjectReference(name="dpr-harbor-secret")],
        service_account_name="default",
        security_context=client.V1PodSecurityContext(fs_group=1001),
        dns_policy="ClusterFirst",
        restart_policy="Always",
        scheduler_name="default-scheduler",
        termination_grace_period_seconds=30,
        volumes=[
            client.V1Volume(name="dshm", empty_dir=client.V1EmptyDirVolumeSource(medium="Memory"))
        ]
    )
)

# === PVC Template ===
pvc_template = client.V1PersistentVolumeClaim(
    metadata=client.V1ObjectMeta(name="data"),
    spec=client.V1PersistentVolumeClaimSpec(
        access_modes=["ReadWriteOnce"],
        storage_class_name="trident-ontap-nas-economy",
        volume_mode="Filesystem",
        resources=client.V1ResourceRequirements(requests={"storage": "10Gi"})
    )
)

# === StatefulSet Definition ===
statefulset = client.V1StatefulSet(
    metadata=client.V1ObjectMeta(name=STS_NAME, namespace=NAMESPACE),
    spec=client.V1StatefulSetSpec(
        service_name=SERVICE_NAME,
        replicas=1,
        selector=client.V1LabelSelector(
            match_labels={
                "app.kubernetes.io/component": "primary",
                "app.kubernetes.io/instance": "dpr-infra",
                "app.kubernetes.io/name": STS_NAME
            }
        ),
        template=pod_template,
        volume_claim_templates=[pvc_template],
        pod_management_policy="OrderedReady",
        update_strategy=client.V1StatefulSetUpdateStrategy(
            type="RollingUpdate",
            rolling_update=client.V1RollingUpdateStatefulSetStrategy(partition=0)
        ),
        persistent_volume_claim_retention_policy=client.V1StatefulSetPersistentVolumeClaimRetentionPolicy(
            when_deleted="Delete",
            when_scaled="Retain"
        )
    )
)

# === Helper Functions ===
def wait_for_pod_ready(label_selector, namespace, timeout=300, interval=5):
    """Wait for a pod with the given label selector to be ready."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        pods = core_v1.list_namespaced_pod(namespace=namespace, label_selector=label_selector).items
        for pod in pods:
            conditions = pod.status.conditions or []
            for condition in conditions:
                if condition.type == "Ready" and condition.status == "True":
                    print(f"Pod {pod.metadata.name} is ready.")
                    return pod
        print("Waiting for pod to be ready...")
        time.sleep(interval)
    raise TimeoutError("Timed out waiting for pod to become ready.")

def wait_for_job_completion(job_name, namespace, timeout=1200, interval=5):
    """
    Waits for the Kubernetes Job to complete (or fail).
    Raises an error if it fails or times out.
    """
    start_time = time.time()

    while time.time() - start_time < timeout:
        job = batch_v1.read_namespaced_job(name=job_name, namespace=NAMESPACE)
        status = job.status

        if status.succeeded:
            print(f"Job '{job_name}' completed successfully.")
            return

        if status.failed:
            raise RuntimeError(f"Job '{job_name}' failed.")

        print(f"Waiting for job '{job_name}' to complete...")
        time.sleep(interval)

    raise TimeoutError(f"Job '{job_name}' did not complete within {timeout} seconds.")

    
print("-----------------------------------------------------EXECUTION------------------------------------------------------------------------------------------------")
# === Deploy Resources ===
core_v1.create_namespaced_service(namespace=NAMESPACE, body=service_manifest)
apps_v1.create_namespaced_stateful_set(namespace=NAMESPACE, body=statefulset)

# === Wait for the StatefulSet to be ready ===
label_selector = f"app.kubernetes.io/component=primary,app.kubernetes.io/instance=dpr-infra,app.kubernetes.io/name={STS_NAME}"
wait_for_pod_ready(label_selector=label_selector, namespace=NAMESPACE)


# === Patch CronJob Env Vars and Trigger Manual Run ===
cronjob = batch_v1.read_namespaced_cron_job(name=CRONJOB_NAME, namespace=NAMESPACE)
container = cronjob.spec.job_template.spec.template.spec.containers[0]

# === Add environment variables ===
new_env_vars = [
    client.V1EnvVar(name="EPOCH_TIMESTAMP", value=TIMESTAMP),
    client.V1EnvVar(name="PG_TARGET_HOST", value=SERVICE_NAME),
    client.V1EnvVar(name="PG_TARGET_PORT", value=SERVICE_PORT)
]
container.env = (container.env or []) + new_env_vars

# === Patch CronJob ===
batch_v1.patch_namespaced_cron_job(name=CRONJOB_NAME, namespace=NAMESPACE, body=cronjob)

# === Manually trigger the job ===
job_name = f"{CRONJOB_NAME}-test-{int(time.time())}"
job_spec = client.V1Job(
    metadata = client.V1ObjectMeta(name=job_name),
    spec = cronjob.spec.job_template.spec
)

batch_v1.create_namespaced_job(namespace=NAMESPACE, body=job_spec)
wait_for_job_completion(job_name=job_name, namespace=NAMESPACE)

print("------------------------------------------------------------------------------------------------------------------------------------------------------------")

# === Cleanup ===
#core_v1.delete_namespaced_service(name=SERVICE_NAME, namespace=NAMESPACE)
#apps_v1.delete_namespaced_stateful_set(name=STS_NAME, namespace=NAMESPACE)
                     