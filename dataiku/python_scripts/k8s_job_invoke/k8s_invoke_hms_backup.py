from kubernetes import client, config
import datetime
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))
from util import create_k8s_client


batch_v1 = client.BatchV1Api()

namespace = "dpr-uat-infrapz"
cronjob_name = "postgres-recovery-v2"

# Get the CronJob object
cronjob = batch_v1.read_namespaced_cron_job(cronjob_name, namespace)

# Generate a unique job name (CronJobs normally do this automatically)
timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
job_name = f"{cronjob_name}-manual-{timestamp}"

# Build a Job from the CronJob's template
job = client.V1Job(
    api_version="batch/v1",
    kind="Job",
    metadata=client.V1ObjectMeta(
        name=job_name,
        labels=cronjob.spec.job_template.metadata.labels
    ),
    spec=cronjob.spec.job_template.spec
)

# Create the Job
batch_v1.create_namespaced_job(namespace=namespace, body=job)

print(f"Manually triggered CronJob '{cronjob_name}' as Job '{job_name}'")
