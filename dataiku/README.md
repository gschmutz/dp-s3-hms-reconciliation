# Dataiku

## compare\_s3_to\_hms

Step Define Variables

```
{
   "HMS_DB_HOST": "10.156.72.217",
   "HMS_DB_PORT": "5442",
   "HMS_DB_DBNAME": "metastore_db",
   "S3_ENDPOINT_URL": "http://10.156.72.217:9000",
   "S3_BASELINE_BUCKET": "admin-bucket",
   "S3_BASELINE_OBJECT_NAME": "baseline_s3.csv"
}
```

Step Excute Python Test 

 * **Unit tests to run**: `python/s3_hms_compare`
 * **Code env**: Select an environment
 * **Envrionment**: `test`
 * **Ignore failure**: `true`
 * **Log level**: `INFO`


Step custom python

```python
from dataiku.scenario import Scenario

# The Scenario object is the main handle from which you initiate steps
scenario = Scenario()

from allure_uploader import send_allure_results

if __name__ == "__main__":
    report_url = send_allure_results(
        allure_results_directory='../report/allure-results/s3-hms-compare',
        allure_server='http://10.156.72.217:28278',
        project_id='s3-hms-compare',
        security_user='admin',
        security_password='abc123!',
        ssl_verification=True
    )

    print("The Allure report is available at:", report_url)
    
scenario.set_scenario_variables(allure_report_url=report_url)
    
```

Step custom python

```
import time
import dataiku
from dataiku.scenario import Scenario

# The Scenario object is the main handle from which you initiate steps
scenario = Scenario()

my_param_value = scenario.get_trigger_params().get("requestId")

print("Variables: ", scenario.get_all_variables())

client = dataiku.api_client()
auth_info = client.get_auth_info(with_secrets=True)
print (auth_info["secrets"])



# Get current project
project = client.get_default_project()
print (project)
print("Project key:", project.project_key)
print("Project variables:", project.get_variables())
```

## compare\_table\_metrics

Define Variables step

```json
{
   "TRINO_HOST": "10.156.72.217",
   "TRINO_PORT": "28082",
   "KAFKA_BOOTSTRAP_SERVERS": "10.156.72.217:9092",
   "KAFKA_SECURITY_PROTOCOL": "PLAINTEXT"
}
```

Step Excute Python Test 

 * **Unit tests to run**: `python/table_metrics_compare/`
 * **Code env**: Select an environment
 * **Envrionment**: `test`
 * **Ignore failure**: `true`
 * **Log level**: `INFO`


Step custom python

```python
from dataiku.scenario import Scenario

# The Scenario object is the main handle from which you initiate steps
scenario = Scenario()

from allure_uploader import send_allure_results

if __name__ == "__main__":
    report_url = send_allure_results(
        allure_results_directory='../report/allure-results/table-metrics-compare',
        allure_server='http://10.156.72.217:28278',
        project_id='table-metrics-compare',
        security_user='admin',
        security_password='abc123!',
        ssl_verification=True
    )

    print("The Allure report is available at:", report_url)
    
scenario.set_scenario_variables(allure_report_url=report_url)
```

Step custom python

```python
import time
import dataiku
from dataiku.scenario import Scenario

# The Scenario object is the main handle from which you initiate steps
scenario = Scenario()

my_param_value = scenario.get_trigger_params().get("requestId")

print("Variables: ", scenario.get_all_variables())

client = dataiku.api_client()
auth_info = client.get_auth_info(with_secrets=True)
print (auth_info["secrets"])

# Get current project
project = client.get_default_project()
print (project)
print("Project key:", project.project_key)
print("Project variables:", project.get_variables())
```

## verify\_hms\_availability

Define Variables step

```json
{
   "HMS_HOST": "10.156.72.217",
   "HMS_PORT": "9083"
}
```

Step Excute Python Test 

 * **Unit tests to run**: `python/verify_hms_availability/`
 * **Code env**: Select an environment
 * **Envrionment**: `test`
 * **Ignore failure**: `true`
 * **Log level**: `INFO`


Step custom python

```python
from dataiku.scenario import Scenario

# The Scenario object is the main handle from which you initiate steps
scenario = Scenario()

from allure_uploader import send_allure_results

if __name__ == "__main__":
    report_url = send_allure_results(
        allure_results_directory='../report/allure-results/verify-hms-availability',
        allure_server='http://10.156.72.217:28278',
        project_id='verify-hms-availability',
        security_user='admin',
        security_password='abc123!',
        ssl_verification=True
    )

    print("The Allure report is available at:", report_url)
    
scenario.set_scenario_variables(allure_report_url=report_url)
```

Step custom python

```python
import time
import dataiku
from dataiku.scenario import Scenario

# The Scenario object is the main handle from which you initiate steps
scenario = Scenario()

my_param_value = scenario.get_trigger_params().get("requestId")

print("Variables: ", scenario.get_all_variables())

client = dataiku.api_client()
auth_info = client.get_auth_info(with_secrets=True)
print (auth_info["secrets"])



# Get current project
project = client.get_default_project()
print (project)
print("Project key:", project.project_key)
print("Project variables:", project.get_variables())
```
