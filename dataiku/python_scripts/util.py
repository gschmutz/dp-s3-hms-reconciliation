import logging
import os
import re
import boto3
import hashlib
import pandas as pd
import io
from datetime import datetime, timezone
from urllib.parse import urlparse

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# will only be used when running inside a scenario in Dataiku
try:
    from dataiku.scenario import Scenario
    # This will only succeed if running inside DSS
    scenario = Scenario()
except ImportError:
    logger.info("Unable to setup dataiku scenario API due to import error")    
    scenario = None
 
# will only be used when running inside a scenario in Dataiku
try:
    import dataiku
    # This will only succeed if running inside DSS
    client = dataiku.api_client()
except ImportError:
    logger.info("Unable to setup dataiku client API due to import error")
    client = None
 
def get_param(name, default=None, upper=False) -> str:
    """
    Retrieves the value of a parameter by name from the scenario variables if available,
    otherwise from the environment variables.
 
    Args:
        name (str): The name of the parameter to retrieve.
        default (Any, optional): The default value to return if the parameter is not found. Defaults to None.
 
    Returns:
        Any: The value of the parameter if found, otherwise the default value.
    """
    return_value = default
    if scenario is not None:
        return_value = scenario.get_all_variables().get(name, default)
    else:
        return_value = os.getenv(name, default)

    logger.info(f"{name}: {return_value}")

    if upper:
        return return_value.upper()
    else:
        return return_value

def get_credential(name, default=None) -> str:
    """
    Retrieves the value of a secret credential by its name.
    Args:
        name (str): The key name of the credential to retrieve.
        default (str, optional): The default value to return if the credential is not found. Defaults to None.
    Returns:
        str: The value of the credential if found, otherwise the default value.
    """
    return_value = default
    if client is not None:
        secrets = client.get_auth_info(with_secrets=True)["secrets"]
        for secret in secrets:
            if secret["key"] == name:
                if "value" in secret:
                    return_value = secret["value"]
                else:
                    break
    else:
        return_value = os.getenv(name, default)
    logger.info(f"{name}: *****")
         
    return return_value

def get_zone_name(upper=False) -> str:
    """
    Retrieves the zone name from the Dataiku global variable.
    """
    return_value = "unknown"
    if scenario is not None:
        env_var = scenario.get_all_variables().get("env")
        if env_var == "des":
            return_value = "sz"
        elif env_var == "des_pz":
            return_value = "pz"
    else:
        return_value = os.getenv("DATAIKU_ENV", "unknown")

    logger.info(f"Zone: {return_value}")

    if upper:
        return return_value.upper()
    else:
        return return_value

def get_run_id() -> str:
    """
    Retrieves the run ID from the Dataiku scenario if available, otherwise generates a new UUID.
    Returns:
        str: The run ID.
    """
    return_value = None
    if scenario is not None:
        return_value = scenario.get_all_variables().get("scenarioTriggerRunId")
    if not return_value:
        import uuid
        return_value = str(uuid.uuid4())
    logger.info(f"Run ID: {return_value}")
    return return_value

def replace_vars_in_string(s, variables):
    print(f"Replacing variables in string: {s} with {variables}")
    # Replace {var} with value from variables dict
    return re.sub(r"\{(\w+)\}", lambda m: str(variables.get(m.group(1), m.group(0))), s)        

def get_s3_location_list(s3: boto3.client, s3_admin_bucket: str, s3_location_list_object_name: str, batch: str, stage: str) -> pd.DataFrame:
    """
    Retrieves a list of S3 locations from a CSV file stored in an S3 bucket and returns it as a pandas DataFrame.
    Parameters:
        s3 (boto3.client): An initialized boto3 S3 client.
        s3_admin_bucket (str): The name of the S3 bucket where the CSV file is stored.
        s3_location_list_object_name (str): The key (object name) of the CSV file in the S3 bucket.
        batch (str): The name of the batch to filter the locations by. If provided, only locations matching this batch are returned.
        stage (str): The name of the stage to filter the locations by. If provided, only locations matching this stage are returned.
    Returns:
        pd.DataFrame: A DataFrame containing the S3 location list, optionally filtered by the specified batch.
    Raises:
        ValueError: If the CSV file retrieved from S3 is empty.
    Notes:
        - The function expects the CSV file to be accessible via the global S3_ADMIN_BUCKET and S3_LOCATION_LIST_OBJECT_NAME.
        - The function assumes the existence of a global `s3` client and required imports (`io`, `pandas as pd`).
    """

    logger.info(f"Retrieving S3 location list from s3://{s3_admin_bucket}/{s3_location_list_object_name}")
    # Read the object
    response = s3.get_object(Bucket=s3_admin_bucket, Key=s3_location_list_object_name)

    # `response['Body'].read()` returns bytes, decode to string
    csv_string = response['Body'].read().decode('utf-8')

    # Debug: print the content
    logger.info(f"CSV content length: {len(csv_string)}")
    logger.info(f"First 100 chars: {csv_string[:100]}")

    # Add error handling
    if not csv_string.strip():
        raise ValueError("CSV file is empty")

    # Wrap the string in a StringIO buffer
    csv_buffer = io.StringIO(csv_string)

    s3_location_list = pd.read_csv(csv_buffer)
    if batch:
        s3_location_list = s3_location_list[s3_location_list["batch"] == int(batch)]
        logger.info(s3_location_list)
    if stage:    
        s3_location_list = s3_location_list[s3_location_list["stage"] == int(stage)]
        logger.info(s3_location_list)

    return s3_location_list

def is_hidden(path):
    """
    Determines if a given path is considered hidden.
    A path is considered hidden if any of its components start with an underscore (_) or a dot (.).
    Args:
        path (str): The path to check.

    Returns:
        bool: True if the path is hidden, False otherwise.
    """
    components = path.split("/")
    return any(comp.startswith(".") for comp in components[:-1])

def get_partition_info(s3, s3a_url):
    """
    Analyzes an S3 location to extract partition information.
    Converts an S3A URL to an S3 URL, lists objects under the specified prefix,
    and detects partition-style folder structures (e.g., col=value). Collects
    all unique partitions, determines the latest modification timestamp among
    objects, and generates a fingerprint of the partition set.
    Args:
        s3 (boto3.client): An initialized boto3 S3 client.
        s3a_url (str): The S3A URL pointing to the location to analyze.
    Returns:
        dict: A dictionary containing:
            - "s3_location" (str): The original S3A URL.
            - "partition_count" (int): Number of unique partitions detected.
            - "fingerprint" (str): SHA256 hash of the sorted partition list.
            - "timestamp" (int): Unix timestamp of the latest modification.
    """
    # Convert s3a:// to s3://
    s3_url = s3a_url.replace("s3a://", "s3://")
    parsed = urlparse(s3_url)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/") + "/"

    print(f"Analyzing S3 location: bucket={bucket}, prefix={prefix}")

    paginator = s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

    partitions = set()
    latest_ts = datetime(1970, 1, 1, tzinfo=timezone.utc)

    for page in page_iterator:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            #print("key: " + key)
            # Detect partition-style folder structure like col=value
            parts = key[len(prefix):].strip("/").split("/")
            partition_parts = [p for p in parts if "=" in p]
            if not is_hidden(key) and partition_parts:
                partitions.add("/".join(partition_parts))
            if obj["LastModified"] > latest_ts:
                logger.debug(f"Found new latest partition: {key} (last modified: {obj['LastModified']})")
                latest_ts = obj["LastModified"]                

    fingerprint = ""
    sorted_partitions = []
    if len(partitions) > 0:
        sorted_partitions = sorted(partitions)
        joined = ",".join(sorted_partitions)
        fingerprint = hashlib.sha256(joined.encode('utf-8')).hexdigest()
                      
    return {
        "s3_location": s3a_url,
        "partition_count": len(partitions),
        "partition_list": sorted_partitions,
        "fingerprint": fingerprint,
        "timestamp": int(latest_ts.timestamp())
    }