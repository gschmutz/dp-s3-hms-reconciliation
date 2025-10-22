import os
import requests
import json
import base64
import boto3
import uuid
from util import get_param, get_credential, get_zone_name, get_run_url, get_run_id, create_s3_client

def allure_login(allure_server, security_user, security_password, ssl_verification):
    print("------------------LOGIN-----------------")
    credentials_body = {
        "username": security_user,
        "password": security_password
    }
    json_credentials_body = json.dumps(credentials_body)
    headers = {'Content-type': 'application/json'}

    session = requests.Session()
    response = session.post(
        f"{allure_server}/allure-docker-service/login",
        headers=headers,
        data=json_credentials_body,
        verify=ssl_verification
    )

    print("STATUS CODE:")
    print(response.status_code)
    print("RESPONSE COOKIES:")
    print(json.dumps(session.cookies.get_dict(), indent=4, sort_keys=True))
    csrf_access_token = session.cookies.get('csrf_access_token')
    if not csrf_access_token:
        raise Exception("CSRF access token not found in cookies.")
    print("CSRF-ACCESS-TOKEN: " + csrf_access_token)
    access_token_cookie = session.cookies.get('access_token_cookie')
    if not access_token_cookie:
        raise Exception("Access cookie token not found in cookies.")
    print("ACCESS-COOKIE-TOKEN: " + access_token_cookie)
    
    return csrf_access_token, access_token_cookie

def upload_to_s3(s3_client, local_directory, bucket, s3_prefix=""):
    """
    Recursively upload a local directory to an S3 bucket.

    :param local_directory: Path to the local directory
    :param bucket: Target S3 bucket name
    :param s3_prefix: Optional prefix (S3 folder path) to upload into
    """
    current_directory = os.path.dirname(os.path.realpath(__file__))
    absolute_directory = os.path.join(current_directory, local_directory)

    print(f"Uploading contents of {absolute_directory} to s3://{bucket}/{s3_prefix}")

    for root, dirs, files in os.walk(absolute_directory):
        for filename in files:
            local_path = os.path.join(root, filename)
            # Compute the relative path from the absolute_directory
            relative_path = os.path.relpath(local_path, absolute_directory)
            # Join s3_prefix and relative_path to preserve folder structure
            s3_path = os.path.join(s3_prefix, relative_path).replace("\\", "/")
            
            print(f"Uploading {local_path} → s3://{bucket}/{s3_path}")
            
            s3_client.upload_file(local_path, bucket, s3_path)

def download_from_s3(s3_client, bucket, s3_prefix, local_directory):
    """
    Recursively download a directory from an S3 bucket to a local directory.

    :param bucket: Source S3 bucket name
    :param s3_prefix: S3 folder path to download
    :param local_directory: Path to the local directory
    """
    current_directory = os.path.dirname(os.path.realpath(__file__))
    absolute_directory = os.path.join(current_directory, local_directory)

    print(f"Downloading contents of s3://{bucket}/{s3_prefix} to {absolute_directory}")

    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=s3_prefix):
        for obj in page.get('Contents', []):
            s3_path = obj['Key']
            relative_path = os.path.relpath(s3_path, s3_prefix)
            local_path = os.path.join(absolute_directory, relative_path)
            local_dir = os.path.dirname(local_path)
            if not os.path.exists(local_dir):
                os.makedirs(local_dir)
            
            print(f"Downloading s3://{bucket}/{s3_path} → {local_path}")
            
            s3_client.download_file(bucket, s3_path, local_path)

def upload_to_allure_server(
    local_directory,
    allure_server,
    project_id,
    security_user,
    security_password,
    create_project=False,
    ssl_verification=True,
    execution_name='execution from my script',
    execution_from='http://google.com',
    execution_type='teamcity'
):
    current_directory = os.path.dirname(os.path.realpath(__file__))
    results_directory = os.path.join(current_directory, local_directory)
    print('RESULTS DIRECTORY PATH: ' + results_directory)

    print("------------------LOGIN-----------------")
    credentials_body = {
        "username": security_user,
        "password": security_password
    }
    json_credentials_body = json.dumps(credentials_body)
    headers = {'Content-type': 'application/json'}

    session = requests.Session()
    response = session.post(
        f"{allure_server}/allure-docker-service/login",
        headers=headers,
        data=json_credentials_body,
        verify=ssl_verification
    )

    print("STATUS CODE:")
    print(response.status_code)
    print("RESPONSE COOKIES:")
    print(json.dumps(session.cookies.get_dict(), indent=4, sort_keys=True))
    csrf_access_token = session.cookies.get('csrf_access_token')
    if not csrf_access_token:
        raise Exception("CSRF access token not found in cookies.")
    print("CSRF-ACCESS-TOKEN: " + csrf_access_token)

    if create_project:
        print("------------------CREATE-PROJECT------------------")
        create_project_body = {
            "id": project_id
        }
        json_request_body = json.dumps(create_project_body)
        headers['X-CSRF-TOKEN'] = csrf_access_token
        response = session.post(
            f"{allure_server}/allure-docker-service/projects",
            headers=headers,
            data=json_request_body,
            verify=ssl_verification
        )

    print("------------------SEND-RESULTS------------------")
    files = os.listdir(results_directory)
    print('FILES:')
    results = []
    # Paginate files in blocks of 50
    block_size = 50
    for i in range(0, len(files), block_size):
        block_files = files[i:i + block_size]
        results = []
        for file in block_files:
            result = {}
            file_path = os.path.join(results_directory, file)
            print(file_path)
            if os.path.isfile(file_path):
                with open(file_path, "rb") as f:
                    content = f.read()
                    if content.strip():
                        b64_content = base64.b64encode(content)
                        result['file_name'] = file
                        result['content_base64'] = b64_content.decode('UTF-8')
                        results.append(result)
                    else:
                        print('Empty File skipped: ' + file_path)
            else:
                print('Directory skipped: ' + file_path)

        headers = {'Content-type': 'application/json'}
        request_body = {"results": results}
        json_request_body = json.dumps(request_body)

        headers['X-CSRF-TOKEN'] = csrf_access_token
        response = session.post(
            f"{allure_server}/allure-docker-service/send-results?project_id={project_id}",
            headers=headers,
            data=json_request_body,
            verify=ssl_verification
        )
        print("STATUS CODE:")
        print(response.status_code)
        print("RESPONSE:")
        print(response.content)
        json_response_body = json.loads(response.content)
        print(json.dumps(json_response_body, indent=4, sort_keys=True))

    print("------------------GENERATE-REPORT------------------")
    response = session.get(
        f"{allure_server}/allure-docker-service/generate-report?project_id={project_id}&execution_name={execution_name}&execution_from={execution_from}&execution_type={execution_type}",
        headers=headers,
        verify=ssl_verification
    )
    print("STATUS CODE:")
    print(response.status_code)
    print("RESPONSE:")
    json_response_body = json.loads(response.content)
    print(json.dumps(json_response_body, indent=4, sort_keys=True))

    print('ALLURE REPORT URL:')
    print(json_response_body['data']['report_url'])
    return json_response_body['data']['report_url']

def send_reports(
    report_directory,
    allure_server,
    project_id,
    run_id,
    security_user,
    security_password,
    create_project=False,
    ssl_verification=True,
    upload_to_s3_enabled=False,
    upload_to_s3_client=None,
    upload_to_s3_bucket="",
    upload_to_s3_prefix="",
    execution_name='execution from my script',
    execution_from='http://google.com',
    execution_type='teamcity'
):
    allure_results_directory = os.path.join(report_directory, "allure-results").replace("\\", "/")

    report_url = upload_to_allure_server(
        local_directory=allure_results_directory,
        allure_server=allure_server,
        project_id=project_id,
        security_user=security_user,
        security_password=security_password,
        create_project=create_project,
        ssl_verification=ssl_verification,
        execution_name=execution_name,
        execution_from=execution_from,
        execution_type=execution_type
    )

    if upload_to_s3_enabled and upload_to_s3_bucket:
        print("------------------UPLOAD-TO-S3------------------")

        upload_to_s3(upload_to_s3_client, report_directory, upload_to_s3_bucket, s3_prefix=upload_to_s3_prefix)

    return report_url    

def upload_reports(project_basename, report_directory = None):

    # Environment variables
    ALLURE_SERVER = get_param('ALLURE_REPORT_SERVER_URL', '')
    ALLURE_USER = get_credential('ALLURE_REPORT_USER', 'admin')
    ALLURE_PASSWORD = get_credential('ALLURE_REPORT_PASSWORD', 'admin')
    ALLURE_CREATE_PROJECT_ENABLED = get_param('ALLURE_CREATE_PROJECT_ENABLED', 'false').lower() in ('true', '1', 't')
    ALLURE_SSL_VERIFICATION = get_param('ALLURE_SSL_VERIFICATION', 'false').lower() in ('true', '1', 't')
    UPLOAD_TO_S3_ENABLED = get_param('ALLURE_UPLOAD_TO_S3_ENABLED', 'false').lower() in ('true', '1', 't')
    UPLOAD_S3_ENDPOINT_URL = get_param('S3_ENDPOINT_URL', 'http://localhost:9000')
    UPLOAD_TO_S3_BUCKET = get_param('S3_ADMIN_BUCKET', '')
    AWS_ACCESS_KEY = get_credential('AWS_ACCESS_KEY', '')
    AWS_SECRET_ACCESS_KEY = get_credential('AWS_SECRET_ACCESS_KEY', '')

    zone = get_zone_name()
    run_id = get_run_id()
    run_url = get_run_url()
    project_id = f"{zone.lower()}-{project_basename}"


    if report_directory is None:
        report_directory = f"../../../../pytest-step-execute-tests/project-python-libs/GDP_RECONCILIATION_JOBS/report"

    allure_results_directory = os.path.join(report_directory, "allure-results").replace("\\", "/")

    report_url = upload_to_allure_server(
        local_directory=allure_results_directory,
        allure_server=ALLURE_SERVER,
        project_id=project_id,
        security_user=ALLURE_USER,
        security_password=ALLURE_PASSWORD,
        create_project=ALLURE_CREATE_PROJECT_ENABLED,
        ssl_verification=ALLURE_SSL_VERIFICATION,
        execution_from=run_url
    )

    if UPLOAD_TO_S3_ENABLED and UPLOAD_TO_S3_BUCKET:
        print("------------------UPLOAD-TO-S3------------------")

        s3_client = create_s3_client(aws_access_key=AWS_ACCESS_KEY, aws_secret_key=AWS_SECRET_ACCESS_KEY, endpoint_url=UPLOAD_S3_ENDPOINT_URL, verify_ssl=False)
        s3_prefix = f"test/{zone.lower()}/report/{project_id}/{run_id}"
        upload_to_s3(s3_client, report_directory, UPLOAD_TO_S3_BUCKET, s3_prefix=s3_prefix)

    return report_url 

def export_emailable_report(project_basename):

    ALLURE_SERVER = get_param('ALLURE_REPORT_SERVER_URL', '')
    ALLURE_USER = get_credential('ALLURE_REPORT_USER', 'admin')
    ALLURE_PASSWORD = get_credential('ALLURE_REPORT_PASSWORD', 'admin')
    ALLURE_SSL_VERIFICATION = get_param('ALLURE_SSL_VERIFICATION', 'false').lower() in ('true', '1', 't')

    zone = get_zone_name()

    csrf_access_token, access_token_cookie = allure_login(ALLURE_SERVER, ALLURE_USER, ALLURE_PASSWORD, ALLURE_SSL_VERIFICATION)

    project_id = f"{zone.lower()}-{project_basename}"
    url = f"{ALLURE_SERVER}/allure-docker-service/emailable-report/export?project_id={project_id}"
    headers = { 
        'accept': '*/*',
        'X-CRSF-TOKEN': csrf_access_token
    }
    cookies = {
        'access_token_cookie': access_token_cookie
    }
    
    print("------------------GET ALLURE EMAILABLE REPORT------------------")
    session = requests.Session()
    response = session.get(
        url,
        headers=headers,
        cookies=cookies,
        verify=ALLURE_SSL_VERIFICATION
    )
    print("STATUS CODE:")
    print(response.status_code)
    folder=dataiku.Folder("zadpu87O")
    filename='allure-report.html'
    with folder.get_writer(filename) as writer:
        writer.write(response.text.encode("utf-8"))
