import os
import requests
import json
import base64

def send_allure_results(
    allure_results_directory,
    allure_server,
    project_id,
    create_project=False,
    security_user,
    security_password,
    ssl_verification=True,
    execution_name='execution from my script',
    execution_from='http://google.com',
    execution_type='teamcity'
):
    current_directory = os.path.dirname(os.path.realpath(__file__))
    results_directory = os.path.join(current_directory, allure_results_directory)
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