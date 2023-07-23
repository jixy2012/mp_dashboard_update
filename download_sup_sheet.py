# coding: utf-8


import requests
import json
import time
import sys
import io


def get_tat(app_id, app_token):
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    payload = json.dumps({
      "app_id": app_id,
      "app_secret": app_token
    })
    
    headers = {
      'Content-Type': 'application/json; charset=utf-8',
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    return json.loads(response.text)["tenant_access_token"]


def create_task(tat, token):
	url = "https://open.feishu.cn/open-apis/drive/v1/export_tasks"
	payload = json.dumps({
		"file_extension": "xlsx",
		"token": f"{token}",
		"type": "sheet"
	})


	headers = {
	  'Content-Type': 'application/json',
	  'Authorization': f'Bearer {tat}'
	}

	response = requests.request("POST", url, headers=headers, data=payload)
	response_json = json.loads(response.text)
	ticket = response_json['data']['ticket']
	if 'error' in response_json:
		print(response_json, flush=True)
		return
	return ticket

def check_task_status(tat, token, ticket):

	url = f"https://open.feishu.cn/open-apis/drive/v1/export_tasks/{ticket}?token={token}"
	payload = ''


	headers = {
	  'Authorization': f'Bearer {tat}'
	}

	response = requests.request("GET", url, headers=headers, data=payload)
	response_json = json.loads(response.text)

	if 'error' in response_json:
		print(response_json, flush=True)
		return
	return response_json['data']['result']['file_token']


def download_sheet(tat, file_token):

	url = f"https://open.feishu.cn/open-apis/drive/v1/export_tasks/file/{file_token}/download"
	payload = ''


	headers = {
	  'Authorization': f'Bearer {tat}'
	}

	response = requests.request("GET", url, headers=headers, data=payload)
	if response.status_code == 200:
		print("download superior form success", file=sys.stderr)
		with open(f'关联上级.xlsx', 'wb') as output_file:
			output_file.write(response.content)

	else:
		print('encountered error when downloading!!')


def full_dld_sheet_seq(tat, token):
	ticket = create_task(tat, token)
	time.sleep(5)
	file_token = check_task_status(tat, token, ticket)
	time.sleep(5)
	download_sheet(tat, file_token)

def sup_sheet_download(app_id, app_token, superior_sheet_token):
	tat = get_tat(app_id, app_token)
	full_dld_sheet_seq(tat, superior_sheet_token)