# author: xingyu ji.
import requests
import json
import os
import io
import sys
from pathlib import Path
import time

from bs4 import BeautifulSoup



def get_login_cookies(base_url, login_url, session):
    login_page = session.get(login_url)
    soup = BeautifulSoup(login_page.text, 'html.parser')
    token = soup.select_one('meta[name=csrf-token]').get('content')
    payload = {
      'login_ways': 'email',
      'user[source]': '',
      'user[login]': 'miracleplusbrain@gmail.com',
      'user[password]': 'MPB87654321',
      'commit': 'Login',
      'authenticity_token': token
    }

    headers = { 
      'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36', 
      'Content-Type': 'application/x-www-form-urlencoded',
      'Origin': base_url,
      'Referer': login_url,
    }
    response = session.post(login_url, headers=headers, data=payload)
    # to save cookies:  # get some cookies
    cookies = requests.utils.dict_from_cookiejar(session.cookies)  # turn cookiejar into dict
    return cookies

def get_all_texts(url, session, cookies):

    # send a GET request
    response = session.get(url)
    
    # check for successful request
    if response.status_code != 200:
        print("Failed to get the page:", response.status_code)
        return []
    
    # parse the page's content
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # get all the text from the parsed HTML
    text = soup.get_text()
    
    return text

def json_str_to_list_ids(json_str):
    json_item = json.loads(json_str)
    ret = {}
    for group_dict in json_item['options']:
        ret[group_dict['id']] = group_dict['name']
    return ret

def download_contact_csv_file(contacts_url, cookies, group_id, batch_id, file_name):
    # The contacts URL you want to download from
    url = contacts_url
    # The parameters for the GET request
    params = {
        'group_id': group_id,
        'batch_id': batch_id
    }
    cookies = cookies
    # The headers for the request
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Referer': 'https://apply.miracleplus.com/admin/contacts?filters=0&page=1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
    }
    # Send the GET request
    response = requests.get(url, headers=headers, params=params, cookies=cookies)
    # Check if the request was successful
    if response.status_code == 200:
        # If successful, write the content to a file (replace 'file_path' with the path where you want to save the file)
        with open('人脉数据/' + str(group_id) + f' {file_name}' + '.csv', 'wb') as file:
            file.write(response.content)
        print('downloaded group', group_id)
    else:
        print(f"encountered error when downloading id {group_id}!")
        return

def download_all_contact(group_info_url, contacts_download, batch_id, session, cookies):
    pwd = os.getcwd()
    for file in os.listdir(r'人脉数据'):
        os.remove(pwd + '\\人脉数据\\' + file)
    leaders_info_str = get_all_texts(group_info_url, session, cookies)
    all_leaders_dict = json_str_to_list_ids(leaders_info_str)
    for key, value in all_leaders_dict.items():
        download_contact_csv_file(contacts_download, cookies, key, batch_id, value)
    
def export_contacts_match(exportation_contact_url, batch_id, session, cookies):
    exportation_contact_page = session.get(exportation_contact_url)
    exportation_contact_soup = BeautifulSoup(exportation_contact_page.text, 'html.parser')
    contact_match_token = exportation_contact_soup.select_one('meta[name=csrf-token]').get('content')

    contact_match_headers = {
      'accept': 'application/json, text/plain, */*',
      'accept-encoding': 'gzip, deflate, br',
      'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
      'referer': 'https://apply.miracleplus.com/evaluation/applications',
      'sec-ch-ua': '".Not/A)Brand";v="99", "Google Chrome";v="103", "Chromium";v="103"',
      'sec-ch-ua-mobile': '?0',
      'sec-ch-ua-platform': "Windows",
      'sec-fetch-dest': 'empty',
      'sec-fetch-mode': 'cors',
      'sec-fetch-site': 'same-origin',
      'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
      'x-csrf-token': contact_match_token
    }

    contact_params = {
        'batch_id': 10,
        'format': 'json',
        'page': 1
    }
    export_contacts_response = session.get(f'{exportation_contact_url}/export_with_contacts?format=json&batch={batch_id}&page=1', headers=contact_match_headers, params=contact_params, cookies=cookies)
    print("contacts match csv export status: ", export_contacts_response.status_code)
    
def export_application(exportation_application_url, base_url, session, cookies):
    exportation_apply_page= session.get(exportation_application_url)
    exportation_apply_soup = BeautifulSoup(exportation_apply_page.text, 'html.parser')
    exportation_apply_token = exportation_apply_soup.select_one('meta[name=csrf-token]').get('content')

    url = f"{exportation_application_url}?q=15101520627&count=6"

    exporation_apply_headers = { 
      'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36', 
      'Content-Type': 'application/x-www-form-urlencoded',
      'Origin': base_url,
      'Referer': f"{base_url}/evaluation/applications/exportation",
    }


    apply_keys_payload = {
      'columns': json.dumps(["apply.applications.url", "birthday", "created_at","submitted_at","apply.questions.names.application_introducer","apply.questions.names.main_founder_career_stage","apply.questions.names.product_introduction","apply.questions.names.product_name", "apply.founder_questions.names.founder_education", "apply.founder_questions.names.founder_technical_background", "apply.founder_questions.names.founder_work_experience"]),
      'queries': json.dumps({'batch': '10', 'status': 'submitted'}),
      'authenticity_token': exportation_apply_token
    }
    exportation_apply_response = session.post(url, headers=exporation_apply_headers, data=apply_keys_payload, cookies=cookies)
    print("applications csv export status: ", exportation_apply_response.status_code)
    
def get_export_download_links(url, base_url, session, cookies):

    response = session.get(url=url, cookies=cookies)
    
    # check for successful request
    if response.status_code != 200:
        print("Failed to get the page:", response.status_code)
        return []
    
    # parse the page's content
    soup = BeautifulSoup(response.content, 'html.parser')

    # find the table
    table = soup.find('table', class_='table')
    if table is None:
        print("Failed to find the table")
        return []
    
    # get all rows
    rows = table.find_all('tr')
    try:
        # extract the second and third row
        row_contact_export = rows[2]
        row_application_export = rows[1]

        link_contact = base_url + row_contact_export.find('a').get('href')
        link_application = base_url + row_application_export.find('a').get('href')        
    except AttributeError:
        return None, None
    return link_contact, link_application

def wait_for_export_links(url, base_url, session, cookies):
    link_contact, link_application = get_export_download_links(url, base_url, session, cookies)
    while link_contact is None or link_application is None:
        time.sleep(60)
        link_contact, link_application = get_export_download_links(url, base_url, session, cookies)
    return link_contact, link_application

def get_download_link(url, session, cookies):
    json_item = json.loads(get_all_texts(url, session, cookies))
    return json_item['download_link']
    
def download_from_link(url, session, cookies, file_path):
    response = session.get(url, cookies=cookies)
    # Check if the request was successful
    if response.status_code == 200:
        # If successful, write the content to a file (replace 'file_path' with the path where you want to save the file)
        with open(file_path, 'wb') as file:
            file.write(response.content)
        print('downloaded ', url)
    else:
        print(f"encountered error when downloading id {url}!")
        return

def full_file_download_sequence(
        batch_id, 
        base_url, 
        group_info_url, 
        login_url,
        contacts_download,
        exportation_url, 
        exportation_contact_url,
        exportation_application_url,
        history_export_url
    ):
    full_download_sessions = requests.Session() # start session
    cookies = get_login_cookies(base_url, login_url, full_download_sessions) # get cookies
    export_contacts_match(exportation_contact_url, batch_id, full_download_sessions, cookies)
    time.sleep(50)
    export_application(exportation_application_url, base_url, full_download_sessions, cookies)
    download_all_contact(group_info_url, contacts_download, batch_id, full_download_sessions, cookies)
    link_contacts, link_apps = wait_for_export_links(history_export_url, base_url, full_download_sessions, cookies)
    contacts_export_download_link = get_download_link(link_contacts, full_download_sessions, cookies)
    application_export_download_link = get_download_link(link_apps, full_download_sessions, cookies)
    download_from_link(contacts_export_download_link, full_download_sessions, cookies, "每日数据/F23RM_export.csv")
    download_from_link(application_export_download_link, full_download_sessions, cookies, "每日数据/F23ZDY.csv")
