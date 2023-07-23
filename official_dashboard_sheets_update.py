# -*- coding: utf-8 -*-
# author: xingyu ji.



import requests
import json
import time
import pandas as pd
import numpy as np
import os
import time
import sys
import io
from datetime import datetime, timedelta, date




def get_tat(app_id, app_token):
    """
    get tenant access token of the bot. this access token is crucial for accessing any of the sheets. it refreshes every couple of hours,
    so you need to get the new one before you do any updating. All you need is the app_id and app_token, which shouldn't change across the 
    seasons and can be found in full_exec_seq.py.
    """


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
# sheet_token is for the entire sheet file, and the sheet_id is for each individual subsheet.



def add_row_to_sheet(tat, sheet_token, sheet_id, start_idx, end_idx):
    """
    utility function that adds rows to a sheet. you need to specify which sheet and the start and end indices. I think the 
    end index is just gonna depend on how many rows you want to add to the sheet. try it out, so far i haven't used it.
    """
    url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{sheet_token}/insert_dimension_range"
    payload = json.dumps({
        "dimension":{
            "sheetId": f"{sheet_id}",
            "majorDimension": "ROWS",
            "startIndex": start_idx,
            'endIndex': end_idx
        },
        "inheritStyle": "BEFORE"
    })
    
    headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'Authorization': f"Bearer {tat}"
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    if 'error' in json.loads(response.text):
        print('error when adding row to sheet!', flush=True)


def add_vals_to_sheet(tat, sheet_range, sheet_token, sheet_id, values):
    """
    utility function that copies values to a sheet
    given a sheet, a range, and the values. the values should be a list of lists. each list is one row in the sheet.
    """
    url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{sheet_token}/values"
    payload = json.dumps({
        "valueRange":{
            "range": f"{sheet_id}!{sheet_range}",
            "values": values
        }
    })
    
    headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'Authorization': f"Bearer {tat}"
    }

    response = requests.request("PUT", url, headers=headers, data=payload)
    response_result = json.loads(response.text)
    
    if "error" in response_result:
        print("error when adding values to sheet!", flush=True)
        print(sheet_token, flush=True)
        
def unmerge_cells(tat, sheet_token, sheet_id, sheet_range):
    """
    utility function that merge cells.
    give a sheet and a range, unmerge all the cells in the range. recommend to use with add_vals_to_sheet only with the same range input.
    some cells in the sheets you don't want to unmerged, but the ones you update are fine to unmerge.
    """
    url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{sheet_token}/unmerge_cells"
    temp_lst = sheet_range.split(":")
    new_range = temp_lst[0] + ":" + temp_lst[1][0] + str(int(temp_lst[1][1:]) - 1)
    payload = json.dumps({
        "range": f"{sheet_id}!{new_range}"
    })
    
    headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'Authorization': f"Bearer {tat}"
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    response_result = json.loads(response.text)
    
    if "error" in response_result:
        print("encountered error when unmerging cells!!", flush=True)
        print(sheet_token, flush=True)
        
def get_last_row_col(tat, sheet_token, sheet_id):
    """
    utility function to get the last row of the entire sheet.
    """
    url = f"https://open.feishu.cn/open-apis/sheets/v3/spreadsheets/{sheet_token}/sheets/{sheet_id}"
    payload = json.dumps({
        "spreadsheet_token": f"{sheet_token}",
        "sheet_id": f"{sheet_id}"
    })
    
    headers = {
        'Authorization': f"Bearer {tat}"
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    response_result = json.loads(response.text)
    
    if "error" in response_result:
        print("encountered error when getting bottom row!!", flush=True)
        print(sheet_token, flush=True)
    return response_result["data"]["sheet"]["grid_properties"]["row_count"]

def end_of_sheet_top_left(left_col, tat, sheet_token, sheet_id):
    """
    utility function used only for repeated contacts sheet. just returns the top left cell of the appended rows.
    """
    return left_col + str(get_last_row_col(tat, sheet_token, sheet_id) + 1)


def is_datetime(potential_date_string, date_format="%Y-%m-%d"):
    """
    utility function that checks if something looks like a date.
    """
    try :
        cur_time = datetime.strptime(potential_date_string, date_format)
        return cur_time
    except (ValueError, TypeError):
        return False
    
def df_to_list_of_lists(df, date_for_sheet=True):
    """
    utility function for turning a dataframe to a list of lists. default setting is that it will convert date to feishu dates.
    how feishu does dates is that it calculates the number of days from 1899. 12. 30 to the day give. weird date to pick.
    """
    feishu_start_date = datetime(1899, 12, 30)
    res = []
    for i in range(len(df)):
        cur_row = []
        for elem in df.iloc[i].tolist():
            if type(elem) == np.int64:
                elem = int(elem)
            elif (type(elem) == np.float64 or type(elem) == float) and np.isnan(elem):
                elem = ""
            elif isinstance(elem, str) and is_datetime(elem) and date_for_sheet:
                elem = (is_datetime(elem) - feishu_start_date).days
            cur_row.append(elem)
        res.append(cur_row)
    return res

def get_range(top_left_cell, values):
    """
    utility function that gets the range of the update.
    """
    return top_left_cell + ":" + chr(ord(top_left_cell[0]) + len(values[0]) - 1) + str(int(top_left_cell[1:]) + len(values))

def update_values_only_one_tab(df, tat, sheet_token, sheet_id, top_left_cell, date_for_sheet=True):
    """
    one of the real updater functions that you may use. 
    @param df: the dataframe corresponding to the sheet you want to update. this get processed first to be a list of lists.
    @param tat: tenant access code you got from eariler.
    @param sheet_token: sheet token for the sheet you want to update.
    @param sheet_id: sheet id for the sub-sheet you want to update.
    @param top_left_cell: top left cell in the sheet you want the update value to go into.
    @param date_for_sheet: whether you need feishu format date for the datetime values.
    """
    values = df_to_list_of_lists(df, date_for_sheet)
    while len(values) > 4950:
        cur_val = values[:4950]
        values = values[4950:]
        cur_range = get_range(top_left_cell, cur_val)
        top_left_cell = top_left_cell[0] + cur_range.split(":")[1][1:]
        print(cur_range)
        add_vals_to_sheet(tat, cur_range, sheet_token, sheet_id, cur_val)
        unmerge_cells(tat, sheet_token, sheet_id, cur_range)
        time.sleep(0.1)
    sheet_range = get_range(top_left_cell, values)
    print(sheet_range)
    add_vals_to_sheet(tat, sheet_range, sheet_token, sheet_id, values)
    unmerge_cells(tat, sheet_token, sheet_id, sheet_range)



def day_of_week_to_range(start_cell):
    """
    utility function for updating the sum range according to the day of week it is.
    """
    wd = datetime.weekday(date.today() + timedelta(days=-1))
    this_week = start_cell + ":" + start_cell[0] + str(int(start_cell[1:]) + wd)
    last_week = start_cell[0] + str(int(start_cell[1:]) + wd + 1) + ":" + start_cell[0] + str(int(start_cell[1:]) + wd + 7)
    if wd == 6:
        return "0", start_cell + ":" + start_cell[0] + str(int(start_cell[1:]) + wd)
    return this_week, last_week

def update_sum_ranges(tat, sheet_token, sheet_id, this_week_cell, last_week_cell, top_left_cell, length):
    """
    update the sum ranges.
    another function that you will probably be calling directly. usually there's a sum for last week and a sum for thise week. 
    @param tat: tenant access code
    @param sheet_token: sheet token of the sheet you want to update
    @param sheet_id: sheet id of the subsheet you want to update
    @param this_week_cell: the cell for the sum of this week
    @param top_week_cell: the cell for the sum of last week
    @param top_left_cell: the top left cell where the data lives
    @param length: number of sum cells
    """
    values_this_week = []
    values_last_week = []
    for i in range(length):
        cur_this_week, cur_last_week = day_of_week_to_range(chr(ord(top_left_cell[0]) + i) + str(top_left_cell[1:]))
        values_this_week.append({"type": "formula", "text": "=SUM(" + cur_this_week + ")"})
        values_last_week.append({"type": "formula", "text": "=SUM(" + cur_last_week + ")"})

    add_vals_to_sheet(tat, get_range(this_week_cell, [values_this_week]), sheet_token, sheet_id, [values_this_week])
    add_vals_to_sheet(tat, get_range(last_week_cell, [values_last_week]), sheet_token, sheet_id, [values_last_week])

def set_date_format(tat, sheet_token, sheet_id, top_left):
    """
    set the date format for column that have date values.
    @param tat: tenant access token
    @param sheet_token: sheet token for the sheet
    @param sheet_id: sheet id for the subsheet
    @param top_left: top left cell of the column you want to set date format for.
    """
    last_row = get_last_row_col(tat, sheet_token, sheet_id)
    
    tail_case = last_row % 5000
    new_range = top_left + ':' + top_left[0] + str(tail_case)
    url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{sheet_token}/style"
    payload = json.dumps({
        "appendStyle":{
            "range": f"{sheet_id}!{new_range}",
            "style": {
                'formatter': 'yyyy-MM-dd'
            }
        }
    })
    
    headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'Authorization': f"Bearer {tat}"
    }
    response = requests.request("PUT", url, headers=headers, data=payload)
    response_result = json.loads(response.text)
    cur_row = tail_case
    last_top = top_left[0] + str(tail_case + 1)
    if "error" in response_result:
        print("encountered error when setting date format!!", flush=True)
        print(sheet_token, flush=True)
    while cur_row < last_row:
        cur_row += 5000
        new_range = last_top + ':' + top_left[0] + str(cur_row)
        last_top = top_left[0] + str(cur_row + 1)
        url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{sheet_token}/style"
        payload = json.dumps({
            "appendStyle":{
                "range": f"{sheet_id}!{new_range}",
                "style": {
                    'formatter': 'yyyy-MM-dd'
                }
            }
        })

        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': f"Bearer {tat}"
        }
        response = requests.request("PUT", url, headers=headers, data=payload)
        if "error" in json.loads(response.text):
            print("encountered error when setting date format!!", flush=True)
            print(sheet_token, flush=True)

def bot_send_messages(tat, repeated_contact_sheet_token, chat_id):
    """
    bot send message for repeated contact sheet.
    @param tat: tenant access token
    @param repeated_contact_sheet_token: sheet token for the repeated contacts since it's the only message we send
    @param chat_id: id of the brain group chat
    """
    url = "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id"
    link = f"https://miracleplus.feishu.cn/sheets/{repeated_contact_sheet_token}"
    content_txt = "{\"text\":\"" + link + "\"}"
    payload = json.dumps({
        "content": content_txt,
        "msg_type": "text",
        "receive_id": f"{chat_id}"
    })


    headers = {
      'Content-Type': 'application/json',
      'Authorization': f'Bearer {tat}'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    if "error" in json.loads(response.text):
        print("encountered error when sending message!!", flush=True)
        print(repeated_contact_sheet_token, flush=True)


# 几个多维表格的函数

def get_db_records(tat, grid_token, table_token):
    """
    utility function that gets all the records in a multi-dim sheet like the dashboard or the hr db.
    """
    full_record_id = []
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{grid_token}/tables/{table_token}/records"
    payload = ""
    
    headers = {
        'Authorization': f"Bearer {tat}"
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    response_result = json.loads(response.text)
    
    if "error" in response_result:
        print("encountered error when getting records!", flush=True)
        print(grid_token, flush=True)
    
    if 'items'not in response_result['data']:
        return []
    
    for record in response_result["data"]['items']:
        full_record_id.append(record['id'])
    
    while response_result['data']['has_more'] == True:
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{grid_token}/tables/{table_token}/records?page_size=500&page_token={response_result['data']['page_token']}"
        payload = ""
        response = requests.request("GET", url, headers=headers, data=payload)
        response_result = json.loads(response.text)
        if "error" in response_result:
            print("encountered error when getting records!", flush=True)
            print(grid_token, flush=True)
        for record in response_result["data"]['items']:
            full_record_id.append(record['id'])
    time.sleep(0.05)

    return full_record_id

def del_db_records(tat, grid_token, table_token, records_lst):

    """
    delete all records in a multi-dim sheet.
    """
    if not records_lst:
        return
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{grid_token}/tables/{table_token}/records/batch_delete"
    
    headers = {
        'Authorization': f"Bearer {tat}",
        "Content-Type": "application/json; charset=utf-8"
    }

    tail_case_len = len(records_lst) % 500
    
    payload = json.dumps({
        "records": records_lst[:tail_case_len]
    })
    records_lst = records_lst[tail_case_len:]
    
    response = requests.request("POST", url, headers=headers, data=payload)
    response_result = json.loads(response.text)
    
    if "error" in response_result:
        print("encountered error when deleting records!", flush=True)
        print(grid_token, flush=True)
    while records_lst:
        payload = json.dumps({
            "records": records_lst[:500]
        })
        records_lst = records_lst[500:]
        response = requests.request("POST", url, headers=headers, data=payload)
        response_result = json.loads(response.text)
        if "error" in response_result:
            print("encountered error when deleting records!", flush=True)
            print(response_result)
            print(grid_token, flush=True)
        time.sleep(0.05)


def get_fields_name_types(tat, grid_token, table_token):
    """
    get the type of data contained in each of the column in a multi-dim sheet.
    """
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{grid_token}/tables/{table_token}/fields?page_size=50"
    payload = ''


    headers = {
      'Authorization': f'Bearer {tat}'
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    response_primitive_dict = json.loads(response.text)['data']['items']
    fields_dict = {}
    fields_names = []
    for field in response_primitive_dict:
        fields_dict[field["field_name"]] = field["ui_type"]
        fields_names.append(field["field_name"])
    return fields_dict, fields_names

def values_to_records(values, fields, fields_dict):
    """
    convert values from a list of lists to records for a multi-dim sheet.
    """
    res = []
    for lst in values:
        cur_dict = {}
        for i in range(len(lst)):
            item = lst[i]
            needed_type = fields_dict[fields[i]]
            if needed_type == 'DateTime':
                if isinstance(item, pd._libs.tslibs.timestamps.Timestamp):
                    item = item.value // 10**6
                elif isinstance(item, str):
                    item = datetime.strptime(item, "%Y-%m-%d").timestamp() * 10**3

            elif needed_type == 'Url':
                item = {
                    'text': item,
                    'link': item
                }                
            cur_dict[fields[i]] = item
        new_dict = {}
        new_dict["fields"] = cur_dict
        res.append(new_dict)
    return res
        
def add_db_records(tat, grid_token, table_token, df):

    """
    add records to a multi-dim sheet. need a dataframe as source of data.
    """

    values = df_to_list_of_lists(df, date_for_sheet=False)
    fields_dict, fields_name = get_fields_name_types(tat, grid_token, table_token)
    records_values = values_to_records(values, fields_name, fields_dict)
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{grid_token}/tables/{table_token}/records/batch_create"
    headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'Authorization': f"Bearer {tat}"
    }
    
    tail_case_len = len(records_values) % 500

    
    payload = json.dumps({
        "records": records_values[:tail_case_len]
    })
    records_values = records_values[tail_case_len:]

    response = requests.request("POST", url, headers=headers, data=payload)
    response_result = json.loads(response.text)
    if "error" in response_result:
        print("encountered error when adding records!", flush=True)
        print(grid_token, flush=True)
        
    while records_values:
        payload = json.dumps({
            "records": records_values[:500]
        })
        records_values = records_values[500:]
        response = requests.request("POST", url, headers=headers, data=payload)
        response_result = json.loads(response.text)
        if "error" in response_result:
            print("encountered error when adding records!", flush=True)
            print(grid_token, flush=True)
        time.sleep(0.05)

def update_one_tab_in_db(tat, grid_token, table_token, df):
    """
    another function you will use mainly for updating multi-dim sheets like dashboard1.0 or hr db.
    @param tat: tenant access token
    @param grid_token: grid token for the multi-dim sheet
    @param table_token: subtable token
    @param df: dataframe that the data come from.
    """
    records_lst = get_db_records(tat, grid_token, table_token)
    del_db_records(tat, grid_token, table_token, records_lst)
    add_db_records(tat, grid_token, table_token, df)




def get_db_full_records(tat, grid_token, table_token, view_id = ""):
    """
    get all the records in a multi-dim sheet.
    """
    full_record_id = []
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{grid_token}/tables/{table_token}/records"
    if not view_id:
        payload = ""
    else:
        payload = json.dumps({
            "view_id": view_id
        })
    headers = {
        'Authorization': f"Bearer {tat}"
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    response_result = json.loads(response.text)
    
    if "error" in response_result:
        print("encountered error when getting records!")
        print(response_result)
    
    if 'items'not in response_result['data']:
        return []
    
    for record in response_result["data"]['items']:
        full_record_id.append(record)
    
    while response_result['data']['has_more'] == True:
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{grid_token}/tables/{table_token}/records?page_size=500&page_token={response_result['data']['page_token']}"
        response = requests.request("GET", url, headers=headers, data=payload)
        response_result = json.loads(response.text)
        for record in response_result["data"]['items']:
            full_record_id.append(record)
    return full_record_id


def update_data_for_db_records(data_lst, source_target_pairs, percentage=False):
    """
    update your data list of list so that it's fit for the multi-dim sheet.
    """
    updated_dict_lst = []
    for cur_dict in data_lst:
        new_fields_dict = {}
        for tup in source_target_pairs:
            item = None
            if isinstance(cur_dict['fields'][tup[0]], list):
                item = cur_dict['fields'][tup[0]][0]['text']
            else:
                item = str(round(cur_dict['fields'][tup[0]], 2))
            if percentage:
                if '%' in item:
                    item = float(item.strip('%')) / 100
                else:
                    item = float(item)
            new_fields_dict[tup[1]] = item
        new_di = {}
        new_di['record_id'] = cur_dict['id']
        new_di['fields'] = new_fields_dict
        updated_dict_lst.append(new_di)
    return updated_dict_lst

def update_records(tat, grid_token, table_token, view_id, records_dict):
    """
    utility function. update records in a multi-dim sheet.
    """
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{grid_token}/tables/{table_token}/records/batch_update"
    payload = json.dumps({
        'records': records_dict
    })
    
    headers = {
      'Content-Type': 'application/json',
      'Authorization': f'Bearer {tat}'
    }
    response = requests.request("POST", url, headers=headers, data=payload)
def update_columns_in_db(tat, grid_token, table_token, view_id, source_target_pairs, percentage=False):
    """
    function that updates full columns as specified in a multi-dim sheet
    @param tat: tenant access token
    @param grid_token: token for the multi-dim sheet
    @param table_token: subtable token
    @param view_id: view id for the specific view
    @param source_target_pairs: pairs of column names you want to copy data from and to. each element in the list is a tuple, the first
        elem of the tuple should be the source column, and the second should be the target column.
    @param percentage: whether the column data is in percentage form.
    
    """
    cur_data_lst = get_db_full_records(tat, grid_token, table_token, view_id)
    updated_data_lst = update_data_for_db_records(cur_data_lst, source_target_pairs, percentage)
    update_records(tat, grid_token, table_token, view_id, updated_data_lst)


"""

Function for the full Feishu sheets update with api calls.
Called by full_exec_seq.py, you can set all ids and tokens in that script at the top.
Right now it is a defined sequence of updates for the specific sheets of F23 dashboard files.
Be sure to make necessary changes to this function and its input variables according to the changes in the dashboard structure.
Each update funciton call is briefly explained by comments above it. Some metadata about the update is provided.

"""
def full_update_sequence(
        app_id,
        app_token,
        brain_chat_id,
        daily_submission_tab_names,
        personal_kpi_checker_tab_names,
        group_lead_checker_tab_names,
        daily_submission_sheet_token,
        department_submission_id,
        department_contacts_id,
        department_start_apps_id,
        daily_sub_id,
        daily_sub_base_token,
        daily_sub_base_table_id,
        alum_referral_id,
        personal_kpi_checker_sheet_token,
        personal_production_full_id,
        personal_submission_full_id,
        group_lead_checker_sheet_token,
        gl_daily_kpi_id,
        gl_weekly_kpi_id,
        gl_full_production_id,
        gl_full_submission_id,
        gl_weekly_no_prod_id,
        gl_2_weeks_no_prod_id,
        repeated_contacts_sheet_token,
        rc_sheet_id,
        hr_db_grid_token,
        f23_brain_update_table_id,
        dashboard_grid_token,
        ds_last_week_table_id,
        ds_lwt_nonkh_source_target_pairs,
        ds_lwt_nonkh_view_id,
        ds_lwt_kh_source_target_pairs,
        ds_lwt_kh_view_id,
        ds_full_season_table_id,
        ds_fst_nonkh_source_target_pairs,
        ds_fst_nonkh_view_id,
        ds_fst_kh_source_target_pairs,
        ds_fst_kh_view_id,
        db_submission_details_table_id,
        db_daily_kpi_table_id,
        db_weekly_kpi_table_id,
        db_monthly_kpi_table_id,
        high_quality_grid_token,
        high_quality_table_id,
        feishu_start_date
):
    # getting the tenant access code. this is an identifier for the updater bot so that the files allows for automatic access.
    tat = get_tat(app_id, app_token)

    # reading in the necessary files for the update.
    os.path.join("数据看板", ((date.today() + timedelta(days=-1)).strftime("%Y-%m-%d") + "重复人脉.xlsx"))
    daily_submission = pd.read_excel(os.path.join("数据看板","F23每日提交名单.xlsx"), sheet_name=None)
    group_lead = pd.read_excel(os.path.join("数据看板","F23产出小组长看板.xlsx"), sheet_name=None)
    personal_kpi = pd.read_excel(os.path.join("数据看板","F23个人绩效check1.0.xlsx"), sheet_name=None)
    repeated_contacts = pd.read_excel(os.path.join("数据看板", ((date.today() + timedelta(days=-1)).strftime("%Y-%m-%d") + "重复人脉.xlsx")), sheet_name=None)
    db = pd.read_excel(os.path.join("数据看板","dashboard1.xlsx"), sheet_name=None)
    high_quality = pd.read_excel(os.path.join("数据看板", "F23高质量项目.xlsx"), sheet_name=None)
    db["提交明细"]['推荐人'].fillna("", inplace=True)

    # # 以下是更新每日提交表格的内容

    """
    update每日部门提交
    本周sum：B7
    上周sum：B6
    数据开始cell：B14
    需要更新值的cell：A14

    """
    update_sum_ranges(tat, daily_submission_sheet_token, department_submission_id, "B7", "B6", "B14", len(daily_submission["每日部门提交"].iloc[0].tolist()))
    update_values_only_one_tab(daily_submission["每日部门提交"], tat, daily_submission_sheet_token, department_submission_id, "A14")
    print("finished daily department sub", flush=True)


    """
    update每日部门人脉新增
    本周sum：B9
    上周sum：B8
    数据开始cell：B10
    需要更新值的cell：A10

    """
    update_sum_ranges(tat, daily_submission_sheet_token, department_contacts_id, "B9", "B8", "B10", len(daily_submission["每日部门人脉新增"].iloc[0].tolist()))
    update_values_only_one_tab(daily_submission["每日部门人脉新增"], tat, daily_submission_sheet_token, department_contacts_id, "A10")
    print("finished daily department added contacts", flush=True)




    """
    update每日部门人脉开表
    本周sum：B7
    上周sum：B6
    数据开始cell：B8
    需要更新值的cell：A8
    """
    update_sum_ranges(tat, daily_submission_sheet_token, department_start_apps_id, "B7", "B6", "B8", len(daily_submission["每日部门开表"].iloc[0].tolist()))
    update_values_only_one_tab(daily_submission["每日部门开表"], tat, daily_submission_sheet_token, department_start_apps_id, "A8")
    print("finished finished daily department start apps", flush=True)


    """
    update校友裂变表
    需要更新值的cell：A2
    """
    update_values_only_one_tab(daily_submission["校友裂变"], tat, daily_submission_sheet_token, alum_referral_id, "A2") #校友裂变update
    print("finished alum referral", flush=True)



    """
    update每日提交表
    需要更新值的cell：A2
    """
    update_values_only_one_tab(daily_submission["每日提交"], tat, daily_submission_sheet_token, daily_sub_id, "A2") #每日提交update，可能有两个请求，因为cell比较多
    print("finished daily subs full", flush=True)


    # # 以下是更新小组长看板
    # 就按照表头名字来，然后top left cell都是A2，也不需要重新调sum公式

    """
    update每日绩效表
    需要更新值的cell：A2
    需要设置日期格式的左上cell：A2
    """
    update_values_only_one_tab(group_lead["每日绩效"], tat, group_lead_checker_sheet_token, gl_daily_kpi_id, "A2")
    set_date_format(tat, group_lead_checker_sheet_token, gl_daily_kpi_id, 'A2')
    print("finished daily kpi", flush=True)

    """
    update每周绩效表
    需要更新值的cell：A2
    需要设置日期格式的左上cell：B2
    """
    update_values_only_one_tab(group_lead["每周绩效"], tat, group_lead_checker_sheet_token, gl_weekly_kpi_id, "A2")
    set_date_format(tat, group_lead_checker_sheet_token, gl_weekly_kpi_id, 'B2')
    print("finished weekly kpi", flush=True)

    """
    update F23总绩效表
    需要更新值的cell：A2
    """
    update_values_only_one_tab(group_lead["F23总绩效"], tat, group_lead_checker_sheet_token, gl_full_production_id, "A2")
    print("finished f23 total kpi", flush=True)

    """
    update F23总提交明细表
    需要更新值的cell：A2
    需要设置日期格式的左上cell：C2
    """
    update_values_only_one_tab(group_lead["F23总提交明细"], tat, group_lead_checker_sheet_token, gl_full_submission_id, "A2")
    set_date_format(tat, group_lead_checker_sheet_token, gl_full_submission_id, 'C2')
    print("finished f23 submission details", flush=True)

    """
    update每周0产出名单表
    需要更新值的cell：A2
    需要设置日期格式的左上cell：B2
    """
    update_values_only_one_tab(group_lead["每周0产出名单"], tat, group_lead_checker_sheet_token, gl_weekly_no_prod_id, "A2")
    set_date_format(tat, group_lead_checker_sheet_token, gl_weekly_no_prod_id, 'B2')
    print("finished weekly 0 production", flush=True)


    """
    update每日绩效表
    需要更新值的cell：A2
    """
    if datetime.weekday(date.today() + timedelta(days=-1)) == 6:
        update_values_only_one_tab(group_lead["连续两周0产出名单"], tat, group_lead_checker_sheet_token, gl_2_weeks_no_prod_id, "A2")
        print("finished two weeks no production", flush=True)


    # # 以下是更新个人绩效check

    """
    update F23个人产出总数
    需要更新值的cell：A2
    """
    update_values_only_one_tab(personal_kpi["F23个人产出总数"], tat, personal_kpi_checker_sheet_token, personal_production_full_id, "A2")
    print("finished personal kpi f23 full", flush=True)

    """
    update F23总提交明细
    需要更新值的cell：A2
    """
    update_values_only_one_tab(personal_kpi["F23总提交明细"], tat, personal_kpi_checker_sheet_token, personal_submission_full_id, "A2")
    set_date_format(tat, personal_kpi_checker_sheet_token, personal_submission_full_id, 'C2')
    print("finished f23 full submission details", flush=True)

    # # 以下是更新重复人脉
    # #### 请确保重复人脉表格最下面没有空行！
    """
    update 重复人脉
    重复人脉并非每天全部重新粘贴，而需要append到表格结尾。所以，首先用end_of_sheet_top_left找到最后一行的行数，则得到了左上的cell
    需要更新值的cell：top_left_cell值
    需要设置日期格式的左上cell：G2, H2
    最后，用机器人功能把重复人脉表格发到大脑群里。
    """
    if len(repeated_contacts['重复人脉表']) > 0:
        top_left_cell = end_of_sheet_top_left("E", tat, repeated_contacts_sheet_token, rc_sheet_id)
        update_values_only_one_tab(repeated_contacts["重复人脉表"], tat, repeated_contacts_sheet_token, rc_sheet_id, top_left_cell)
        set_date_format(tat, repeated_contacts_sheet_token, rc_sheet_id, 'G2')
        set_date_format(tat, repeated_contacts_sheet_token, rc_sheet_id, 'H2')

        print("finished repeated contacts", flush=True)

        bot_send_messages(tat, repeated_contacts_sheet_token, brain_chat_id)
        print('message sent!', flush=True)

    # # 以下是更新HR数据库

    """
    update hr的F23大脑更新用表
    """
    update_one_tab_in_db(tat, hr_db_grid_token, f23_brain_update_table_id, personal_kpi["F23个人产出总数"])
    print("finished hr dashboard", flush=True)

    # # 以下是更新看板

    """
    update 每日绩效
    """
    update_one_tab_in_db(tat, dashboard_grid_token, db_daily_kpi_table_id, db["每日绩效"])
    print("finished db daily kpi", flush=True)

    """
    update 每周绩效
    """
    update_one_tab_in_db(tat, dashboard_grid_token, db_weekly_kpi_table_id, db["每周绩效"])
    print("finished db weekly kpi", flush=True)

    """
    update 每月绩效
    """
    update_one_tab_in_db(tat, dashboard_grid_token, db_monthly_kpi_table_id, db["每月绩效"])
    print("finished db monthly kpi", flush=True)

    """
    update 提交明细
    """
    update_one_tab_in_db(tat, dashboard_grid_token, db_submission_details_table_id, db["提交明细"])
    print("finished db full sub details", flush=True)
    time.sleep(1)

    """
    复制粘贴列的值（source to target）
    """
    update_columns_in_db(tat, dashboard_grid_token, ds_last_week_table_id, ds_lwt_nonkh_view_id, ds_lwt_nonkh_source_target_pairs, True)
    update_columns_in_db(tat, dashboard_grid_token, ds_last_week_table_id, ds_lwt_kh_view_id, ds_lwt_kh_source_target_pairs, True)
    update_columns_in_db(tat, dashboard_grid_token, ds_full_season_table_id, ds_fst_nonkh_view_id, ds_fst_nonkh_source_target_pairs, True)
    update_columns_in_db(tat, dashboard_grid_token, ds_full_season_table_id, ds_fst_kh_view_id, ds_fst_kh_source_target_pairs, True)
    print("finished db columns pasting", flush=True)
    time.sleep(0.5)

    # # 以下是更新每日提交里面的多维表格
    update_one_tab_in_db(tat, daily_sub_base_token, daily_sub_base_table_id, daily_submission["每日提交"])
    print('finished daily sub base sheet', flush=True)
    time.sleep(0.5)

    # # # 以下是更新高质量提交多为表格
    update_one_tab_in_db(tat, high_quality_grid_token, high_quality_table_id, high_quality['年轻+技术+名校or大厂'])
    print('finished high quality base sheet', flush=True)
