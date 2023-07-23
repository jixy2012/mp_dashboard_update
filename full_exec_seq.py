# coding: utf-8
import time
from daily_sub_updater import generate_daily_submission_sheets
from download_sup_sheet import sup_sheet_download
from automatic_download_everything import full_file_download_sequence
from dashboard_updater import generate_dashboard_info
from datetime import datetime
from official_dashboard_sheets_update import full_update_sequence
from high_quality_match import high_quality_match

superior_sheet_token = "shtcnHlvHoQaAymjwsnEK1iU4oc"
app_id = "cli_a4e5badd833bd00d"
app_token = "n16oQih9feqSrPRvx4y10eoImrUOLDqd"

batch_id = 10
base_url = 'https://apply.miracleplus.com'
group_info_url = f"{base_url}/admin/contact_statistics/groups"
login_url = f"{base_url}/users/sign_in"
contacts_download = f"{base_url}/admin/contact_statistics/export"
exportation_url = f"{base_url}/evaluation/applications/"
exportation_contact_url = f"{exportation_url}"
exportation_application_url = f"{exportation_url}/exportation"
history_export_url = f"{base_url}/export_history"


brain_chat_id = 'oc_5b97f7ed498b493d357b4eb274f12c72'
# all sheet updates
daily_submission_tab_names = ["每日部门提交", "每日部门人脉新增", "每日部门开表", "每日提交", "校友裂变"]
personal_kpi_checker_tab_names = ["F23个人产出总数", "F23总提交明细"]
group_lead_checker_tab_names = ["每日绩效", "每周绩效", "F23总绩效", "F23总提交明细", "每周0产出名单", "连续两周0产出名单"]
# 每日提交明细
daily_submission_sheet_token = "shtcnaCnBxPLfF0Jf20IKyo3mvd"
department_submission_id = "bwbiFz"
department_contacts_id = "rJaWnF"
department_start_apps_id = "YEScm3"
daily_sub_id = "oUlnHs"
daily_sub_base_token = 'bascnR3VlJCAnm6PcaLPKp9tUxe'
daily_sub_base_table_id = 'tbleYCS8EUrzB9MZ'
alum_referral_id = "y4TLZc"

# 个人绩效check
personal_kpi_checker_sheet_token = "shtcnhSLyN1AADG8M9zNgPv2Brb"
personal_production_full_id = "UTikV9"
personal_submission_full_id = "lqmfOF"

# 小组长看板
group_lead_checker_sheet_token = "shtcn7XCz4zSHoD2TmCTaiu9Wjb"
gl_daily_kpi_id = "MkBzHV"
gl_weekly_kpi_id = "xt9ZXS"
gl_full_production_id = "0Q3Pe6"
gl_full_submission_id = "G8TwQS"
gl_weekly_no_prod_id = "THcb0A"
gl_2_weeks_no_prod_id = "gSknkc"

#重复人脉
repeated_contacts_sheet_token = "shtcnMJnJ9C95lGFiTllHX3CCEG"
rc_sheet_id = "0kjgLz"

# HR data update
hr_db_grid_token = "bascn0b13i7Yh8RhtZXn0tSLlDc"
f23_brain_update_table_id = "tblrdJLGhwsGS0Yy"

# Dashboard update
dashboard_grid_token = "bascnDJ1WPeRBja7qn2nTo6vtRc"

ds_last_week_table_id = "tblKGWbTG586DEQI"
ds_lwt_nonkh_source_target_pairs = [('上周提交目标完成率', '上周提交目标完成进度')]
ds_lwt_nonkh_view_id = 'vewUrb9fVY'
ds_lwt_kh_source_target_pairs = [('F23总目标完成率', 'F23总目标完成进度（1.12起）'), ('F23截止到上周末目标完成率', 'F23截止到上周末目标完成进度'), ('上周目标完成率', '上周提交目标完成进度')]
ds_lwt_kh_view_id = 'vewzysoTns'

ds_full_season_table_id = "tblhSQ8WpPjzXGy0"
ds_fst_nonkh_source_target_pairs = [('F23总目标完成率（1.12起）', 'F23总目标完成进度（1.12起）'), ('F23截止到上周末目标完成率', 'F23截止到上周末目标完成进度')]
ds_fst_nonkh_view_id = 'vewUrb9fVY'
ds_fst_kh_source_target_pairs = [('F23总目标完成率（1.12起）', 'F23总目标完成进度（1.12起）'), ('F23截止到上周末目标完成率', 'F23截止到上周末目标完成进度'), ('上周目标完成率', '上周提交目标完成进度')]
ds_fst_kh_view_id = 'vewzysoTns'

db_submission_details_table_id = "tble3Z65HBDYBIyu"
db_daily_kpi_table_id = "tblg9jeYIGIwJAT8"
db_weekly_kpi_table_id = "tblMZHxr9c53gDqv"
db_monthly_kpi_table_id = "tblwE2VRYpyiAGzI"

# High Quality
high_quality_grid_token = 'FXGybddgyaBCVrsZCAtccLAanWK'
high_quality_table_id = 'tblDQhwvelTmRdni'

feishu_start_date = datetime(1899, 12, 30)

sup_sheet_download(app_id, app_token, superior_sheet_token)


time.sleep(.5)
full_file_download_sequence(
    batch_id, 
    base_url, 
    group_info_url, 
    login_url,
    contacts_download,
    exportation_url, 
    exportation_contact_url,
    exportation_application_url,
    history_export_url
)
time.sleep(1)

generate_daily_submission_sheets()

time.sleep(.5)

generate_dashboard_info()

time.sleep(.5)

high_quality_match()

time.sleep(.5)

full_update_sequence(
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
)

