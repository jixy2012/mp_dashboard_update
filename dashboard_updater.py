# coding: utf-8

import os
import numpy as np
import pandas as pd
from tqdm import tqdm
import jieba.posseg as pseg
from datetime import date, timedelta, datetime
def generate_dashboard_info():
    #==========================================1、读取每日导出的数据==========================================#
    yesterday = (date.today() + timedelta(days=-1)).strftime("%Y-%m-%d") # 昨天的日期字符串
    today = (date.today()).strftime("%Y-%m-%d") # 今天的日期字符串

    # 从3月1号到昨天的所有日期放在datelist这个列表里
    datelist = []
    start = datetime.strptime('2023/3/1', '%Y/%m/%d')
    end = datetime.strptime(yesterday, '%Y-%m-%d')
    step = timedelta(days=1)
    while start<=end:
        datestr = (start.date()).strftime("%Y-%m-%d")
        datelist.append(datestr)
        start += step


    # 读取导出的数据
    matching_df_1 = pd.read_csv('每日数据/F23RM_export.csv', encoding='utf-8')
    matching_df_2 = pd.read_csv('每日数据/S23RM_export.csv', encoding='utf-8')
    matching_df = pd.concat([matching_df_1,matching_df_2],ignore_index= True)
    performance_df_1 = pd.read_csv('每日数据/F23ZDY.csv', encoding='utf-8')
    performance_df_2 = pd.read_csv('每日数据/S23ZDY.csv', encoding='utf-8')
    performance_df = pd.concat([performance_df_1,performance_df_2],ignore_index= True)
    performance_df = performance_df[performance_df[r'提交时间'].isin(datelist)]

    #csv_files = [file for file in os.listdir(r'E:/111奇绩创坛/11每日看板生成/F23每日看板生成（新）/每日数据') if file.endswith('.csv')]
    #for i in csv_files:
    #    keystring = 'export'
    #    if keystring in i:
    #        matching_df = pd.read_csv('E:/111奇绩创坛/11每日看板生成/F23每日看板生成（新）/每日数据/'+i, encoding='utf-8')
    #        # matching_df：人脉匹配名单
    #    else:
    #        performance_df = pd.read_csv('E:/111奇绩创坛/11每日看板生成/F23每日看板生成（新）/每日数据/'+i, encoding='utf-8')
    #        performance_df = performance_df[performance_df[r'提交时间'].isin(datelist)]
            # performance_df：自定义导出的申请表信息
    department_df = pd.read_excel('关联上级.xlsx', sheet_name=0)
    department_df["姓名"] = department_df["姓名"].apply(lambda x: x.strip())
    department_df["人脉中关联上级"] = department_df["人脉中关联上级"].apply(lambda x: x.strip())

    # department_df：关联上级表
    matching_df = matching_df[['链接', '匹配到的申请表中人员', '匹配到的联系方式', '匹配到的奇绩人脉负责人']]
    indexNames = performance_df[performance_df.提交时间 == today].index
    performance_df.drop(indexNames, inplace=True)
    performance_df = performance_df.rename(columns={'如果有，请填写推荐人姓名，推荐人可以包括奇绩团队成员，奇绩校友，投资人等。':'推荐人',
                                                    '核心创始人目前所处的职业阶段是？':'核心创始人画像',
                                                    '请对产品进行一句话概括（15字以内，如“极其方便的云端协作文档”、“开源自动驾驶技术”）':'一句话概括'})
    order = ['产品名称', '链接', '开表时间', '提交时间', '推荐人', '核心创始人画像', '一句话概括']
    performance_df = performance_df[order]
    # 读取人脉导出的数据
    csv_files = [file for file in os.listdir(r'人脉数据') if file.endswith('.csv')]
    renmai_df = pd.DataFrame()
    for csv in csv_files:
        df_csv = pd.read_csv(r'人脉数据/%s' % csv)
        df_csv = df_csv[df_csv[r'\uFEFF日期'].isin(datelist)]
        renmai_df = pd.concat([renmai_df, df_csv], axis=0)

    renmai_df = renmai_df.reset_index(drop=True)
    renmai_df = renmai_df.rename(columns={r'\uFEFF日期': '日期'})

    renmai_df = renmai_df.sort_values(['日期', '姓名'], ascending=[True, True]).groupby(by=['日期', '姓名'], as_index=False).sum()

    pdpper_all_except_tijiao_df = renmai_df
    kaibiao_df = renmai_df[['日期', '姓名', '开表增量']]
    renmai_df = renmai_df[['日期', '姓名', '人脉增量']]
    #print(department_df)
    pdpper_all_except_tijiao_df = pd.merge(pdpper_all_except_tijiao_df, department_df, how='left', on='姓名')
    pdpper_all_except_tijiao_df = pdpper_all_except_tijiao_df.drop(['提交增量', '产出归属小组', '是否追产出'], axis=1)
    pdpper_all_except_tijiao_df = pdpper_all_except_tijiao_df.sort_values(by=['日期', '人脉中关联上级', '姓名'])
    pdpper_all_except_tijiao_df = pdpper_all_except_tijiao_df.reset_index(drop=True)
    print(len(pdpper_all_except_tijiao_df))
    pdpper_all_except_tijiao_df = pdpper_all_except_tijiao_df[(pdpper_all_except_tijiao_df.日期 > pdpper_all_except_tijiao_df.入职时间)|(pdpper_all_except_tijiao_df.入职时间.isnull())]
    print(len(pdpper_all_except_tijiao_df))

    employee_df = department_df
    # employee_df = employee_df.reset_index()

    all_performance = pd.merge(performance_df, matching_df, how='left', on='链接')
    # 4、每日产出总表
    all_performance = all_performance.drop(['匹配到的联系方式'], axis=1)
    all_performance = all_performance.rename(columns={'匹配到的奇绩人脉负责人':'姓名','匹配到的申请表中人员':'申请者&创始人'})
    for i in range(len(all_performance)):
        strg = all_performance.loc[i, '姓名']
        if type(strg) != float:
            strg = strg.strip(';')
            strg = strg.split(';')
            if len(strg)==1:
                strg = strg[-1]
            else:
                if strg[-1] == '重复数据':
                    strg = strg[-2]
                else:
                    strg = strg[-1]
            all_performance.loc[i, '姓名'] = strg
    # 先将负责人为null的填充为“空”
    all_performance['姓名'].fillna("空",inplace=True)
    all_performance = pd.merge(all_performance, department_df, how='left', on='姓名')
    # all_performance = all_performance.drop(['是否追产出'], axis=1)
    all_performance = all_performance.sort_values(by='提交时间', ascending=[False])
    all_performance = all_performance.drop(['入职时间'], axis=1)
    # 关联上级表没有的intern但在人脉系统中有
    all_performance['人脉中关联上级'].fillna("MiraclePlusBrain",inplace=True)
    all_performance['产出归属小组'].fillna("Inbound",inplace=True)
    all_performance['是否追产出'].fillna("否",inplace=True)
    all_performance['是否在职'].fillna("否",inplace=True)
    all_performance['是否开通人脉权限'].fillna("否",inplace=True)
    all_performance['推荐人'].fillna("",inplace=True)
    all_performance = all_performance.drop(['备注'], axis=1)

    # 2、每日绩效
    employee_performance = pd.DataFrame(columns=['日期', '姓名', '提交数', '人脉中关联上级','产出归属小组','是否追产出','是否在职'])


    for i in tqdm(range(len(datelist))):
        #df = employee_df.drop(['是否追产出'], axis=1)
        df = employee_df[:]
        df.loc[:, '提交数'] = [0] * len(df)
        df.loc[:, '日期'] = datelist[i]
        df = df[['日期', '姓名', '提交数', '人脉中关联上级','产出归属小组','是否追产出','是否在职']]
        pdf = all_performance[all_performance.提交时间==datelist[i]]
        for k in range(len(df)):
            for j in range(len(pdf)):
                if type(pdf.loc[pdf.index[j], '姓名']) != float:
                    if pdf.loc[pdf.index[j], '姓名'] == str(df.loc[k, '姓名']):
                        if pdf.loc[pdf.index[j], '提交时间'] == str(df.loc[k, '日期']):
                            df.loc[k, '提交数'] += 1
        employee_performance = pd.concat([employee_performance, df], axis=0)

    employee_performance = employee_performance.sort_values(by=['日期', '人脉中关联上级', '姓名'])
    pdpper_all_except_tijiao_df = pdpper_all_except_tijiao_df.drop(['人脉中关联上级','是否在职'], axis=1)
    employee_performance = pd.merge(employee_performance, pdpper_all_except_tijiao_df, how='left', on=['日期', '姓名'])
    employee_performance = employee_performance.rename(columns={'自增总量':'人脉自增数', '开表增量':'开表数'})

    sums = employee_performance[['提交数', '人脉增量', '人脉自增数', '开表数']].sum(axis=1)
    filtered_employee_performance = employee_performance[sums != 0]

    employee_performance = filtered_employee_performance[['日期', '姓名', '提交数', '人脉增量', '人脉自增数', '开表数', '人脉中关联上级','产出归属小组','是否追产出','是否在职']]
    employee_performance['提交数'].fillna(0,inplace=True)
    employee_performance['人脉增量'].fillna(0,inplace=True)
    employee_performance['人脉自增数'].fillna(0,inplace=True)
    employee_performance['开表数'].fillna(0,inplace=True)


    # 3、每周绩效表
    weekly_employee_performance = employee_performance[['日期', '姓名', '提交数', '人脉中关联上级','产出归属小组','是否追产出','是否在职']]
    weekly_employee_performance.loc[:,'日期'] = pd.to_datetime(weekly_employee_performance['日期'])
    weekly_employee_performance = weekly_employee_performance.set_index('日期')
    weekly_employee_performance = weekly_employee_performance.groupby('姓名')
    weekly_employee_performance = weekly_employee_performance.resample('w').sum()
    order = ['提交数']
    weekly_employee_performance = weekly_employee_performance[order]
    weekly_employee_performance['周（周最后一日）'] = weekly_employee_performance.index.get_level_values(1)
    weekly_employee_performance['姓名'] = weekly_employee_performance.index.get_level_values(0)
    weekly_employee_performance.index = weekly_employee_performance.index.droplevel(1) # 删除指定等级的索引
    weekly_employee_performance = weekly_employee_performance.reset_index(drop=True)
    weekly_employee_performance = pd.merge(weekly_employee_performance, employee_df, how='left', on='姓名')
    order = ['姓名', '周（周最后一日）', '提交数', '人脉中关联上级','产出归属小组','是否追产出','是否在职']
    weekly_employee_performance = weekly_employee_performance[order]
    weekly_employee_performance = weekly_employee_performance.sort_values(by=['人脉中关联上级', '姓名'])
    weekly_employee_performance['周（周最后一日）'] = weekly_employee_performance['周（周最后一日）'].astype('str')

    # 上面算的是提交数
    # 下面算的是另外三个

    weekly_employee_performance_2 = employee_performance
    weekly_employee_performance_2.loc[:, '日期'] = pd.to_datetime(weekly_employee_performance_2['日期'])
    weekly_employee_performance_2 = weekly_employee_performance_2.set_index('日期')
    weekly_employee_performance_2 = weekly_employee_performance_2.groupby('姓名')
    weekly_employee_performance_2 = weekly_employee_performance_2.resample('w').sum()

    weekly_employee_performance_2['周（周最后一日）'] = weekly_employee_performance_2.index.get_level_values(1)
    weekly_employee_performance_2['姓名'] = weekly_employee_performance_2.index.get_level_values(0)
    weekly_employee_performance_2.index = weekly_employee_performance_2.index.droplevel(1) # 删除指定等级的索引
    weekly_employee_performance_2 = weekly_employee_performance_2.reset_index(drop=True)
    weekly_employee_performance_2 = weekly_employee_performance_2[['姓名', '周（周最后一日）', '人脉增量', '人脉自增数', '开表数']]

    weekly_employee_performance = pd.concat([weekly_employee_performance, weekly_employee_performance_2[['人脉增量', '人脉自增数', '开表数']]], axis=1)

    week_date = weekly_employee_performance['周（周最后一日）'].unique().tolist()
    week_date.sort()
    for i in range(len(weekly_employee_performance)):
        for j in range(len(week_date)):
            if weekly_employee_performance.loc[i,"周（周最后一日）"] == week_date[j]:
                weekly_employee_performance.loc[i,"weekX"] = 'week'+str(j+1)
    weekly_employee_performance = weekly_employee_performance[['姓名', '周（周最后一日）','weekX', '提交数', '人脉增量', '人脉自增数', '开表数', '人脉中关联上级','产出归属小组','是否追产出','是否在职']]

    # 4、每月绩效表
    monthly_employee_performance = employee_performance[['日期', '姓名', '提交数', '人脉中关联上级','产出归属小组','是否追产出','是否在职']]
    monthly_employee_performance.loc[:,'日期'] = pd.to_datetime(monthly_employee_performance['日期'])
    monthly_employee_performance = monthly_employee_performance.set_index('日期')
    monthly_employee_performance = monthly_employee_performance.groupby('姓名')
    monthly_employee_performance = monthly_employee_performance.resample('m').sum()
    order = ['提交数']
    monthly_employee_performance = monthly_employee_performance[order]
    monthly_employee_performance['月（月最后一日）'] = monthly_employee_performance.index.get_level_values(1)
    monthly_employee_performance['姓名'] = monthly_employee_performance.index.get_level_values(0)
    monthly_employee_performance.index = monthly_employee_performance.index.droplevel(1) # 删除指定等级的索引
    monthly_employee_performance = monthly_employee_performance.reset_index(drop=True)
    monthly_employee_performance = pd.merge(monthly_employee_performance, employee_df, how='left', on='姓名')
    order = ['姓名', '月（月最后一日）', '提交数', '人脉中关联上级','产出归属小组','是否追产出','是否在职']
    monthly_employee_performance = monthly_employee_performance[order]
    monthly_employee_performance = monthly_employee_performance.sort_values(by=['人脉中关联上级', '姓名'])
    monthly_employee_performance['月（月最后一日）'] = monthly_employee_performance['月（月最后一日）'].astype('str')
    # 上面算的是提交数
    # 下面算的是另外三个

    monthly_employee_performance_2 = employee_performance
    monthly_employee_performance_2.loc[:,'日期'] = pd.to_datetime(monthly_employee_performance_2['日期'])
    monthly_employee_performance_2 = monthly_employee_performance_2.set_index('日期')
    monthly_employee_performance_2 = monthly_employee_performance_2.groupby('姓名')
    monthly_employee_performance_2 = monthly_employee_performance_2.resample('m').sum()

    monthly_employee_performance_2['月（月最后一日）'] = monthly_employee_performance_2.index.get_level_values(1)
    monthly_employee_performance_2['姓名'] = monthly_employee_performance_2.index.get_level_values(0)
    monthly_employee_performance_2.index = monthly_employee_performance_2.index.droplevel(1) # 删除指定等级的索引
    monthly_employee_performance_2 = monthly_employee_performance_2.reset_index(drop=True)
    monthly_employee_performance_2 = monthly_employee_performance_2[['姓名', '月（月最后一日）', '人脉增量', '人脉自增数', '开表数']]

    monthly_employee_performance = pd.concat([monthly_employee_performance, monthly_employee_performance_2[['人脉增量', '人脉自增数', '开表数']]], axis=1)

    month_date = monthly_employee_performance['月（月最后一日）'].unique().tolist()
    month_date.sort()
    for i in range(len(monthly_employee_performance)):
        for j in range(len(month_date)):
            if monthly_employee_performance.loc[i,"月（月最后一日）"] == month_date[j]:
                monthly_employee_performance.loc[i,"monthX"] = 'month' + str(j + 1)
    monthly_employee_performance = monthly_employee_performance[['姓名', '月（月最后一日）','monthX', '提交数', '人脉增量', '人脉自增数', '开表数', '人脉中关联上级','产出归属小组','是否追产出','是否在职']]

    path = os.path.join("数据看板", "dashboard1.xlsx")
    with pd.ExcelWriter(path) as writer:
        employee_performance.to_excel(writer, sheet_name='每日绩效', index=False, engine='xlsxwriter')
        weekly_employee_performance.to_excel(writer, sheet_name='每周绩效', index=False, engine='xlsxwriter')
        monthly_employee_performance.to_excel(writer, sheet_name='每月绩效', index=False, engine='xlsxwriter')
        all_performance.to_excel(writer, sheet_name='提交明细', index=False, engine='xlsxwriter')