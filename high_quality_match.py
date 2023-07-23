    #!/usr/bin/env python
    # coding: utf-8
import re
import pandas as pd
import numpy as np
import datetime
import string
from pandas.api.types import is_string_dtype
import xlsxwriter
import re


def high_quality_match():
    ## 自定义导出名单：S23和F23中 2023.1.12 之后 
    ## 七个字段：公司名称；链接；提交时间；出生日期；教育经历；技术创始ornot；工作经历 
    df_customS23 = pd.read_csv("每日数据\S23ZDY.csv")
    df_customF23 = pd.read_csv("每日数据\F23ZDY.csv")
    df_custom = pd.concat([df_customS23, df_customF23])

    df_custom = df_custom[["链接","提交时间","出生日期 (第一位创始人)","你是个技术型创始人吗？","教育经历 - 学校、专业、学位和毕业年份","工作经历 - 工作过的公司，职位/头衔和日期"]]

    ## 用每日提交名单进行人脉匹配
    ## 六个字段 主要用 链接--做匹配；奇绩人脉负责人--再去对应部门
    merged_match1 = pd.read_excel('数据看板\F23每日提交名单.xlsx', sheet_name=None)['每日提交']
    merged_match2 = pd.read_excel('数据看板\F23留存每日提交名单.xlsx')
    merged_match = pd.concat([merged_match1,merged_match2])
    merged_match['产出归属小组'].fillna('Inbound', inplace=True)
    merged_match = merged_match[["链接","姓名","人脉中关联上级","产出归属小组"]]


    ## 合并两个表，以每日提交名单为准
    all_performance = pd.merge(df_custom, merged_match, on='链接')



    # 删除没有出生日期的行
    all_performance = all_performance.dropna(subset=['出生日期 (第一位创始人)'])
    # Convert '提交时间' and '出生日期' columns to datetime
    all_performance['提交时间'] = pd.to_datetime(all_performance['提交时间'])
    all_performance['出生日期 (第一位创始人)'] = pd.to_datetime(all_performance['出生日期 (第一位创始人)'])
    # 计算第一创始人年龄
    all_performance['第一创始人年龄'] = (all_performance['提交时间'].dt.year - all_performance['出生日期 (第一位创始人)'].dt.year)
    all_performance['第一创始人年龄'] = all_performance['第一创始人年龄'].astype(int)
    all_performance



    # 将 "工作经历""教育经历""技术型”列转换为字符串类型
    all_performance["工作经历 - 工作过的公司，职位/头衔和日期"] = all_performance["工作经历 - 工作过的公司，职位/头衔和日期"].astype(str)
    all_performance["教育经历 - 学校、专业、学位和毕业年份"] = all_performance["教育经历 - 学校、专业、学位和毕业年份"].astype(str)
    #all_performance["工作经历 - 工作过的公司，职位/头衔和日期"] = all_performance["工作经历 - 工作过的公司，职位/头衔和日期"].astype(str)
    all_performance["第一创始人工作经历"] = all_performance["工作经历 - 工作过的公司，职位/头衔和日期"].apply(lambda x: x.split("###")[0].strip())
    all_performance["第一创始人教育经历"] = all_performance["教育经历 - 学校、专业、学位和毕业年份"].apply(lambda x: x.split("###")[0].strip())
    all_performance["第一创始人技术型"] = all_performance["你是个技术型创始人吗？"].apply(lambda x: x.split("###")[0].strip())



    all_performance = all_performance.drop(columns = ["工作经历 - 工作过的公司，职位/头衔和日期","教育经历 - 学校、专业、学位和毕业年份","你是个技术型创始人吗？"])



    filtered_all_performance = all_performance[(all_performance["第一创始人技术型"] == "是") & (all_performance["第一创始人年龄"] < 35)]



    filtered_all_performance = filtered_all_performance[['链接','提交时间','第一创始人年龄','第一创始人工作经历','第一创始人教育经历','姓名','人脉中关联上级','产出归属小组']]



    famous_schools = pd.read_excel("每日数据\国内985和qs100.xlsx")["优质学校"].tolist()



    # 创建是否为名校的新列并设置初始值为 "否"
    filtered_all_performance["是否为名校"] = "否"

    # 遍历每行数据，检查教育经历是否包含名校
    for index, row in filtered_all_performance.iterrows():
        education = row["第一创始人教育经历"]
        for school in famous_schools:
            if school in education:
                filtered_all_performance.at[index, "是否为名校"] = "是"
                break


    famousschool_performance = filtered_all_performance[(filtered_all_performance["是否为名校"] == "是") ]




    company_list = ['滴滴', '旷视', '商汤', '快手', '爱奇艺', 'iqiyi', 'adobe', 'youtube', '谷歌', 'google', 'uber', 'tiktok', 'amazon', 'aws', 'apple', '苹果', 'meta', 'alibaba', '阿里', '腾讯', 'tencent', '百度', 'baidu', '京东', '蚂蚁金融', '网易', '美团', '字节', 'bytedance', '360', '新浪', '上海寻梦', '搜狐', '五八同城', '苏宁', '小米', '携程', '用友', '猎豹移动', '车之家', '唯品会', '浪潮', '同程旅游', '斗鱼', '咪咕', '鹏博士', '迅雷', '米哈游', '完美世界', '波克城市', '科大讯飞', '房多多', '美图', '美柚', '汇量科技', '创梦天地', '二三四五', '游族', '好未来', '金蝶软件', '贝壳找房', '途牛科技', '东方财富', '乐游网络', '蓝鲸人', '大众书网', '淘友天下', '多点', '蚂蚁金服', '三六零', '拼多多', '58集团', '58同城', '智联招聘', '4399', '东软', '盛趣游戏', '哔哩哔哩', '拉卡拉', '吉比特', '小红书', '学霸君', 'bilibili', 'b站', '甲骨文', 'oracle', '华为', 'huawei', 'sap', 'linkedin', '领英', '三星', 'samsung', '微软', 'microsoft', 'msra', 'ibm', '汽车之家', '摩拜', '哈啰', 'at&t', '小鹏汽车', '理想汽车', '蔚来汽车', '英特尔', 'intel', '英伟达', 'nvidia', '爱立信', '脸书', 'facebook', '大众点评', '支付宝', 'alipay', '大疆', '雅虎', 'yahoo', 'cisco', 'salesforce', 'netflix','deepmind',]

    # 创建一个空的列 "是否为大厂"
    filtered_all_performance["是否为大厂"] = ""

    # 遍历数据行
    for i, row in filtered_all_performance.iterrows():
        experience = row["第一创始人工作经历"]
        is_company = False


        # 检查工作经历是否包含大厂，排除实习和 "intern"

        for company in company_list:
                if (company in experience) and ("实习" not in experience) and ("intern" not in experience.lower()):
                    is_company = True
                    break


        # 将结果写入 "是否为大厂" 列
        if is_company:
            filtered_all_performance.at[i, "是否为大厂"] = "是"
        else:
            filtered_all_performance.at[i, "是否为大厂"] = "否"



    filtered_all_performance = filtered_all_performance[(filtered_all_performance["是否为大厂"] == "是") | (filtered_all_performance["是否为名校"] == "是") ]


    path = "数据看板\F23高质量项目.xlsx"
    group = filtered_all_performance.groupby('产出归属小组')['链接'].count()
    personal_match = filtered_all_performance[['姓名','产出归属小组']].drop_duplicates()
    personal = filtered_all_performance.groupby('姓名')['链接'].count()
    personal = pd.merge(personal, personal_match, on='姓名', how='left')


    with pd.ExcelWriter(path) as writer:
        filtered_all_performance.to_excel(writer, sheet_name='年轻+技术+名校or大厂', index=False, engine='xlsxwriter')
        group.to_excel(writer, sheet_name='Grouped_resultF23年轻技术', index=False, engine='xlsxwriter')
        personal.to_excel(writer, '个人_resultF23年轻技术', index=False, engine='xlsxwriter')

    print('finished the high quality match')


