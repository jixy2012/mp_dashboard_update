# -*- coding: utf-8 -*-
"""
Created on Fri Apr 28 14:38:52 2023

@author: 靓仔艺
"""

import os
import numpy as np
import pandas as pd
from tqdm import tqdm
import jieba.posseg as pseg
from datetime import date, timedelta, datetime
import xlsxwriter



def generate_daily_submission_sheets():
    # ==========================================1、存储日期==========================================#
    yesterday = (date.today() + timedelta(days=-1)).strftime("%Y-%m-%d") 
    # 使用了 date.today() 函数获取今天的日期，然后使用 timedelta(days=-1) 函数将日期减去一天，得到昨天的日期，最后使用 strftime() 函数将日期转换为格式化的字符串。
    today = (date.today()).strftime("%Y-%m-%d")  # 今天的日期字符串

    # 从3月1日到昨天的所有日期放在datelist这个列表里
    datelist = [] #用于存储生成的日期
    start = datetime.strptime('2023/3/1', '%Y/%m/%d') #开始的日期=start
    end = datetime.strptime(yesterday, '%Y-%m-%d') #结束的日期=end
    step = timedelta(days=1) #每次迭代步长为1天
    while start <= end:
        datestr = (start.date()).strftime("%Y-%m-%d")
        datelist.append(datestr)
        start += step
    #存储日期


    # ==================================2、读取数据并将S23和F23的数据合并在同一个表格中===================================================#
    df1 = pd.read_csv("每日数据\S23RM_export.csv")
    df2 = pd.read_csv("每日数据\F23RM_export.csv")
    #合并
    dfRM = pd.concat([df1,df2])
    path00 = '每日数据\matching_export.xlsx'
    with pd.ExcelWriter(path00) as writer: #使用 pd.ExcelWriter() 函数创建一个 ExcelWriter 对象，并将其赋值给变量 writer。该对象将用于写入 Pandas DataFrame 到 Excel 文件中。
        dfRM.to_excel(writer, sheet_name='合并后人脉匹配名单', index=False, engine='xlsxwriter')
        # 将 dfRM 中的数据写入到 ExcelWriter 对象中。该方法还接受 sheet_name 和 index 参数，用于指定生成的 Excel 工作表的名称和是否包含行索引。

    df3 = pd.read_csv("每日数据\S23ZDY.csv")
    df4 = pd.read_csv("每日数据\F23ZDY.csv")
    df3 = df3.drop('工作经历 - 工作过的公司，职位/头衔和日期', axis=1)
    df4 = df4.drop('工作经历 - 工作过的公司，职位/头衔和日期', axis=1)
    df3 = df3.drop('教育经历 - 学校、专业、学位和毕业年份', axis=1)
    df4 = df4.drop('教育经历 - 学校、专业、学位和毕业年份', axis=1)

    # 合并
    dfZDY = pd.concat([df3,df4])
    path01 = '每日数据\ZDY.xlsx'
    with pd.ExcelWriter(path01) as writer:
        dfZDY.to_excel(writer, sheet_name='合并后自定义名单', index=False, engine='xlsxwriter')



    #创建了一个包含所有扩展名为".xlsx"的Excel文件的列表，这些文件存储在名为“每日数据”的目录中。
    matching_df = pd.read_excel('每日数据\matching_export.xlsx')
    performance_df = pd.read_excel('每日数据\ZDY.xlsx')
    performance_df = performance_df[performance_df[r'提交时间'].isin(datelist)]#对时间进行处理，确保是3.1之后的提交日期
    #如果文件名包含字符串“export”，则代码将该Excel文件读入名为“matching_df”的Pandas数据帧中。否则，它将Excel文件读入名为“performance_df”的Pandas数据帧中。      


    
    department_df = pd.read_excel('关联上级.xlsx', sheet_name=0)
    department_df["姓名"] = department_df["姓名"].apply(lambda x: x.strip())
    department_df["人脉中关联上级"] = department_df["人脉中关联上级"].apply(lambda x: x.strip())
    department_df['入职时间'] = pd.to_datetime(department_df['入职时间'])#将“department_df”中的“入职时间”列转换为datetime格式。
    # department_df：关联上级表

    matching_df = matching_df[['链接', '匹配到的申请表中人员', '匹配到的联系方式', '匹配到的奇绩人脉负责人']]
    indexNames = performance_df[performance_df.提交时间 == today].index #删除“提交时间”列与今天日期相符的行
    performance_df.drop(indexNames, inplace=True)#排序
    performance_df = performance_df.rename(
        columns={'如果有，请填写推荐人姓名，推荐人可以包括奇绩团队成员，奇绩校友，投资人等。': '推荐人',
                '核心创始人目前所处的职业阶段是？': '核心创始人画像',
                '请对产品进行一句话概括（15字以内，如“极其方便的云端协作文档”、“开源自动驾驶技术”）': '一句话概括'})
    #重新命名
    order = ['产品名称', '链接', '开表时间', '提交时间', '推荐人', '核心创始人画像', '一句话概括']
    performance_df = performance_df[order]


    # 读取人脉导出的数据
    csv_files = [file for file in os.listdir(r'人脉数据') if file.endswith('.csv')]
    renmai_df = pd.DataFrame()
    for csv in csv_files:
        df_csv = pd.read_csv(r'人脉数据/%s' % csv)
        df_csv = df_csv[df_csv[r'\uFEFF日期'].isin(datelist)]  #选出数据框中日期与“datelist”中指定的日期相同的行
        renmai_df = pd.concat([renmai_df, df_csv], axis=0)
    renmai_df = renmai_df.reset_index(drop=True)
    renmai_df = renmai_df.rename(columns={r'\uFEFF日期': '日期'})
    
    renmai_df = renmai_df.sort_values(['日期', '姓名'], ascending=[True, True]).groupby(by=['日期', '姓名'],as_index=False).sum()
    #对renmai_df进行排序和分组


    pdpper_all_except_tijiao_df = renmai_df
    kaibiao_df = renmai_df[['日期', '姓名', '开表增量']]
    renmai_df = renmai_df[['日期', '姓名', '人脉增量']]
    # print(department_df)
    pdpper_all_except_tijiao_df = pd.merge(pdpper_all_except_tijiao_df, department_df, how='left', on='姓名')
    #左连接，即在数据集中根据姓名作为关键字，连接两个表格
    pdpper_all_except_tijiao_df = pdpper_all_except_tijiao_df[pdpper_all_except_tijiao_df.是否追产出 == '是']
    #筛选出追产出的行
    pdpper_all_except_tijiao_df = pdpper_all_except_tijiao_df.drop(['提交增量', '产出归属小组', '是否追产出'], axis=1)
    #删除了三列的数据
    pdpper_all_except_tijiao_df = pdpper_all_except_tijiao_df.sort_values(by=['日期', '人脉中关联上级', '姓名'])
    #排序，按日期排序然后按姓名排序
    pdpper_all_except_tijiao_df = pdpper_all_except_tijiao_df.reset_index(drop=True)
    #重置索引（删除合并或更改数据后需要做的）
    #print(len(pdpper_all_except_tijiao_df))
    pdpper_all_except_tijiao_df = pdpper_all_except_tijiao_df[
        pdpper_all_except_tijiao_df.日期 >= pdpper_all_except_tijiao_df.入职时间]
    #筛选出数据中日期列中大于入职时间的行


    renmai_df = pd.merge(renmai_df, department_df[['姓名', '产出归属小组']], how='left', on='姓名')
    #左连接，renmai_df中添加了关联上级表的产出归属小组
    renmai_df = renmai_df.sort_values(['日期', '产出归属小组'], ascending=[True, True]).groupby(by=['日期', '产出归属小组'],as_index=False).sum()
    #按升序对“renmai_df”数据框进行排序，首先按日期排序，然后按“产出归属小组”排序。接着按“日期”和“产出归属小组”两列进行分组，并对每组的“人脉增量”列求和。
    renmai_df = renmai_df[['日期', '产出归属小组', '人脉增量']]

    renmai_df = renmai_df.pivot(index='日期', columns='产出归属小组', values='人脉增量')
    #创建一个新的数据框，其中“日期”列用作行索引，“产出归属小组”列用作列索引，而“人脉增量”列则用作值。
    renmai_df['总计'] = renmai_df.sum(axis=1)#添加了一列名为“总计”的新列，其中每行的值是该行所有其他列的和。
    renmai_df = renmai_df[
        ['总计', 'CCC', 'NEXT', 'UNIX', 'Jedi', 'Hermit', 'Global I', 'Global Ⅱ', 'Shawn Pang', 'PTA', 'Fellow',
        'Inbound', 'Core Team', 'Scout', 'E3C', '校友裂变']]
    renmai_df = renmai_df.sort_index(ascending=False)



    # 2、小组每日开表数
    kaibiao_df = pd.merge(kaibiao_df, department_df[['姓名', '产出归属小组']], how='left', on='姓名')

    #kaibiao_df  日期、姓名、开表增量   和关联上级进行左连接  ——> kaibiao_df=日期、姓名、开表增量、产出归属小组
    kaibiao_df = kaibiao_df.sort_values(['日期', '产出归属小组'], ascending=[True, True]).groupby(by=['日期', '产出归属小组'], as_index=False).sum()
    #按升序对“renmai_df”数据框进行排序，首先按日期排序，然后按“产出归属小组”排序。接着按“日期”和“产出归属小组”两列进行分组，并对每组的“人脉增量”列求和
    kaibiao_df = kaibiao_df[['日期', '产出归属小组', '开表增量']]
    kaibiao_df = kaibiao_df.pivot(index='日期', columns='产出归属小组', values='开表增量')


    kaibiao_df['总计'] = kaibiao_df.sum(axis=1)
    kaibiao_df = kaibiao_df[
        ['总计', 'CCC', 'NEXT', 'UNIX', 'Jedi', 'Hermit', 'Global I', 'Global Ⅱ', 'Shawn Pang', 'PTA', 'Fellow',
        'Inbound', 'Core Team', 'Scout', 'E3C', '校友裂变']]
    kaibiao_df = kaibiao_df.sort_index(ascending=False)

    #同上

    # 3、每日重复人脉
    yesterday_performance = performance_df[performance_df.提交时间 == yesterday]
    yesterday_performance = pd.merge(yesterday_performance, matching_df, how='left', on='链接')
    repeated_df = yesterday_performance[yesterday_performance.匹配到的奇绩人脉负责人.str.contains(';', na=False)]
    path1 = '数据看板/' + yesterday + '重复人脉.xlsx'
    with pd.ExcelWriter(path1) as writer:
        repeated_df.to_excel(writer, sheet_name='重复人脉表', index=False, engine='xlsxwriter')

    # 所有重复人脉
    all_performance = pd.merge(performance_df, matching_df, how='left', on='链接')
    repeated_df = all_performance[all_performance.匹配到的奇绩人脉负责人.str.contains(';', na=False)]
    path1 = '数据看板/重复人脉-总.xlsx'
    with pd.ExcelWriter(path1) as writer:
        repeated_df.to_excel(writer, sheet_name='重复人脉表', index=False, engine='xlsxwriter')




    # 4、每日产出总表
    all_performance = all_performance.drop(['匹配到的申请表中人员', '匹配到的联系方式'], axis=1)
    all_performance = all_performance.rename(columns={'匹配到的奇绩人脉负责人': '姓名'})
    for i in range(len(all_performance)):
        strg = all_performance.loc[i, '姓名']
        if type(strg) != float:
            strg = strg.strip(';')#去除多余的；  
            strg = strg.split(';')#分割数据
            if len(strg) == 1:
                strg = strg[-1]
            else:
                if strg[-1] == '重复数据':
                    strg = strg[-2]
                else:
                    strg = strg[-1]
            all_performance.loc[i, '姓名'] = strg
    #将分号（;）分隔的多个人名中的最后一个人名作为该行的负责人姓名。如果该列中只有一个人名，则直接使用该人名作为负责人姓名。如果最后一个人名是“重复数据”，则使用倒数第二个人名作为负责人姓名。处理结果保存在all_performance数据表的“姓名”列中。

    all_performance = pd.merge(all_performance, department_df, how='left', on='姓名')
    all_performance = all_performance.drop(['是否追产出'], axis=1)
    all_performance = all_performance.sort_values(by='提交时间', ascending=[False])
    all_performance = all_performance.drop(['入职时间'], axis=1)
    all_performance_df = pd.merge(all_performance, matching_df[['链接', '匹配到的申请表中人员']], how='left', on='链接')
    all_performance_df = all_performance_df.rename(columns={'匹配到的申请表中人员': '申请者&创始人'})
    all_performance_df = all_performance_df[
        ['产品名称', '申请者&创始人', '链接', '开表时间', '提交时间', '推荐人', '核心创始人画像', '一句话概括', '姓名',
        '人脉中关联上级', '产出归属小组']]
    all_performance_df_limited_view = all_performance_df[
        ['产品名称', '申请者&创始人', '提交时间', '推荐人', '姓名', '人脉中关联上级', '产出归属小组']]
    all_performance_df_limited_view = all_performance_df_limited_view.rename(columns={'姓名': '负责人姓名'})

    # 从每日产出总表中筛选出校友裂变项目
    # (1)产出归属小组为“校友裂变”
    # (2)推荐人是校友（特殊情况：校友“王雪”会匹配到intern“王雪健”和“王雪晨”，需去除）
    # (3)特殊情况：奇绩负责人是校友
    xiaoyou_list = pd.read_excel(r'校友名单.xlsx', sheet_name=0, header=None)
    xiaoyou_list = xiaoyou_list.iloc[:, 0].tolist()
    xiaoyouliebian_df1 = all_performance[all_performance.产出归属小组 == '校友裂变']
    xiaoyouliebian_df2 = all_performance[all_performance.推荐人.str.contains('|'.join(xiaoyou_list), na=False)]
    #筛选出“推荐人”中包含校友名单中任意一个名字的数据，赋值给xiaoyouliebian_df2变量。
    xiaoyouliebian_df2 = xiaoyouliebian_df2[~xiaoyouliebian_df2.推荐人.str.contains('王雪健|王雪晨|陈悦韬')]
    #删除“推荐人”中包含“王雪健”、“王雪晨”或“陈悦韬”的数据
    xiaoyouliebian_df2 = xiaoyouliebian_df2.reset_index()
    for i in range(len(xiaoyouliebian_df2)):
        name = []
        line = xiaoyouliebian_df2.loc[i, '推荐人']
        poss = pseg.cut(line)
        for j in poss:
            if j.flag == 'nr':
                name.append(j.word)
        if name == []:
            pass
        elif name[0] not in xiaoyou_list:
            xiaoyouliebian_df2.loc[i, 'flag'] = '否'
            indexNames = xiaoyouliebian_df2[xiaoyouliebian_df2.flag == '否'].index
    xiaoyouliebian_df2.drop(indexNames, inplace=True)
    # xiaoyouliebian_df2 = xiaoyouliebian_df2[~xiaoyouliebian_df2.flag=='否']
    #xiaoyouliebian_df2 = xiaoyouliebian_df2.drop(['index', 'flag'], axis=1)
    xiaoyouliebian_df2 = xiaoyouliebian_df2.drop(['index', ], axis=1)
    # %%
    # xiaoyouliebian_df3 = all_performance[all_performance.姓名.isin(['金赢', '陈龙博', '徐律涛', '陈龙', '庞舜心'])]
    xiaoyouliebian_df = pd.concat([xiaoyouliebian_df1, xiaoyouliebian_df2], axis=0)
    # xiaoyouliebian_df = pd.concat([xiaoyouliebian_df, xiaoyouliebian_df3], axis=0)
    xiaoyouliebian_df = xiaoyouliebian_df.drop_duplicates()#删除重复行数据
    xiaoyouliebian_df = xiaoyouliebian_df.sort_values(by='提交时间', ascending=[False])

    alum_list = [name.strip() for name in xiaoyou_list]
    alum_referral_dict = {}
    for name in alum_list:
        alum_referral_dict[name] = 0
    for referer_name in performance_df['推荐人'].tolist():
        for key, value in alum_referral_dict.items():
            if isinstance(referer_name, str) and (key in referer_name or referer_name in key):
                alum_referral_dict[key] = value + 1
    alum_referral_df = pd.DataFrame(columns=['校友', '推荐项目数量'])
    for key, value in alum_referral_dict.items():
        alum_referral_df.loc[len(alum_referral_df.index)] = [key, value]
    alum_referral_df.to_excel('数据看板/校友推荐提交量.xlsx', index=False, engine='xlsxwriter')

    # 计算每日提交的校友裂变项目
    xiaoyouliebian_count = pd.DataFrame(columns=['日期', '提交数'])
    xiaoyouliebian_count['日期'] = datelist
    for i in range(len(xiaoyouliebian_count)):
        xiaoyouliebian_count.loc[i, '提交数'] = len(
            xiaoyouliebian_df[xiaoyouliebian_df.提交时间 == xiaoyouliebian_count.loc[i, '日期']])
    xiaoyouliebian_count = xiaoyouliebian_count[['日期', '提交数']]
    xiaoyouliebian_count = xiaoyouliebian_count.sort_values(by='日期', ascending=[False])
    xiaoyouliebian_count = xiaoyouliebian_count.rename(columns={'提交数': '校友裂变（人脉+推荐人）'})



    from_112_all_format = ['F23总产出', '总数', 'CCC', 'NEXT', 'UNIX', 'Jedi', 'Hermit', 'Global I', 'Global Ⅱ',
                        'Shawn Pang', 'PTA', 'Fellow', 'Inbound', 'Scout', 'Core Team', 'E3C', '校友裂变']
    performance_112 = all_performance[all_performance.提交时间.isin(datelist)]
    all_count_112 = ['3.1起总产出',
                    len(performance_112),
                    len(performance_112[performance_112.产出归属小组 == 'CCC']),
                    len(performance_112[performance_112.产出归属小组 == 'NEXT']),
                    len(performance_112[performance_112.产出归属小组 == 'UNIX']),
                    len(performance_112[performance_112.产出归属小组 == 'Jedi']),
                    len(performance_112[performance_112.产出归属小组 == 'Hermit']),
                    len(performance_112[performance_112.产出归属小组 == 'Global I']),
                    len(performance_112[performance_112.产出归属小组 == 'Global Ⅱ']),
                    len(performance_112[performance_112.产出归属小组 == 'Shawn Pang']),
                    len(performance_112[performance_112.产出归属小组 == 'PTA']),
                    len(performance_112[performance_112.产出归属小组 == 'Fellow']),
                    len(performance_112[performance_112.产出归属小组 == 'Inbound']),
                    len(performance_112[performance_112.产出归属小组 == 'Scout']),
                    len(performance_112[performance_112.产出归属小组 == 'Core Team']),
                    len(performance_112[performance_112.产出归属小组 == 'E3C']),
                    len(performance_112[performance_112.产出归属小组 == '校友裂变'])]
    N = all_count_112[1] - sum(all_count_112[2:])
    all_count_112[12] = all_count_112[12] + N  # 处理Inbound
    from_112_all_df = pd.DataFrame(columns=from_112_all_format)
    from_112_all_df.loc[0] = all_count_112

    # 6、部门每日产出数
    performance_112_statis = pd.DataFrame(
        columns=['日期', '总数', 'CCC', 'NEXT', 'UNIX', 'Jedi', 'Hermit', 'Global I', 'Global Ⅱ', 'Shawn Pang',
                'PTA', 'Fellow', 'Inbound', 'Scout', 'Core Team', 'E3C', '校友裂变'])
    for i in range(len(datelist)):
        performance_perday = all_performance[all_performance.提交时间 == datelist[i]]
        statis_count = [datelist[i],
                        len(performance_perday),
                        len(performance_perday[performance_perday.产出归属小组 == 'CCC']),
                        len(performance_perday[performance_perday.产出归属小组 == 'NEXT']),
                        len(performance_perday[performance_perday.产出归属小组 == 'UNIX']),
                        len(performance_perday[performance_perday.产出归属小组 == 'Jedi']),
                        len(performance_perday[performance_perday.产出归属小组 == 'Hermit']),
                        len(performance_perday[performance_perday.产出归属小组 == 'Global I']),
                        len(performance_perday[performance_perday.产出归属小组 == 'Global Ⅱ']),
                        len(performance_perday[performance_perday.产出归属小组 == 'Shawn Pang']),
                        len(performance_perday[performance_perday.产出归属小组 == 'PTA']),
                        len(performance_perday[performance_perday.产出归属小组 == 'Fellow']),
                        len(performance_perday[performance_perday.产出归属小组 == 'Inbound']),
                        len(performance_perday[performance_perday.产出归属小组 == 'Scout']),
                        len(performance_perday[performance_perday.产出归属小组 == 'Core Team']),
                        len(performance_perday[performance_perday.产出归属小组 == 'E3C']),
                        len(performance_perday[performance_perday.产出归属小组 == '校友裂变'])]
        N = statis_count[1] - sum(statis_count[2:])
        statis_count[12] = statis_count[12] + N
        performance_112_statis.loc[i] = statis_count
    performance_112_statis = performance_112_statis.sort_values(by='日期', ascending=[False])

    # 7、部门产出画像
    portrait_format = ['日期', '总数', 'CCC', 'Ethan-全职创业', 'Ethan-非全职创业',
                    'Watson-在读学生', 'Watson-非在读学生', 'Jack-企业在职', 'Jack-非企业在职',
                    'Elaine', 'Scout-在读学生', 'Scout-非在读学生',
                    'E3C', 'Fellow', 'PTA', 'Global I', 'Global Ⅱ', 'Shawn Pang', 'Huangqi', 'Inbound', 'Core Team']
    portrait_df = pd.DataFrame(columns=portrait_format)
    l1 = ['full_time_entrepreneur']
    l2 = ['on_campus__master', 'on_campus__undergraduate_or_high_school']
    l3 = ['employed__corporate']
    l4 = ['employed__research_institution', 'on_campus__phd', 'on_campus__master']  # MiracleX的画像
    for i in range(len(datelist)):
        performance_perday = all_performance[all_performance.提交时间 == datelist[i]]
        portrait_count = [datelist[i],
                        len(performance_perday),
                        len(performance_perday[performance_perday.产出归属小组 == 'CCC']),
                        len(performance_perday[
                                (performance_perday.产出归属小组 == 'NEXT') & performance_perday.核心创始人画像.isin(
                                    l1)]),
                        len(performance_perday[
                                (performance_perday.产出归属小组 == 'NEXT') & ~performance_perday.核心创始人画像.isin(
                                    l1)]),
                        len(performance_perday[
                                (performance_perday.产出归属小组 == 'UNIX') & performance_perday.核心创始人画像.isin(
                                    l2)]),
                        len(performance_perday[
                                (performance_perday.产出归属小组 == 'UNIX') & ~performance_perday.核心创始人画像.isin(
                                    l2)]),
                        len(performance_perday[
                                (performance_perday.产出归属小组 == 'Jedi') & performance_perday.核心创始人画像.isin(
                                    l3)]),
                        len(performance_perday[
                                (performance_perday.产出归属小组 == 'Jedi') & ~performance_perday.核心创始人画像.isin(
                                    l3)]),

                        len(performance_perday[performance_perday.产出归属小组 == 'Hermit']),
                        len(performance_perday[
                                (performance_perday.产出归属小组 == 'Scout') & performance_perday.核心创始人画像.isin(
                                    l2)]),
                        len(performance_perday[
                                (performance_perday.产出归属小组 == 'Scout') & ~performance_perday.核心创始人画像.isin(
                                    l2)]),
                        len(performance_perday[performance_perday.产出归属小组 == 'E3C']),
                        len(performance_perday[performance_perday.产出归属小组 == 'Fellow']),
                        len(performance_perday[performance_perday.产出归属小组 == 'PTA']),
                        len(performance_perday[performance_perday.产出归属小组 == 'Global I']),
                        len(performance_perday[performance_perday.产出归属小组 == 'Global Ⅱ']),
                        len(performance_perday[performance_perday.产出归属小组 == 'Shawn Pang']),
                        len(performance_perday[performance_perday.产出归属小组 == '校友裂变']),
                        len(performance_perday[performance_perday.产出归属小组 == 'Inbound']),
                        len(performance_perday[performance_perday.产出归属小组 == 'Core Team'])]
        N = portrait_count[1] - sum(portrait_count[2:])
        portrait_count[19] = portrait_count[19] + N
        portrait_df.loc[i] = portrait_count
    portrait_df = portrait_df.sort_values(by='日期', ascending=[False])

    performance_112_statis = pd.merge(performance_112_statis, xiaoyouliebian_count, how='left', on='日期')






    path2 = '数据看板/F23每日提交名单.xlsx'
    with pd.ExcelWriter(path2) as writer:
        from_112_all_df.to_excel(writer, sheet_name='F23总', index=False, engine='xlsxwriter')
        performance_112_statis.to_excel(writer, sheet_name='每日部门提交', index=False, engine='xlsxwriter')
        renmai_df.to_excel(writer, sheet_name='每日部门人脉新增', engine='xlsxwriter')
        kaibiao_df.to_excel(writer, sheet_name='每日部门开表', engine='xlsxwriter')
        all_performance_df.to_excel(writer, sheet_name='每日提交', index=False, engine='xlsxwriter')
        xiaoyouliebian_df.to_excel(writer, sheet_name='校友裂变', index=False, engine='xlsxwriter')


    # 1、个人F23总产出
    employee_df = department_df[department_df.是否追产出 == '是']
    employee_df = employee_df.reset_index()
    F23_employee_performance = employee_df.drop(['产出归属小组', '是否追产出'], axis=1)
    F23_employee_performance['F23总提交数'] = [0] * len(F23_employee_performance)
    for i in range(len(F23_employee_performance)):
        name = F23_employee_performance.loc[i, '姓名']
        F23_employee_performance.loc[i, 'F23总提交数'] = len(all_performance[all_performance.姓名 == name])
    F23_employee_performance = F23_employee_performance[['姓名', 'F23总提交数', '人脉中关联上级']]

    # 2、每日绩效
    employee_performance = pd.DataFrame(columns=['日期', '姓名', '提交数', '人脉中关联上级'])
    for i in tqdm(range(len(datelist))):
        df = employee_df.drop(['产出归属小组', '是否追产出'], axis=1)
        df['提交数'] = [0] * len(df)
        df['日期'] = datelist[i]
        df = df[['日期', '姓名', '提交数', '人脉中关联上级']]
        pdf = all_performance[all_performance.提交时间 == datelist[i]]
        for k in range(len(df)):
            for j in range(len(pdf)):
                if type(pdf.loc[pdf.index[j], '姓名']) != float:
                    if pdf.loc[pdf.index[j], '姓名'] == str(df.loc[k, '姓名']):
                        if pdf.loc[pdf.index[j], '提交时间'] == str(df.loc[k, '日期']):
                            df.loc[k, '提交数'] += 1
        employee_performance = pd.concat([employee_performance, df], axis=0)
    employee_performance = employee_performance.sort_values(by=['日期', '人脉中关联上级', '姓名'])
    pdpper_all_except_tijiao_df = pdpper_all_except_tijiao_df.drop(['人脉中关联上级'], axis=1)
    employee_performance = pd.merge(employee_performance, pdpper_all_except_tijiao_df, how='inner', on=['日期', '姓名'])
    employee_performance = employee_performance.rename(columns={'自增总量': '人脉自增数', '开表增量': '开表数'})
    employee_performance = employee_performance[
        ['日期', '姓名', '提交数', '人脉增量', '人脉自增数', '开表数', '人脉中关联上级']]

    # 3、补全个人F23总产出（加上了人脉总量、自增人脉总量和总开表数）
    F23_employee_performance = pd.merge(F23_employee_performance, employee_performance.groupby('姓名').sum(), how='inner',
                                        on='姓名')
    F23_employee_performance = F23_employee_performance.rename(
        columns={'人脉增量': '人脉总量', '人脉自增数': '自增人脉总量', '开表数': '总开表数', '人脉中关联上级_x': '人脉中关联上级'})
    F23_employee_performance = F23_employee_performance[
        ['姓名', 'F23总提交数', '人脉总量', '自增人脉总量', '总开表数', '人脉中关联上级']]

    # 3、每周绩效表
    weekly_employee_performance = employee_performance[['日期', '姓名', '提交数', '人脉中关联上级']]
    weekly_employee_performance.loc[:, '日期'] = pd.to_datetime(weekly_employee_performance['日期'])
    weekly_employee_performance = weekly_employee_performance.set_index('日期')
    weekly_employee_performance = weekly_employee_performance.groupby('姓名')
    weekly_employee_performance = weekly_employee_performance.resample('w').sum()
    weekly_employee_performance

    order = ['提交数']
    weekly_employee_performance = weekly_employee_performance[order]
    weekly_employee_performance['周（周最后一日）'] = weekly_employee_performance.index.get_level_values(1)
    weekly_employee_performance['姓名'] = weekly_employee_performance.index.get_level_values(0)

    weekly_employee_performance.index = weekly_employee_performance.index.droplevel(1)  # 删除指定等级的索引
    weekly_employee_performance = weekly_employee_performance.reset_index(drop=True)
    weekly_employee_performance = pd.merge(weekly_employee_performance, employee_df, how='left', on='姓名')
    order = ['姓名', '周（周最后一日）', '提交数', '人脉中关联上级']
    weekly_employee_performance = weekly_employee_performance[order]
    weekly_employee_performance = weekly_employee_performance.sort_values(by=['人脉中关联上级', '姓名'])
    weekly_employee_performance['周（周最后一日）'] = weekly_employee_performance['周（周最后一日）'].astype('str')

    # 上面算的是提交数
    # 下面算的是另外三个

    weekly_employee_performance_2 = employee_performance
    weekly_employee_performance_2['日期'] = pd.to_datetime(weekly_employee_performance_2['日期'])
    weekly_employee_performance_2 = weekly_employee_performance_2.set_index('日期')
    weekly_employee_performance_2 = weekly_employee_performance_2.groupby('姓名')
    weekly_employee_performance_2 = weekly_employee_performance_2.resample('w').sum()

    weekly_employee_performance_2['周（周最后一日）'] = weekly_employee_performance_2.index.get_level_values(1)
    weekly_employee_performance_2['姓名'] = weekly_employee_performance_2.index.get_level_values(0)
    weekly_employee_performance_2.index = weekly_employee_performance_2.index.droplevel(1)  # 删除指定等级的索引
    weekly_employee_performance_2 = weekly_employee_performance_2.reset_index(drop=True)
    weekly_employee_performance_2 = weekly_employee_performance_2[
        ['姓名', '周（周最后一日）', '人脉增量', '人脉自增数', '开表数']]

    weekly_employee_performance = pd.concat(
        [weekly_employee_performance, weekly_employee_performance_2[['人脉增量', '人脉自增数', '开表数']]], axis=1)
    weekly_employee_performance = weekly_employee_performance[
        ['姓名', '周（周最后一日）', '提交数', '人脉增量', '人脉自增数', '开表数', '人脉中关联上级']]

    weeklist = list(weekly_employee_performance['周（周最后一日）'])
    weekdic = {}
    weekdic = weekdic.fromkeys(weeklist).keys()
    weeklist = list(weekdic)
    weeklist.sort()

    # 4、每周0产出表
    production_0_df = weekly_employee_performance[weekly_employee_performance.提交数 == 0]

    # 5、连续两周0产出表
    if date.today().weekday() == 0:
        weeklist = weeklist[-2:]
    else:
        weeklist = weeklist[-3:-1]
    warning_df = production_0_df[production_0_df['周（周最后一日）'].isin(weeklist)]
    namelist = list(warning_df['姓名'])
    namelist = [i for i in namelist if namelist.count(i) == 2]
    namelist = list(set(namelist))
    warning_df = warning_df[warning_df.姓名.isin(namelist)]
    warning_df = warning_df.drop(columns=['周（周最后一日）'])
    warning_df = warning_df.drop_duplicates('姓名', keep='first', inplace=False)
    warning_df['备注'] = np.nan

    path3 = '数据看板/F23个人绩效check1.0.xlsx'
    with pd.ExcelWriter(path3) as writer:
        F23_employee_performance.to_excel(writer, sheet_name='F23个人产出总数', index=False, engine='xlsxwriter')
        all_performance_df_limited_view.to_excel(writer, sheet_name='F23总提交明细', index=False, engine='xlsxwriter')
        
        
    # 6、导出小组长看板
    path3 = '数据看板/F23产出小组长看板.xlsx'
    with pd.ExcelWriter(path3) as writer:
        employee_performance['日期'] = employee_performance['日期'].astype('str')
        employee_performance.to_excel(writer, sheet_name='每日绩效', index=False, engine='xlsxwriter')
        weekly_employee_performance.to_excel(writer, sheet_name='每周绩效', index=False, engine='xlsxwriter')
        F23_employee_performance.to_excel(writer, sheet_name='F23总绩效', index=False, engine='xlsxwriter')
        all_performance_df_limited_view.to_excel(writer, sheet_name='F23总提交明细', index=False, engine='xlsxwriter')
        production_0_df.to_excel(writer, sheet_name='每周0产出名单', index=False, engine='xlsxwriter')
        warning_df.to_excel(writer, sheet_name='连续两周0产出名单', index=False, engine='xlsxwriter')
