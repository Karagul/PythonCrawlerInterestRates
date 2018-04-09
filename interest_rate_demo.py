#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import os
import datetime, time
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup

lable_list = None
value_list = None
dir_path = None
headers = {'content-type': 'application/x-www-form-urlencoded'} # necessary for new web version

def initial_action():
    global lable_list, value_list
    initial_url = "https://www.cbc.gov.tw/sp.asp?xdurl=gopher/chi/busd/bkrate/banklist1.asp&ctNode=809"
    # res = requests.get(initial_url) # old version
    payload = dict(BankGroup='銀行') # get all banks
    # print(payload)
    res = requests.post(initial_url, headers=headers, data=payload)
    res.encoding = 'utf-8' # change encoding from 'ISO-8859-1' to 'utf-8'
    soup = BeautifulSoup(res.text, "lxml")

    #show all forms, find out the index of <form name="frm1">
    # index = 0
    # for form in soup.find_all('form'):
        # print("index {0}".format(index))
        # index += 1
        # print(form)
        # print("\n\n")

    # form index 2 -> <form name="frm1" method="post" action="sp.asp?xdurl=gopher/chi/busd/bkrate/interestrate.asp&ctNode=809">
    # print(soup.find_all('form')[2])

    # extract <td> Tag
    # td_all = soup.find_all('form')[2].find('table').find('table').find_all('td') # old version
    td_all = soup.find_all('form')[2].find('table').find_all('td')
    
    # filter <label> Tag
    label_td_all = list(filter(lambda td: td.find('label'), td_all))

    # extract <label> string
    lable_list = list(map(lambda label_td: label_td.label.string, label_td_all))
    print(lable_list)
    # filter <input> Tag
    input_td_all = list(filter(lambda td: td.find('input'), td_all))

    # extract <input> value
    value_list = list(map(lambda input_td: input_td.input['value'], input_td_all))
    print(value_list)

def fill_payload(value):
    payload_dict = dict(CompanyNo=value)
    return payload_dict

def create_dir():
    now_time = datetime.datetime.now()
    dir_name = now_time.strftime("%Y%m%d_interest_rate")
    if not os.path.exists(dir_name):
        print("mkdir {0}".format(dir_name))
        os.mkdir(dir_name)
    return dir_name

def create_file_name(bank_name):
    now_time = datetime.datetime.now()
    # file_name = now_time.strftime("{0}_%Y%m%d_%H%M%S".format(bank_name)) # on Windows 10 - UnicodeEncodeError: 'locale' codec can't encode character '\u81fa' in position 0: Illegal byte sequence
    time_str = now_time.strftime("_%Y%m%d_%H%M%S")
    file_name = bank_name + time_str
    return file_name

def target_action():
    global lable_list, value_list, dir_path
    target_url = "https://www.cbc.gov.tw/sp.asp?xdurl=gopher/chi/busd/bkrate/interestrate.asp&ctNode=809"
    dir_path = create_dir()
    bank_num = 0

    # connect to all target url
    # for >>>
    for x in range(len(value_list)):
        # print("{0} payload {1} ".format(lable_list[x], fill_payload(value_list[x])))
        payload = fill_payload(value_list[x])
        # r = requests.post(target_url, data=payload) # old version
        r = requests.post(target_url, headers=headers, data=payload)
        r.encoding = 'utf-8' # change encoding from 'ISO-8859-1' to 'utf-8'

        b_soup = BeautifulSoup(r.text, "lxml")
        # print(len(b_soup.find_all('table')))
        # print(b_soup.find_all('table'))
        # table_1 = b_soup.find_all('table')[1] # method one -> find one of all
        table_1 = b_soup.find(summary="個別金融機構牌告存放利率查詢表格") # method two -> find attribute 'summary'
        tr_all = table_1.find_all('tr')
        # print(tr_all[1])
        # <tr><td align="center" colspan="3"><strong>金融機構牌告存放利率</strong></td></tr>

        title = tr_all[1].string
        # print(tr_all[2].find_all('td'))
        # [<td style="text-align:left;padding-left:5px;">金融機構：臺灣銀行</td>, <td style="text-align:left;padding-left:5px;">0040000</td>, <td style="text-align:right;padding-right:5px;">資料日期：107/04/03</td>]

        bank_info = tr_all[2].find_all('td')
        bank_title = bank_info[0].string
        bank_code = bank_info[1].string
        bank_date = bank_info[2].string
        # print(tr_all[3])
        # <tr><td colspan="3" style="text-align:right;padding-right:5px;">單位: 年息百分比率</td></tr>
        # print(tr_all[3].string)
        # 單位: 年息百分比率
        unit_rate = tr_all[3].string

        bank_name = bank_title[5:]
        column_bank_title = bank_title[0:4]
        column_bank_code = 'Bank_Code'
        column_bank_date = bank_date[0:4]
        info_date = bank_date[5:]
        column_unit = unit_rate[0:2]
        annual_rate = unit_rate[4:]
        # print(bank_name, info_date, annual_rate, column_bank_title, column_bank_code, column_bank_date, column_unit)
        # 臺灣銀行 105/08/02 年息百分比率 金融機構 Bank_Code 資料日期 單位
        
        # http://pandas.pydata.org/pandas-docs/stable/generated/pandas.read_html.html
        # Read HTML tables into a list of DataFrame objects.
        # This function will always return a list of DataFrame or it will fail, e.g., it will not return an empty list.
        tables = pd.read_html(r.text)
        # print(tables)
        data_table = tables[-1] # grab the last table
        # print(data_table)
        sql_table = data_table

        columns = sql_table.values[0,0:]
        column_list = list(columns) # transfer to list
        # print(column_list)
        # ['牌告利率項目', '牌告利率存期', '額度別', '生效日期', '固定利率', '機動利率']

        for h in [column_bank_date, column_bank_code, column_bank_title]:
            column_list.insert(0, h)

        column_list.append(column_unit)
        # print(column_list)
        # ['金融機構', 'Bank_Code', '資料日期', '牌告利率項目', '牌告利率存期', '額度別', '生效日期', '固定利率', '機動利率', '單位']

        sql_numpy = sql_table.values[1:,0:] # remove first row
        sql_df = pd.DataFrame(sql_numpy) # transfer to DataFrame
        sql_df.columns = column_list[3:9] # rename columns

        # add new columns and values
        x_list = [column_bank_title, column_bank_code, column_bank_date, column_unit]
        y_list = [bank_name, bank_code, info_date, annual_rate]
        # print(sql_df.values.shape) # e.g. (45, 6)
        # print(type(sql_df.values.shape)) # <class 'tuple'>
        # print(type(sql_df.values)) # <class 'numpy.ndarray'>
        for temp in range(4):
            sql_df[x_list[temp]] = pd.Series([y_list[temp] for i in range(sql_df.values.shape[0])], index=sql_df.index)

        # reindex columns
        sql_df = sql_df.reindex(columns=column_list)

        # combine all dataframe
        if bank_num == 0:
            total_df = sql_df
        else:
            total_df = total_df.append(sql_df)
        bank_num += 1

        # data processing and export to excel
        data_table.loc[-2] = [title, bank_title, bank_code, bank_date, unit_rate, ""] # adding a row
        data_table.loc[-1] = ["" for k in range(6)] # adding a row (blank space)
        data_table.index = data_table.index + 2  # shifting index
        # data_table = data_table.sort() # sort was deprecated for DataFrame
        data_table = data_table.sort_index() # sorting by index
        excel_file = "./{0}/{1}.xlsx".format(dir_path, create_file_name(lable_list[x]))
        print(excel_file)
        data_table.to_excel(excel_file, encoding="utf-8", header=False, index=False, sheet_name='Sheet1')
        # time.sleep(1)
    # for <<<

    return total_df
    
def export_to_sql(total_df):
    global dir_path
    # export to sqlite3 db
    interest_db_path = './{0}/interest_rate{1}.db'.format(dir_path, time.strftime("_%Y%m%d_%H%M%S", time.localtime()))
    print(interest_db_path)
    # ./20180410_interest_rate/interest_rate_20180410_013639.db

    import sqlite3
    conn = sqlite3.connect(interest_db_path)
    total_df.to_sql(name="interest_rate", con=conn, index=False, if_exists="replace")

    cursor = conn.cursor()
    sql = "SELECT * FROM interest_rate LIMIT 10"
    print('\nShow 10 records from {0} \n'.format(interest_db_path))
    for record in cursor.execute(sql):
        print(record)

    conn.close()

def main():
    initial_action()
    final_df = target_action()
    export_to_sql(final_df)

if __name__ == '__main__':
    main()
