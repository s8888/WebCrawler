#!/usr/bin/env python
# coding: utf-8
'''
Project Name: moj project
Crawl Cycle: Daily
Main Website: http://www.ba.org.tw/PublicInformation/PublicinfoAll
author      : 林德昌
update      : 2020/02/14
date        : 2018/12/17
description : 抓取銀行公會每天發布的最新消息
'''


import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(sys.argv[0]))))
from InvestCommon import header

import logging
import traceback # 印log
import datetime
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup


TODAY = str(datetime.date.today())
WEB_URL = "http://www.ba.org.tw/PublicInformation/PublicinfoAll"
CHECK_RANGE = 7 #爬網日期區間為前7日
FINAL_PATH = header.FINAL_PATH #爬網結果路徑
LAST_RESULT_PATH = header.LAST_RESULT_PATH #爬網歷史紀錄
FIX_URL = 'https://www.ba.org.tw'


def compareTo(strDate, endDate):
    strDate = re.sub(r'(/|-|\.)', '-', strDate)
    endDate = re.sub(r'(/|-|\.)', '-', endDate)
    if int(re.split('-', strDate)[0]) < 1911:
        strDate = datetime.datetime.strptime(str(int(re.sub('-', '', strDate)) + 19110000), "%Y%m%d").strftime("%Y-%m-%d")
    if int(re.split('-', endDate)[0]) < 1911:
        endDate = datetime.datetime.strptime(str(int(re.sub('-', '', endDate)) + 19110000), "%Y%m%d").strftime("%Y-%m-%d")
    try:
        strDate = datetime.datetime.strptime(strDate, "%Y-%m-%d")
        endDate = datetime.datetime.strptime(endDate, "%Y-%m-%d")

    except:
        logging.error('compareTo(strDate, endDate):')
        logging.error("日期格式錯誤：strDate = %s, endDate = %s" %(strDate, endDate))
        traceback.print_exc()
        return
    if strDate < endDate:
        return -1
    elif strDate == endDate:
        return 0
    else:
        return 1

def parsingTitle(strDate, endDate):

    try:
        # 取得上次爬網結果
        if os.path.isfile(LAST_RESULT_PATH):
            lastResult = pd.read_csv(LAST_RESULT_PATH, encoding='utf8')
        else:
            lastResult = pd.DataFrame()
        header.lastResult = lastResult

        # 網站網址, 爬網日期, 發文日期, 標題, 內文連結
        df = pd.DataFrame(columns=['WEB_ADDR', 'CRL_DATE', 'ISS_DATE', 'TITL', 'LNK_URL'])

        # 資料處理
        nowPage = 1
        iss_date_check = True
        while iss_date_check:
            try:
                soup = request2soup(WEB_URL)

                rows = soup.findAll('tr', { 'class' : 'table_BG_even' })
                for row in rows:

                    col_data = row.findAll('td')

                    # 檢核最新文章的日期，是否在此次爬網區間的範圍
                    iss_date = col_data[0].string
                    if compareTo(iss_date, strDate) < 0:
                        iss_date_check = False
                        break

                    lnk = col_data[1].find('a')

                    lnk_url = FIX_URL + lnk['href']
                    lnk_titl = lnk['title']

                    d = {
                        'WEB_ADDR' : WEB_URL,
                        'CRL_DATE' : TODAY,
                        'ISS_DATE' : iss_date,
                        'TITL' : lnk_titl,
                        'LNK_URL' : lnk_url
                    }

                    df = df.append(d, ignore_index=True)

                # 換下一頁
                iss_date_check = False
                #nowPage = nowPage + 1
            except:
                header.EXIT_CODE = -1
                logging.error("爬取第 %s 頁主旨發生錯誤" % (nowPage))
                logging.error(str(traceback.format_exc()))

        # 確認查詢區間是否有資料
        if len(df) > 0:
            # 重設每筆資料的索引值
            df.index = [i for i in range(df.shape[0])]

            # 以發文時間與標題，確認是否之前已爬取文章
            if not lastResult.empty:
                LAST_ISS_DATE = list(lastResult['ISS_DATE'])
                LAST_TITL = list(lastResult['TITL'])
                for index, row in df.iterrows():
                    if (row['ISS_DATE'] in LAST_ISS_DATE) and (row['TITL'] in LAST_TITL):
                        df.drop(index, inplace=True)

    except:
        header.EXIT_CODE = -1
        logging.error("爬取主旨列表失敗")
        logging.error(str(traceback.format_exc()))
    return df

def dataProcess_Detail(soup):
    result = dict()
    #fileUrlroot = 'https://www.ba.org.tw'
    result['fileUrls'] = [FIX_URL + e.get('href') for e in soup.select('.main_Content_downloadList a')]
    result['fileNames'] = [e.text for e in soup.select('.main_Content_downloadList a')]
    #result['issue_date'] = [e.text for e in soup.select('.main_Content_day')][0]
    return result

def parsingDetail(df, finalPath=FINAL_PATH):

    # 發文日期, 發文字號, 標題, 本文, 相關法條, 附件, 附件存放資料夾, 附件存檔名稱
    df2 = pd.DataFrame(columns=["ISS_DATE", "ISS_NO", "TITL", "ISS_CTNT", "RLT_RGL", "FILES", "FOLDER_NM", "FILES_NM"])

    # 第一層結果：網站網址, 爬網日期, 發文日期, 標題, 內文連結, 標題種類
    for index, row in df.iterrows():
        try:
            iss_date = row['ISS_DATE']
            title = row['TITL']
            link = row['LNK_URL']

            # 取得內文
            soup = request2soup(link)
            result = dataProcess_Detail(soup)

            # 下載內文附件
            FILES_NM = result['fileNames']
            FOLDER_NM = ''
            if len(FILES_NM) != 0:
                iss_date = re.sub(r'(/|-|\.)', '-', iss_date)
                FOLDER_NM = iss_date + '_' + title[:30].strip() + '_' + str(index) # 有附檔才會有資料夾名稱
                header.downloadFile(FOLDER_NM, finalPath, result['fileUrls'], FILES_NM)

            folder_nm = FOLDER_NM
            files_nm = ','.join(FILES_NM)

            d = {
                'ISS_DATE' : iss_date,
                'TITL' : title,
                'ISS_CTNT' : '',
                'ISS_NO' : '',
                'RLT_RGL' : '',
                'FILES' : files_nm,
                'FOLDER_NM' : folder_nm,
                'FILES_NM' : files_nm
            }

            df2= df2.append(d, ignore_index=True)

        except:
            header.EXIT_CODE = -1
            logging.error("爬取內文失敗")
            logging.error("失敗連結：" + str(link)) # 避免 unicode 編碼錯誤，將 link 轉為字串
            logging.error(str(traceback.format_exc()))
    return df2

def request2soup(url):
    res = requests.get(url)
    res.encoding = 'utf-8'
    soup = BeautifulSoup(res.text, 'html.parser', from_encoding='utf-8')
    return soup

def main(checkRange=30):
    header.processBegin(url=WEB_URL)
    header.clearFolder()

    try:
        # 爬網日期區間
        endDate = datetime.date.today()
        strDate = (endDate - datetime.timedelta(days=checkRange)).isoformat()
        df_1 = parsingTitle(strDate, endDate)

        # 確認是否有新資料待爬取
        RESULT_COUNT = len(df_1)
        if RESULT_COUNT < 1 :
            logging.critical("%s 至 %s 間無資料更新" % (strDate, endDate))
        else:
            header.outputCsv(df_1, "第一層結果", FINAL_PATH)

            df_2 = parsingDetail(df_1)
            header.outputCsv(df_2, "第二層結果", FINAL_PATH)

            header.RESULT_COUNT = RESULT_COUNT

            # 更新 crawlHistory 檔案
            header.outputLastResult(df_1, header.lastResult, checkRange)

        header.zipFile()
        header.createInfoFile()
        header.createOKFile()
    except:
        header.EXIT_CODE = -1
        logging.error("執行爬網作業失敗")
        logging.error(str(traceback.format_exc()))

    header.processEnd()

if __name__ == "__main__":
    main(checkRange=CHECK_RANGE)
