#!/usr/bin/env python
# coding: utf-8
'''
Project Name: lia-roc project
Crawl Cycle: Daily
Main Website: http://www.lia-roc.org.tw/index06.asp?mu=show1
author      : 林德昌
date        : 2019/01/10
update      : 2020/02/17
description : 抓取壽險公會每天發布的最新消息
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
WEB_URL = "http://www.lia-roc.org.tw/index06/regulation/109regu.htm"
CHECK_RANGE = 7 #爬網日期區間為前7日
FINAL_PATH = header.FINAL_PATH #爬網結果路徑
LAST_RESULT_PATH = header.LAST_RESULT_PATH #爬網歷史紀錄
FIX_URL = "http://www.lia-roc.org.tw/index06/regulation/"


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
        strYear = int(str(strDate).split('-')[0]) - 1911
        endYear = int(str(endDate).split('-')[0]) - 1911

        for intYear in range(strYear, endYear+1):

            try:
                year = str(intYear)
                url = 'http://www.lia-roc.org.tw/index06/regulation/' + year + 'regu.htm'
                soup = request2soup(url)

                rows = soup.findAll('a')
                for row in rows:

                    lnk_url = row['href']
                    if (year + 'regu') in lnk_url:

                        d = {
                            'WEB_ADDR' : url,
                            'CRL_DATE' : TODAY,
                            'ISS_DATE' : '',
                            'TITL' : str(row.text),
                            'LNK_URL' : FIX_URL + lnk_url
                        }

                        df = df.append(d, ignore_index=True)

            except:
                header.EXIT_CODE = -1
                logging.error("爬取第 %s 年主旨發生錯誤" % (year))
                logging.error(str(traceback.format_exc()))

        # 確認查詢區間是否有資料
        if len(df) > 0:
            # 重設每筆資料的索引值
            df.index = [i for i in range(df.shape[0])]

            # 以發文時間與標題，確認是否之前已爬取文章
            if not lastResult.empty:
                LAST_TITL = list(lastResult['TITL'])
                for index, row in df.iterrows():
                    if row['TITL'] in LAST_TITL:
                        df.drop(index, inplace=True)

    except:
        header.EXIT_CODE = -1
        logging.error("爬取主旨列表失敗")
        logging.error(str(traceback.format_exc()))
    return df

def parsingDetail(df, finalPath=FINAL_PATH):

    # 發文日期, 發文字號, 標題, 本文, 相關法條, 附件, 附件存放資料夾, 附件存檔名稱
    df2 = pd.DataFrame(columns=["ISS_DATE", "ISS_NO", "TITL", "ISS_CTNT", "RLT_RGL", "FILES", "FOLDER_NM", "FILES_NM"])

    # 第一層結果：網站網址, 爬網日期, 發文日期, 標題, 內文連結, 標題種類
    for index, row in df.iterrows():
        try:
            title = row['TITL']
            link = row['LNK_URL']

            # 下載內文附件
            FOLDER_NM = title[:30].strip() # 有附檔才會有資料夾名稱
            file_name = FOLDER_NM + '.pdf'
            header.downloadFile(FOLDER_NM, finalPath, [link], [file_name])

            d = {
                'ISS_DATE' : '',
                'TITL' : title,
                'ISS_CTNT' : '',
                'ISS_NO' : '',
                'RLT_RGL' : '',
                'FILES' : file_name,
                'FOLDER_NM' : FOLDER_NM,
                'FILES_NM' : file_name
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
    res.encoding = 'big5'
    soup = BeautifulSoup(res.text, 'html.parser', from_encoding='big5')
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
