#!/usr/bin/env python
# coding: utf-8
'''
Project Name: 法源法律網
CTM Name    : 
Crawl Cycle : Daily
Main Website: http://www.lawbank.com.tw/news/NewsSearch.aspx?TY=
Page Type   : 19-法規新訊, 20-判解新訊, 21-函釋新訊, 22-法規草案
Description : 針對法源法律網-法律新訊，每日取得法規異動資訊
Author      : 林竣昇
Update Date : 2020/02/23
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
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager


TODAY = str(datetime.date.today())
WEB_URL = "http://www.lawbank.com.tw/news/NewsSearch.aspx?TY="
PAGE_TYPE = ["19", "20", "21", "22"] # 法規新訊, 判解新訊, 函釋新訊, 法規草案
CHECK_RANGE = 7 #爬網日期區間為前7日
FINAL_PATH = header.FINAL_PATH #爬網結果路徑
LAST_RESULT_PATH = header.LAST_RESULT_PATH #爬網歷史紀錄


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

        # 網站網址, 爬網日期, 發文日期, 標題, 內文連結, 頁面種類
        df = pd.DataFrame(columns=['WEB_ADDR', 'CRL_DATE', 'ISS_DATE', 'TITL', 'LNK_URL', 'PAGE_TYPE'])

        # 資料處理
        driver = webdriver.Chrome(ChromeDriverManager().install())

        for ty in PAGE_TYPE:

            url = WEB_URL + ty
            driver.get(url)

            nowPage = 1
            iss_date_check = True
            while iss_date_check:
                try:
                    dates = driver.find_elements_by_css_selector(".tdDate")
                    dates = [x.text for x in dates]

                    titles = driver.find_elements_by_css_selector(".tdSubject")
                    titles = [x.text for x in titles]

                    links = driver.find_elements_by_css_selector(".tdSubject a")
                    links = [x.get_attribute("href") for x in links]

                    for i in range(len(titles)):

                        iss_date = dates[i]
                        lnk_titl = titles[i]
                        lnk_url = links[i]

                        # 檢核最新文章的日期，是否在此次爬網區間的範圍
                        if compareTo(iss_date, strDate) < 0:
                            iss_date_check = False
                            break

                        d = {
                            'WEB_ADDR' : url,
                            'CRL_DATE' : TODAY,
                            'ISS_DATE' : iss_date,
                            'TITL' : lnk_titl,
                            'LNK_URL' : lnk_url,
                            'PAGE_TYPE' : ty
                        }

                        df = df.append(d, ignore_index=True)

                    # 換下一頁
                    if iss_date_check:
                        driver.find_element_by_id("ctl00_cphMain_PagerTop_butNext").click()
                        nowPage += 1
                except:
                    header.EXIT_CODE = -1
                    logging.error("爬取類別: %s 第 %s 頁主旨發生錯誤" % (ty, nowPage))
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

def parsingDetail(df, finalPath=FINAL_PATH):

    # 發文日期, 發文字號, 標題, 本文, 相關法條, 附件, 附件存放資料夾, 附件存檔名稱
    df2 = pd.DataFrame(columns=["ISS_DATE", "ISS_NO", "TITL", "ISS_CTNT", "RLT_RGL", "FILES", "FOLDER_NM", "FILES_NM"])

    # 第一層結果：網站網址, 爬網日期, 發文日期, 標題, 內文連結, 標題種類
    for index, row in df.iterrows():
        try:
            iss_date = row['ISS_DATE']
            title = row['TITL']
            link = row['LNK_URL']
            page_type = row['PAGE_TYPE']

            # 取得內文
            soup = request2soup(link)

            # 全文內容
            content = soup.select("#pageNews")[0].text.strip()

            # 發文字號, 相關法條
            serialNumber = ''
            if page_type == "19":
                if bool(re.findall(r'日.+?字[\w|\s]+號', content)):    
                    serialNumber = re.sub('\s', '', re.findall(r'日.+?字[\w|\s]+號', content)[0][1:])
                relatedLaw = '|'.join([e.text for e in soup.select("#ctl00_cphMain_relaData a")])
            else:
                if bool(re.findall(r'字號.+?\d+.+?', content)):
                    serialNumber = re.findall(r'字號.+?\d+.+?', content)[0][2:]
                relatedLaw = '|'.join([e.text for e in soup.select('.pageNews-Content a')])

            d = {
                'ISS_DATE' : iss_date,
                'TITL' : title,
                'ISS_CTNT' : content,
                'ISS_NO' : serialNumber,
                'RLT_RGL' : relatedLaw,
                'FILES' : '',
                'FOLDER_NM' : '',
                'FILES_NM' : ''
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
