#!/usr/bin/env python
# coding: utf-8
'''
Project Name: bankingGov project
Crawl Cycle: Daily
Main Website: https://www.banking.gov.tw/ch/home.jsp?id=190&parentpath=0,3&mcustomize=
author      : 林德昌
update      : 2020/02/11
date        : 2018/10/16
description : 抓取金管會銀行局每天發布的最新法令函釋
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
WEB_URL = "https://www.banking.gov.tw/ch/home.jsp?id=190&parentpath=0,3&mcustomize=lawnew_list.jsp"
CHECK_RANGE = 7 #爬網日期區間為前7日
FINAL_PATH = header.FINAL_PATH #爬網結果路徑
LAST_RESULT_PATH = header.LAST_RESULT_PATH #爬網歷史紀錄
FIX_URL = "https://www.banking.gov.tw"


def compareTo(strDate, endDate):
    if int(re.split(r'(/|-|\.)', strDate)[0]) < 1911:
        strDate = datetime.datetime.strptime(str(int(re.sub(r'(/|-|\.)', '', strDate)) + 19110000), "%Y%m%d").strftime("%Y-%m-%d")
    if int(re.split(r'(/|-|\.)', endDate)[0]) < 1911:
        endDate = datetime.datetime.strptime(str(int(re.sub(r'(/|-|\.)', '', endDate)) + 19110000), "%Y%m%d").strftime("%Y-%m-%d")
    try:
        strDate = datetime.datetime.strptime(strDate, "%Y-%m-%d")
        endDate = datetime.datetime.strptime(endDate, "%Y-%m-%d")

    except:
        logging.error('compareTo(strDate, endDate):')
        logging.error("日期格式錯誤：strDate = %s, endDate = %s" %(strDate, endDate))
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

        # 網站網址, 爬網日期, 發文日期, 標題, 內文連結, 類型
        df = pd.DataFrame(columns=['WEB_ADDR', 'CRL_DATE', 'ISS_DATE', 'TITL', 'LNK_URL', 'TYPE'])

        # 資料處理
        nowPage = 1
        iss_date_check = True
        while iss_date_check:
            try:
                soup = request2soup(WEB_URL, page=nowPage)
                rows = soup.findAll('div', { 'class' : 'whitebackground' })

                for row in rows:

                    # 檢核最新文章的日期，是否在此次爬網區間的範圍
                    iss_date = row.find('div', { 'class' : 'pdate1' }).string
                    if compareTo(iss_date, strDate) < 0:
                        iss_date_check = False
                        break

                    lnk = row.find('div', { 'class' : 'ptitle1' }).find('a')

                    lnk_url = str(lnk['href'])

                    if "uploaddowndoc" in lnk_url:
                        lnk_url = FIX_URL + lnk_url
                        lnk_type = "file"
                    elif lnk_url[:8] == "home.jsp":
                        lnk_url = FIX_URL + "/ch/" + lnk_url
                        lnk_type = "article"
                    else:
                        lnk_type = "unknown"

                    d = {
                        'WEB_ADDR' : WEB_URL,
                        'CRL_DATE' : TODAY,
                        'ISS_DATE' : iss_date,
                        'TITL' : lnk.string,
                        'LNK_URL' : lnk_url,
                        'TYPE' : lnk_type
                    }

                    df = df.append(d, ignore_index=True)

                # 換下一頁
                nowPage = nowPage + 1

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

def dataProcess_Detail(soup, fileUrlRoot):
    result = dict()
    result['content'] = [header.spaceAndWrapProcess(e.text) for e in soup.select('.page_content')][0]
    result['FILES'] = [e.text for e in soup.select('.acces a')]
    FILES_NM = [os.path.splitext(ele)[0][:30] + os.path.splitext(ele)[1] for ele in result['FILES']]
    result['FILES_NM'] = header.processDuplicateFiles(FILES_NM)
    
    #logging.info(result['FILES_NM'])
    result['fileUrls'] = [fileUrlRoot + e.get('href') for e in soup.select('.acces a')]
    str_content = str(soup.select('.page_content')[0])
    result['serno'] = re.findall(r'發文字號.+?\d+.+?', str_content)[0][5:]
    result['issue_date'] = re.findall(r'發文日期.+?日', str_content)[0][5:]
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

            iss_no = ''
            iss_ctnt = ''
            folder_nm = ''
            files = ''
            files_nm = ''

            # 檢查文章類型
            lnk_type = row['TYPE']
            if lnk_type == "file":
                folder_name = iss_date + '_' + title[:30]
                file_name = re.findall(r'filedisplay=\w+\.pdf', link)[0][len('filedisplay='):]
                header.downloadFile(folder_name, finalPath, [link], [file_name])

                folder_nm = folder_name
                files = file_name
                files_nm = file_name
            
            elif lnk_type == "article":

                # 取得內文
                soup = request2soup(link)
                result = dataProcess_Detail(soup, FIX_URL)

                # 下載內文附件
                FILES = result['FILES']
                FILES_NM = result['FILES_NM']
                FOLDER_NM = ''
                if len(FILES_NM) != 0:
                    iss_date = re.sub(r'(/|-|\.)', '-', iss_date)
                    FOLDER_NM = iss_date + '_' + title[:30].strip() + '_' + str(index) # 有附檔才會有資料夾名稱
                    header.downloadFile(FOLDER_NM, finalPath, result['fileUrls'], FILES_NM)

                iss_no = result['serno']
                iss_ctnt = result['content']
                folder_nm = FOLDER_NM
                files = ','.join(FILES)
                files_nm = ','.join(FILES_NM)

            d = {
                'ISS_DATE' : iss_date,
                'TITL' : title,
                'ISS_CTNT' : iss_ctnt,
                'ISS_NO' : iss_no,
                'RLT_RGL' : '',
                'FILES' : files,
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

def request2soup(url, page=1):
    url = url + "&page=" + str(page)
    res = requests.get(url)
    res.encoding = "utf-8"
    soup = BeautifulSoup(res.text, "html.parser", from_encoding="utf-8")
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
