#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Project Name: rootlaw project
Crawl Cycle: Daily
Main Website: http://www.rootlaw.com.tw/
author      : 林德昌
update      : 2020/02/10
date        : 2018/12/17
description : 爬取每日最新法規及最新函令
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
WEB_URL = "http://www.rootlaw.com.tw/"
CHECK_RANGE = 7 #爬網日期區間為前7日
FINAL_PATH = header.FINAL_PATH #爬網結果路徑
LAST_RESULT_PATH = header.LAST_RESULT_PATH #爬網歷史紀錄


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

def dataProcess_Title_LetterResult(strDate):
    result = dict()
    titles_result = []
    dates = []
    links = []
    nowPage = 1
    preurl = 'http://www.rootlaw.com.tw/'
    end = False
    while True:
        try:
            url = 'http://www.rootlaw.com.tw/LetterList.aspx?Page=' + str(nowPage) + '&LCode='
            soup = request2soup(url)
            titles = [e.text.strip() for e in soup.select('#gvLetter a')]
            if titles == []:
                break
            for index in range(len(titles)):
                try:
                    title = titles[index]
                    date = soup.select('.gvDate')[index].text.strip()
                    if compareTo(date, strDate) < 0:
                        end = True
                        break
                    link = preurl + soup.select('#gvLetter a')[index].get('href')
                    titles_result.append(title)
                    dates.append(date)
                    links.append(link)
                except:
                    logging.error("爬取第 %s 頁第 %s 筆主旨發生錯誤" %(nowPage, index + 1))
            if end == True:
                break
            nowPage += 1
        except:
            logging.error("爬取第 %s 頁主旨發生錯誤" %(nowPage))
            logging.error(str(traceback.format_exc()))

    result['titles_result'] = titles_result
    result['dates'] = dates
    result['links'] = links
    result['title_Type'] = '最新令函'
    return result

def dataProcess_Title_LawList(strDate):
    result = dict()
    nowPage = 1
    preurl = 'http://www.rootlaw.com.tw/'
    titles_result = []
    dates = []
    links = []
    end = False
    while True:
        try:
            url = 'http://www.rootlaw.com.tw/LawList.aspx?Page=' + str(nowPage) + '&LCode='
            soup = request2soup(url)
            titles = [re.sub(r'[\r|\n]+?', '', e.text.strip()) for e in soup.select('#ctl00_ContentPlaceHolder1_gvLaws a')]
            if titles == []:
                break
            for index in range(len(titles)):
                try:
                    title = titles[index]
                    date = soup.select('.gvDate')[index].text.strip()
                    if compareTo(date, strDate) < 0: # 若發文日期小於開始日期, 則結束爬取主旨
                        end = True
                        break
                    link = preurl + soup.select('#ctl00_ContentPlaceHolder1_gvLaws a')[index].get('href')
                    titles_result.append(title)
                    dates.append(date)
                    links.append(link)
                except:
                    logging.error("爬取第 %s 頁第 %s 筆資料發生錯誤" %(nowPage, index + 1))
                    logging.error(str(traceback.format_exc()))
            if end == True:
                break
            nowPage += 1
        except:
            logging.error("爬取第 %s 頁主旨發生錯誤" %(nowPage))
            logging.error(str(traceback.format_exc()))

    result['titles_result'] = titles_result
    result['dates'] = dates
    result['links'] = links
    result['title_Type'] = '最新法規'
    return result

def parsingTitle(strDate, endDate):

    try:
        # 取得上次爬網結果
        if os.path.isfile(LAST_RESULT_PATH):
            lastResult = pd.read_csv(LAST_RESULT_PATH, encoding='utf8')
        else:
            lastResult = pd.DataFrame()
        header.lastResult = lastResult

        # 網站網址, 爬網日期, 發文日期, 標題, 內文連結, 標題種類
        df = pd.DataFrame(columns=['WEB_ADDR', 'CRL_DATE', 'ISS_DATE', 'TITL', 'LNK_URL', 'TITL_TYPE'])

        # 資料處理
        result_LetterResult = dataProcess_Title_LetterResult(strDate)
        result_LawList = dataProcess_Title_LawList(strDate)

        d_LawList = {
            'WEB_ADDR':WEB_URL,
            'CRL_DATE':TODAY,
            'ISS_DATE': result_LawList['dates'],
            'TITL': result_LawList['titles_result'], 
            'LNK_URL': result_LawList['links'],
            'TITL_TYPE':result_LawList['title_Type']
        }

        d_LetterResult = {
            'WEB_ADDR':WEB_URL,
            'CRL_DATE':TODAY,
            'ISS_DATE': result_LetterResult['dates'],
            'TITL': result_LetterResult['titles_result'], 
            'LNK_URL': result_LetterResult['links'],
            'TITL_TYPE':result_LetterResult['title_Type']
        }

        # 合併資料
        df = df.append(pd.DataFrame(data=d_LawList))
        df = df.append(pd.DataFrame(data=d_LetterResult))

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

def dataProcess_Detail_LetterResult(soup):
    result = dict()
    fileUrlRoot = 'http://www.rootlaw.com.tw/'
    result['serno'] = [e.text for e in soup.select('#ctl00_ContentPlaceHolder1_gvLetter_ctl02_lblSorderTitle')][0]
    result['fileNames'] = [e.text for e in soup.select('#ctl00_ContentPlaceHolder1_gvLetter_ctl02_lblAttachment a')]
    result['fileUrls'] = [fileUrlRoot + e.get('href') for e in soup.select('#ctl00_ContentPlaceHolder1_gvLetter_ctl02_lblAttachment a')]
    #logging.info(result['fileNames'])
    result['issue_date'] = [e.text.strip() for e in soup.select('#ctl00_ContentPlaceHolder1_gvLetter_ctl02_lblAnnounceDate')][0] # 發文日期
    result['abstract'] = [e.text.strip() for e in soup.select('#ctl00_ContentPlaceHolder1_gvLetter_ctl02_lblSorderSubject')][0] # 要旨
    result['content'] = [e.text.strip() for e in soup.select('#ctl00_ContentPlaceHolder1_gvLetter_ctl02_pnlContent pre')][0]
    result['source'] = [e.text.strip() for e in soup.select('#ctl00_ContentPlaceHolder1_gvLetter_ctl02_lblSorderDataSource')][0]
    return result

def dataProcess_Detail_LawList(soup):
    result = dict()
    fileUrlRoot = 'http://www.rootlaw.com.tw/'
    result['fileUrls'] = [fileUrlRoot + e.get('href') for e in soup.select('#ctl00_ContentPlaceHolder1_gvLetter_ctl02_lblAttachment a')]
    result['fileNames'] = [e.text for e in soup.select('#ctl00_ContentPlaceHolder1_gvLetter_ctl02_lblAttachment a')]
    #logging.info(result['fileNames'])
    result['issue_date'] = [e.text.strip() for e in soup.select('#ctl00_ContentPlaceHolder1_lblModifyDate')][0] # 發文日期
    result['serno'] = ''
    result['abstract'] = ''
    result['content'] = ''
    result['source'] = [e.text.strip() for e in soup.select('#ctl00_ContentPlaceHolder1_lblClass')][0]
    return result

def downloadFile(finalPath, savePath, fileUrls, fileNames): # for download pdf or doc

    # 若目錄不存在，建立目錄
    if not os.path.isdir(savePath):
        os.makedirs(savePath)

    saveFileNames = []
    for file_url, fileName in zip(fileUrls, fileNames):
        try:
            response = requests.get(file_url, stream="TRUE")

            # 放置資料夾路徑 + 檔名
            saveFileName = fileName.strip() + os.path.splitext(file_url)[-1]
            downloadFile = os.path.join(savePath, saveFileName)
            #logging.info("附件檔名:" + saveFileName + '\r\n')

            with open(downloadFile, 'wb') as file:
                for data in response.iter_content():
                    file.write(data)

            saveFileNames.append(saveFileName)
        except:
            header.EXIT_CODE = -1
            logging.error("下載附件失敗")
            logging.error("失敗連結：" + str(file_url))
            logging.error(str(traceback.format_exc()))

    return saveFileNames

def parsingDetail(df, finalPath=FINAL_PATH):

    # 發文日期, 發文字號, 標題, 本文, 相關法條, 附件, 附件存放資料夾, 附件存檔名稱
    df2 = pd.DataFrame(columns=["ISS_DATE", "ISS_NO", "TITL", "ISS_CTNT", "RLT_RGL", "FILES", "FOLDER_NM", "FILES_NM"])

    # 第一層結果：網站網址, 爬網日期, 發文日期, 標題, 內文連結, 標題種類
    for index, row in df.iterrows():
        try:
            title = row['TITL']
            title_type = row['TITL_TYPE']
            #logging.info(title)

            link = row['LNK_URL']
            soup = request2soup(link)

            if title_type == '最新令函':
                result = dataProcess_Detail_LetterResult(soup)
            elif title_type == '最新法規':
                result = dataProcess_Detail_LawList(soup)

            folderName = ""
            saveFileNames = []
            fileNames = result['fileNames']
            if len(fileNames) != 0:
                folderName = title[:30].strip()
                savePath = os.path.join(finalPath, folderName)
                saveFileNames = downloadFile(finalPath, savePath, result['fileUrls'], fileNames)

            d = {
                'TITL': title,
                'ISS_DATE': result['issue_date'],
                'ISS_NO': result['serno'],
                'ISS_CTNT': result['content'],
                'RLT_RGL': '',
                'FILES': ','.join(fileNames),
                'FOLDER_NM': folderName,
                'FILES_NM': ','.join(saveFileNames),
                'SOURCE':result['source'], #來源
                'ABSTRACT':result['abstract'] #摘要
            }

            df2= df2.append(d, ignore_index = True)

        except:
            header.EXIT_CODE = -1
            logging.error("爬取內文失敗")
            logging.error("失敗連結：" + str(link)) # 避免 unicode 編碼錯誤，將 link 轉為字串
            logging.error(str(traceback.format_exc()))
    return df2

def request2soup(url):
    res = requests.get(url)
    res.encoding = 'utf-8'
    soup = BeautifulSoup(res.text,'html.parser',from_encoding='utf-8')
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
        if RESULT_COUNT < 1:
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
