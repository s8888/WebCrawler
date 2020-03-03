#!/usr/bin/env python
# coding: utf-8
'''
Project Name: moj project
Crawl Cycle: Daily
Main Website: https://law.moj.gov.tw/News/news_list.aspx?type=olm
author      : 林德昌
update      : 2020/02/10
date        : 2018/10/16
description : 抓取全國法規資料庫每天發布的最新消息
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
WEB_URL = "https://law.moj.gov.tw/News/NewsList.aspx?type=all"
CHECK_RANGE = 7 #爬網日期區間為前7日
FINAL_PATH = header.FINAL_PATH #爬網結果路徑
LAST_RESULT_PATH = header.LAST_RESULT_PATH #爬網歷史紀錄
MOJ_FIX_URL = "https://law.moj.gov.tw/News/"

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

        # 網站網址, 爬網日期, 發文日期, 標題, 內文連結
        df = pd.DataFrame(columns=['WEB_ADDR', 'CRL_DATE', 'ISS_DATE', 'TITL', 'LNK_URL'])

        # 發送請求，取得網頁內容
        soup = request2soup(WEB_URL)

        nowPage = 1
        iss_date_check = True
        while iss_date_check:
            try:
                # 取得該頁的所有文章列表
                table = soup.find('table', { 'class' : 'table table-hover tab-list tab-news' })
                data_rows = table.find('tbody').findAll('tr')

                for data_row in data_rows:
                    # 當前資料的各個欄位值
                    title_columns = data_row.findAll('td')

                    # 檢核最新文章的日期，是否在此次爬網區間的範圍
                    iss_date = str(title_columns[1].string)
                    if compareTo(iss_date, strDate) < 0:
                        iss_date_check = False
                        break

                    lnk_url = str(title_columns[3].find('a')['href'])
                    if lnk_url[:10] == "NewsDetail":
                        lnk_url = MOJ_FIX_URL + lnk_url

                    tempDf = {
                            'WEB_ADDR': WEB_URL,
                            'CRL_DATE': TODAY,
                            'ISS_DATE': iss_date,
                            'TITL': title_columns[3].find('a').string,
                            'LNK_URL': lnk_url
                    }

                    df = df.append(tempDf, ignore_index=True)
            except:
                header.EXIT_CODE = -1
                logging.error("爬取第 %s 頁發生錯誤" % (nowPage))
                logging.error(str(traceback.format_exc()))

            # 發送請求，取得下一頁的網頁內容
            nowPage += 1
            soup = request2soup(WEB_URL + "&page={}".format(nowPage))

        # 確認查詢區間是否有資料
        if len(df) > 0:
            # 重設資料索引值
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

def dataProcess_Detail(soup, title, link):

    urlNTPC = 'http://web.law.ntpc.gov.tw' # linkto 新北市政府網站
    urlGazette = 'http://gazette.nat.gov.tw' # linkto 行政院公報資訊網
    urlTaipei = 'http://www.laws.taipei.gov.tw'# linkto 台北市政府網站
    urlmoj = 'https://law.moj.gov.tw' # linkto 全國法規資料庫
    urlmojlaw = 'https://mojlaw.moj.gov.tw' # linkto 法務部
    #title = row['標題']
    #link = row['內文連結']

    result = dict()
    issue_date = ''
    content = ''
    serno = ''
    fileUrls = []
    fileNames = []
    if link.find(urlNTPC) >= 0:  
        #content = ''
        #abstract = ''
        #serno = ''
        fileUrls = [e.get('href').replace('./', 'http://web.law.ntpc.gov.tw/fn/') for e in soup.select('#Law-Content a')][1:]
        fileNames = [e.text for e in soup.select('#Law-Content a')][1:]
        issue_date = [e.text.strip() for e in soup.select('td b')][0]
        if fileNames == []:
            content = [e.text for e in soup.select('.worddisaplay')][0]
    elif link.find(urlTaipei) >= 0:
        #content = ''
        fileUrls = [e.get('href') for e in soup.select('#ContentPlaceHolder1_gvListF a')]
        serno = [e.text for e in soup.select('#ContentPlaceHolder1_lblCLASSNAME')][0]
        fileNames = [e.text for e in soup.select('#ContentPlaceHolder1_gvListF a')]
        str_content = str(soup.select('#ContentPlaceHolder1_lblREVISIONINFO')[0])
        issue_date = re.findall(r'中華民國.+?日', str_content)[0]
    elif link.find(urlGazette) >= 0: 
        #content = ''
        fileUrls = [urlGazette + e.get('src') for e in soup.select('.embed-responsive-item')]
        fileNames = ['eg.pdf' for i in range(len(fileUrls))]
        serno = [e.text for e in soup.select('section.Block p span')][2]
        issue_date = soup.select('div.Item section.Block p span')[1].text
    elif link.find(urlmoj) >= 0:

        # 20200211 格式修改
        table = soup.find('table', {'class' : 'table table-bordered tab-edit tab-NewsDetail'})
        rows = table.find('tbody').findAll('tr')

        issue_date = rows[0].find('td').string
        content = rows[1].find('td').get_text('\n', '<br>')

        if len(rows) > 3:
            for i in range(2, 4, 1):
                file_lnk = rows[i].find('td').find('a')
                file_url = file_lnk['href']
                file_name = file_lnk.string
                if file_url[:11] == "NewsGetFile":
                    file_url = MOJ_FIX_URL + file_url
                    fileUrls.append(file_url)
                    fileNames.append(file_name)

    elif link.find(urlmojlaw) >= 0:
        pattern = re.compile(r'http(\:|\w|\.|\-|\/)+\/')
        m = pattern.match(link)
        content = [e.text.strip() for e in soup.select('.ClearCss')]
        content = '' if not bool(content) else content[0]
        fileUrls = [m.group(0) + e.get('href') for e in soup.select('.tab-edit a')]
        fileNames = [e.text for e in soup.select('.tab-edit a')]
        serno = [e.text.strip() for e in soup.select('#cp_content_trODWord td')]
        issue_date = [e.text.strip() for e in soup.select('#cp_content_trAnnDate td')]
    elif link.find('NewsContent.aspx') >= 0: # 其他地方政府網站內文
        NotFoundmsg = [e.text for e in soup.select('.text-danger')]  
        if NotFoundmsg != []:  # 若查詢結果為錯誤訊息，則回傳空的dict
            return result
        pattern = re.compile(r'http(\:|\w|\.|\-|\/)+\/')
        m = pattern.match(link)
        content = [e.text.strip() for e in soup.select('.ClearCss')]
        content = '' if not bool(content) else content[0]
        fileUrls = [m.group(0) + e.get('href') for e in soup.select('#ctl00_cp_content_ulAnnFiles02 a')]
        fileNames = [e.text for e in soup.select('#ctl00_cp_content_ulAnnFiles02 a')]
        serno = [e.text.strip() for e in soup.select('#ctl00_cp_content_trODWord td')]
        issue_date = [e.text.strip() for e in soup.select('#ctl00_cp_content_trAnnDate td')]

    result['fileUrls'] = fileUrls
    result['fileNames'] = fileNames 
    result['content'] = content
    result['serno'] = serno
    result['issue_date'] = issue_date

    return result

def downloadFile(finalPath, savePath, fileUrls, fileNames): # for download pdf or doc

    # 若目錄不存在，建立目錄
    if not os.path.isdir(savePath):
        os.makedirs(savePath)

    saveFileNames = []
    for file_url, fileName in zip(fileUrls, fileNames):
        try:
            #logging.info(fileName.strip())
            response = requests.get(file_url, stream="TRUE")

            saveFileName = fileName.strip()# + os.path.splitext(file_url)[-1]
            downloadFile = os.path.join(savePath, saveFileName)
            #logging.info(downloadFile + '\r\n')

            with open(downloadFile,'wb') as file:
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

    for index, row in df.iterrows():
        try:
            title = row['TITL']
            link = row['LNK_URL']
            #logging.info(title)

            # 取得內文
            soup = request2soup(link)
            result = dataProcess_Detail(soup, title, link)

            if not bool(result):
                continue

            # 下載附件
            folderName = ""
            saveFileNames = []
            fileNames = result['fileNames'] 
            if len(fileNames) != 0:
                folderName = title[:30].strip()
                savePath = os.path.join(finalPath, folderName)
                saveFileNames = downloadFile(finalPath, savePath, result['fileUrls'], fileNames)

            d = {
                'ISS_DATE': result['issue_date'],
                'ISS_NO': result['serno'],
                'TITL': title,
                'ISS_CTNT': result['content'],
                'RLT_RGL': '',
                'FILES': ','.join(fileNames),
                'FOLDER_NM': folderName,
                'FILES_NM': ','.join(saveFileNames),
            }

            df2= df2.append(pd.DataFrame(data=d, index=[0]))

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
