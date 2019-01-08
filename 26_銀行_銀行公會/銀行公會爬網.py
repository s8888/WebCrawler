#!/usr/bin/env python
# coding: utf-8
'''
Project Name: moj project
Crawl Cycle: Daily
Main Website: http://www.ba.org.tw/PublicInformation/PublicinfoAll
author      : 林德昌
date        : 2018/12/17
description : 抓取銀行公會每天發布的最新消息
'''
# In[37]:


import requests
from bs4 import BeautifulSoup
import pandas as pd
import header
import logging
import re
import datetime
import traceback # 印log
import os
TempPath = "./Temp"  # browser file
FinalPath = "./Result" # project file
lastResultPath = "./CrawlList/lastResult.csv"


# In[38]:


def downloadFile(finalPath, title, fileUrls, fileNames): # for download pdf or doc
    target = finalPath + '/' + title[:30].strip()
    # 若目錄不存在，建立目錄
    if not os.path.isdir(target):
        os.makedirs(target)
    for file_url, fileName in zip(fileUrls, fileNames):
        try:
            response = requests.get(file_url, stream="TRUE")
            downloadFile = target + '/' + fileName.strip() # 放置資料夾路徑 + 檔名
            logging.info(downloadFile + '\r\n')
            with open(downloadFile,'wb') as file:
                for data in response.iter_content():
                    file.write(data)
        except:
            logging.error("爬取檔案失敗")
            logging.error("失敗連結：" + file_url)


# In[39]:


def dataProcess_Detail(soup, row):
    result = dict()
    fileUrlroot = 'https://www.ba.org.tw'
    result['fileUrls'] = [fileUrlroot + e.get('href') for e in soup.select('.main_Content_downloadList a')]
    result['fileNames'] = [e.text for e in soup.select('.main_Content_downloadList a')]
    result['issue_date'] = [e.text for e in soup.select('.main_Content_day')][0]
    return result


# In[40]:


def parsingDetail(df, finalPath): 
    df2 = pd.DataFrame(columns = ["標題", "全文內容", "附件", "發文字號", "發文日期", "相關法條"])
    fileUrlRoot = 'https://www.banking.gov.tw'
    for index, row in df.iterrows():
        try:
            title = row['標題']
            logging.info(title)
            link = row['內文連結']
            soup = request2soup(link)
            result = dataProcess_Detail(soup, fileUrlRoot)
            fileNames = result['fileNames'] 
            if len(fileNames) != 0:
                downloadFile(finalPath, title, result['fileUrls'], fileNames)
            d = {'標題': title, '全文內容': '', '附件':fileNames, '發文字號':'', '發文日期':result['issue_date'],
                 '相關法條':''}
            df2= df2.append(d, ignore_index = True)
        except:
            logging.error("爬取內文失敗")
            logging.error("失敗連結：" + link)
            traceback.print_exc()
    return df2


# In[41]:


def outputCsv(df, fileName, path):
    # 若目錄不存在，建立目錄
    if not os.path.isdir(path):
        os.mkdir(path)
    df.to_csv(path + "/" + fileName + ".csv", index = False, encoding = "utf_8_sig")


# In[42]:


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
        return 1
    elif strDate == endDate:
        return 0
    else:
        return -1


# In[43]:


def parsingTitle(soup, checkRange):
    try:
        # 取得上次爬網結果
        if os.path.isfile(lastResultPath):
            lastResult = pd.read_csv(lastResultPath)
        else:
            lastResult = pd.DataFrame()
        
        # 爬網日期區間為一個禮拜
        endDate = datetime.date.today()
        preurl = 'https://www.ba.org.tw'
        strDate = (endDate - datetime.timedelta(days = checkRange)).isoformat()
        df = pd.DataFrame(columns=['爬網日期','發文日期','標題','檔案連結'])
        index = 0 # css selector上的資料序號
        nowPage = 1
        totalContent = [e.text for e in soup.select('td')]
        while True:
            try:
                if index >= (len(totalContent) / 3):
                    break
                title = totalContent[3 * index + 1]
                link = [preurl + e.get('href') for e in soup.select('#maincontent a')][2*index + 1]
                date = totalContent[3 * index]
                if compareTo(date, strDate) > 0:
                    break
                tempDf = {'爬網日期': endDate, '發文日期': date,'標題': title, '內文連結': link}
                df = df.append(tempDf, ignore_index = True)
                index += 1
            except:
                logging.error("爬取第 %s 頁第 %s 筆主旨發生錯誤" %(nowPage, index + 1))
                traceback.print_exc()
        df.index = [i for i in range(df.shape[0])] # reset Index 
        
        if not lastResult.empty:
            for index, row in df.iterrows():
                if (row['發文日期'] in list(lastResult['發文日期'])) and (row['標題'] in list(lastResult['標題'])):
                    df.drop(index, inplace = True)

        if len(df) == 0:
            logging.critical("%s 至 %s 間無資料更新" %(strDate, endDate))
        else:
            df.index = [i for i in range(df.shape[0])] # reset 
            outputCsv(df, "lastResult", "./CrawlList")
    except:
        logging.error("爬取主旨列表失敗")
        traceback.print_exc()
    
    return df


# In[44]:


def request2soup(url):
    res = requests.get(url)
    res.encoding = 'utf-8'
    soup = BeautifulSoup(res.text,'html.parser',from_encoding='utf-8')
    return soup


# In[45]:


def main(url, checkRange = 500):
    
    logging.critical("\n")
    logging.critical("爬網開始......")
    logging.critical("目標網址：" + url)
    
    strTime = datetime.datetime.now()
    logging.critical("開始時間：" + strTime.strftime("%Y/%m/%d %H:%M:%S"))
    try:
        soup = request2soup(url)
        df_1 = parsingTitle(soup, checkRange)
        if len(df_1) == 0:
            return
        outputCsv(df_1, "第一層結果", FinalPath)
        df_2 = parsingDetail(df_1, FinalPath)
        outputCsv(df_2, "第二層結果", FinalPath)
    except:
        logging.error("執行爬網作業失敗")
        traceback.print_exc()
        
    endTime = datetime.datetime.now()
    logging.critical("結束時間：" + endTime.strftime("%Y/%m/%d %H:%M:%S"))
    logging.critical("執行時間：" + str((endTime - strTime).seconds) + " 秒")
    logging.critical("輸出筆數：" + str(len(df_1)) + " 筆")
    logging.critical("爬網結束......")


# In[46]:


if __name__ == "__main__":
    url = "http://www.ba.org.tw/PublicInformation/PublicinfoAll"
    main(url)


# In[6]:


url = 'https://www.ba.org.tw/PublicInformation/PublicinfoAll'
soup = request2soup(url)

[e.text for e in soup.select('td')]
# https://www.ba.org.tw/PublicInformation/Detail/2695?enumtype=ImportantnormType&type=99537959-bc87-4d24-bcb7-83c8e7767e65

