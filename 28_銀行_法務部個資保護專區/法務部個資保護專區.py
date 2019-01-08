#!/usr/bin/env python
# coding: utf-8
'''
Project Name: bankingGov project
Crawl Cycle: Daily
Main Website: http://pipa.moj.gov.tw/lp.asp?CtNode=408&CtUnit=115&BaseDSD=7&mp=1&nowPage=1&pagesize=10
author      : 林德昌
date        : 2018/10/16
description : 抓取法務部個資保護專區每天發布最新的個資法問與答
'''
# In[89]:


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


# In[90]:


def dataProcess_Detail(soup, row):
    result = dict()
    result['fileUrls'] = ''
    result['fileNames'] = ''
    result['content'] = [e.text for e in soup.select('#zone\.content11 p')]
    result['issue_date'] = [e.text[1:] for e in soup.select('.info span')]
    return result


# In[91]:


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
            d = {'標題': title, '全文內容': result['content'], '附件':'', '發文字號':'', '發文日期':result['issue_date'],
                 '相關法條':''}
            df2= df2.append(pd.DataFrame(data=d, index=[0]))
        except:
            logging.error("爬取內文失敗")
            logging.error("失敗連結：" + link)
            traceback.print_exc()
    return df2


# In[92]:


def outputCsv(df, fileName, path):
    # 若目錄不存在，建立目錄
    if not os.path.isdir(path):
        os.mkdir(path)
    df.to_csv(path + "/" + fileName + ".csv", index = False, encoding = "utf_8_sig")


# In[93]:


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


# In[94]:


def dataProcess_Title(strDate):
    result = dict()
    nowPage = 1
    preurl = 'http://pipa.moj.gov.tw/'
    titles_result = []
    dates = []
    links = []
    end = False
    while True:
        try:
            url = 'http://pipa.moj.gov.tw/lp.asp?CtNode=408&CtUnit=115&BaseDSD=7&mp=1&nowPage=' + str(nowPage) + '&pagesize=10'
            soup = request2soup(url)
            titles = [str(e.get('title').strip()) for e in soup.select('.list a')]
            if titles == []:
                break
            for index in range(len(titles)):
                try:
                    title = titles[index]
                    date = soup.select('.date')[index].text.strip()
                    if compareTo(date, strDate) > 0: # 若發文日期小於開始日期, 則結束爬取主旨
                        end = True
                        break
                    link = preurl + soup.select('.list a')[index].get('href')
                    titles_result.append(title)
                    dates.append(date)
                    links.append(link)
                except:
                    logging.error("爬取第 %s 頁第 %s 筆資料發生錯誤" %(nowPage, index + 1))
                    traceback.print_exc()
            if end == True:
                break
            nowPage += 1
        except:
            logging.error("爬取第 %s 頁主旨發生錯誤" %(nowPage))
            traceback.print_exc()
        
    result['titles_result'] = titles_result
    result['dates'] = dates
    result['links'] = links
    result['crawl_date'] = datetime.date.today()
    return result


# In[95]:


def parsingTitle(checkRange):
    try:
        # 取得上次爬網結果
        if os.path.isfile(lastResultPath):
            lastResult = pd.read_csv(lastResultPath)
        else:
            lastResult = pd.DataFrame()
        
        # 爬網日期區間為一個禮拜
        endDate = datetime.date.today()
        strDate = (endDate - datetime.timedelta(days = checkRange)).isoformat()
        df = pd.DataFrame(columns = ["爬網日期","發文日期", "標題", "內文連結"])
        
        # 資料處理
        result = dataProcess_Title(strDate)
        
        d = {'爬網日期':result['crawl_date'], '發文日期': result['dates'], '標題': result['titles_result'], '內文連結': result['links']}
        df = df.append(pd.DataFrame(data = d))    
        # 若與上次發文日期和標題相同，則跳至下一筆
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
    


# In[96]:


def request2soup(url):
    res = requests.get(url)
    res.encoding = 'utf-8'
    soup = BeautifulSoup(res.text,'html.parser',from_encoding='utf-8')
    return soup


# In[97]:


def main(url, checkRange = 1000):
    
    logging.critical("\n")
    logging.critical("爬網開始......")
    logging.critical("目標網址：" + url)
    
    strTime = datetime.datetime.now()
    logging.critical("開始時間：" + strTime.strftime("%Y/%m/%d %H:%M:%S"))
    try:
        df_1 = parsingTitle(checkRange)
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


# In[98]:


if __name__ == "__main__":
    url = "http://pipa.moj.gov.tw/lp.asp?CtNode=408&CtUnit=115&BaseDSD=7&mp=1&nowPage=1&pagesize=10"
    main(url)

