#!/usr/bin/env python
# coding: utf-8
'''
Project Name: 法務部調查局洗錢防制處
CTM Name    : 
Crawl Cycle : Daily
Main Website: https://www.mjib.gov.tw/mlpc
Description : 法務部調查局洗錢防制處
Author      : 謝子嫣
Update Date : 2018.11.21
'''
# In[8]:


import header
import logging

import datetime
import pandas as pd
import traceback
from bs4 import BeautifulSoup

import os
import requests
import re


# In[9]:


def dataProcess_Title(soup, strDate):
    result = dict()
    titles_result = []
    dates = []
    links = []

    try:
        soup = request2soup(url)
        titles = [e.text for e in soup.select('#wrapper li a')]
        links_tmp = [x.get('href') for x in soup.select("#wrapper li a")]
        
        for link in links_tmp:
            fulLink = re.findall(r'https://.+?',link)
            if fulLink:
                links.append(link)
            else:
                links.append("https://www.mjib.gov.tw" + link)

    except:
        logging.error("爬取主旨發生錯誤")
        traceback.print_exc()
        
    result['titles_result'] = titles
    result['links'] = links
    result['crawl_date'] = datetime.date.today()
    return result


# In[10]:


def parsingTitle(soup, checkRange):
    try:
        # 取得上次爬網結果
        lastResultPath = header.LAST_RESULT_PATH # +"/lastResult.csv"#[2019.02.11]
        
        if os.path.isfile(lastResultPath):
            lastResult = pd.read_csv(lastResultPath)
        else:
            lastResult = pd.DataFrame()
        header.lastResult = lastResult #[2019.02.11]新增全域變數
        
        # 爬網日期區間為一個禮拜
        endDate = datetime.date.today()
        strDate = (endDate - datetime.timedelta(days = checkRange)).isoformat()        
        
        df = pd.DataFrame(columns = ["WEB_ADDR", "CRL_DATE", "ISS_DATE", "TITL", "LNK_URL"])
        soup = request2soup(url)
        
        # 資料處理
        result = dataProcess_Title(soup, strDate)

        d = {'WEB_ADDR':url, 'CRL_DATE':result['crawl_date'], 'ISS_DATE':'', 
             'TITL': result['titles_result'], 'LNK_URL': result['links']}
        
        df = df.append(pd.DataFrame(data = d))    
        # 若與上次發文日期和標題相同，則跳至下一筆
        if not lastResult.empty:
            for index, row in df.iterrows():
                if row['TITL'] in list(lastResult['TITL']):
                    df.drop(index, inplace = True)
                    
        if len(df) == 0:
            logging.critical("%s 至 %s 間無資料更新" %(strDate, endDate))
        else:
            df.index = [i for i in range(df.shape[0])] # reset
            lastResult = lastResult.append(df)
            lastResult.index = [i for i in range(lastResult.shape[0])] # reset
            lastResult = lastResult[pd.to_datetime(lastResult['CRL_DATE']) >= (datetime.date.today() - datetime.timedelta(days = checkRange))]
            header.outputCsv(lastResult, "lastResult", header.CRAWL_LIST_PATH)
        
    except:
        header.EXIT_CODE = -1
        logging.error("爬取主旨列表失敗")
        traceback.print_exc()
    return df


# In[11]:


def request2soup(url, page = None):
    res = requests.get(url)
    res.encoding = "utf-8"
    soup = BeautifulSoup(res.text, "html.parser", from_encoding = "utf-8")
    return soup


# In[12]:


def main(url, checkRange = 30):
    header.processBegin(url = url)
    header.clearFolder()#[2019.02.11]
    
    try:
        soup = request2soup(url)
        
        df_1 = parsingTitle(soup, checkRange)
        if len(df_1) != 0:
            #outputCsv(df_1, "第一層結果", FinalPath)
            header.outputCsv(df_1, "第一層結果")
            
        header.zipFile()
        header.createInfoFile()
        header.createOKFile()
        header.outputLastResult(df_1, header.lastResult, checkRange)   #[2019.02.11]新增產出lastResult方法
    except:
        logging.error("執行爬網作業失敗")
        traceback.print_exc()
        header.createInfoFile()
        
    header.processEnd()


# In[13]:


print(header.TIMELABEL)
logging.fatal("FINAL_PATH:"+ header.FINAL_PATH)
url = "https://www.mjib.gov.tw/mlpc"


# In[14]:


main(url)


# In[ ]:




