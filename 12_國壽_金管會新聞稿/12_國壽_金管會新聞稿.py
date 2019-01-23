#!/usr/bin/env python
# coding: utf-8
'''
Project Name: 金管會新聞稿
CTM Name    : 
Crawl Cycle : Daily
Main Website: https://www.fsc.gov.tw/ch/home.jsp?id=96&parentpath=0,2
Description : 針對金管會-公告資訊-新聞稿，每日取得新聞稿
Author      : 謝子嫣
Update Date : 2019.01.23
'''
# In[1]:


import header
import logging

import datetime
import pandas as pd
import traceback
from bs4 import BeautifulSoup

import os
import requests


# In[8]:


def parsingDetail(df):

    df_detail = pd.DataFrame(columns = ["ISS_DATE", "TITL", "ISS_CTNT", "ISS_NO", "RLT_RGL", "FILES"])

    for link in df.LNK_URL:
        try:
            soup = request2soup(link)

            # 主旨
            title = soup.select("#maincontent h3")[0]
            # 內容
            content = soup.select(".page_content")[0]
            # 附件
            attachments = soup.select(".acces a") # n 個附件
            
            df_detail = df_detail.append({"ISS_DATE" : '', 
                                          "TITL" : title.text.strip(), 
                                          "ISS_CTNT" : content.text,
                                          "ISS_NO" : '',
                                          "RLT_RGL" : '',
                                          "FILES" : " , ".join(str(e.get("title")).replace("(開啟新視窗)", "") 
                                                            for e in attachments)
                                         }, 
                                         ignore_index = True)
           
            # 附件
            if len(attachments) != 0:
                
                # 建立資料夾
                target = header.FINAL_PATH + "/" + title.text[:30]
                
                # 若目錄不存在, 建立目錄
                if not os.path.isdir(target):
                    os.mkdir(target)
                    
                for attach in attachments:
                    fileLink = "https://www.fsc.gov.tw" + attach.get("href")
                    # 下載附件
                    response = requests.get(fileLink, stream = "TRUE")
                    fileName = attach.get("title").replace("(開啟新視窗)", "")
                    endLoc = fileName.rfind(".") # 檔名結尾位置
                    extName = fileName[endLoc:]  # 副檔名
                    fileName = fileName[:endLoc] # 檔名
                    fileName = fileName[:30]     # 截短檔名
                    with open(target + "/" + fileName + extName, "wb") as file:
                        for data in response.iter_content():
                            file.write(data)

        except:
            logging.error("爬取內文失敗")
            logging.error("失敗連結：" + link)
            traceback.print_exc()

    return df_detail


# In[9]:


def parsingTitle(soup, checkRange):
    try:
        # 取得上次爬網結果
        lastResultPath = header.LAST_RESULT_PATH + "/lastResult.csv"
        if os.path.isfile(lastResultPath):
            lastResult = pd.read_csv(lastResultPath)
        else:
            lastResult = pd.DataFrame()
        
        # 爬網日期區間為一個禮拜
        endDate = datetime.date.today()
        strDate = (endDate - datetime.timedelta(days = checkRange)).isoformat()
        
        totalPage = soup.select(".page")[0].text.split("/")[1] # 總頁數
        ending = False
        
        df = pd.DataFrame(columns = ["WEB_ADDR", "CRL_DATE", "ISS_DATE", "TITL", "LNK_URL"])

        for i in range(int(totalPage)):
            if (i != 0):
                soup = request2soup(url, i + 1)

            try:
                sorts = soup.select(".sort1")
                sorts = [x.text.strip() for x in sorts]
                
                dates = soup.select(".pdate1")
                dates = [x.text.strip() for x in dates]

                sources = soup.select(".souse1")
                sources = [x.text.strip() for x in sources] 

                titles = soup.select(".ptitle1")
                titles = [x.text.strip() for x in titles] 

                links = soup.select(".ptitle1 a")
                links = ["https://www.fsc.gov.tw/ch/" + x.get("href") for x in links]

                idx = pd.Series([False] * len(dates))
                for j in range(len(dates)):
                    date = dates[j]
                    if date < strDate: # 若發文日期小於開始日期, 則結束爬取主旨
                        ending = True
                        break
                    idx[j] = True
                d = {"WEB_ADDR" : url, "CRL_DATE" : endDate, "ISS_DATE" : dates, "TITL" : titles, "LNK_URL" : links}
                df = df.append(pd.DataFrame(data = d)[idx])  # append page

                # 若結束爬取主旨, 停止爬取剩下的 pages
                if ending:
                    break
            except:
                logging.error("爬取第 %s 頁主旨發生錯誤" %str(i + 1))
                traceback.print_exc()

        df.index = [i for i in range(df.shape[0])] # reset Index 
        header.outputCsv(df, "lastResult", header.LAST_RESULT_PATH)

        if not lastResult.empty:
            # 若與上次發文日期和標題相同，則跳至下一筆
            for i in range(len(df)):
                for j in range(len(lastResult)):
                    if (df.ISS_DATE[i] == lastResult.ISS_DATE[j]) & (df.TITL[i] == lastResult.TITL[j]): 
                        df.drop(i, inplace = True)
                        break

        if len(df) == 0:
            logging.critical("%s 至 %s 間無資料更新" %(strDate, endDate))
        else:
            df.index = [i for i in range(df.shape[0])] # reset 

        return df
    
    except:
        logging.error("爬取主旨列表失敗")
        traceback.print_exc()
        return pd.DataFrame(columns = ["WEB_ADDR", "CRL_DATE", "ISS_DATE", "TITL", "LNK_URL"])
  


# In[10]:


def request2soup(url, page = None):
    if page is None:
        res = requests.get(url)
    else:
        formData = {"id"         : "96", 
                    "contentid"  : "96", 
                    "parentpath" : "0,2", 
                    "mcustomize" : "news_list.jsp", 
                    "keyword"    : "請輸入查詢關鍵字", 
                    "page"       : page}
        res = requests.post(url, data = formData)
        
    res.encoding = "utf-8"
    soup = BeautifulSoup(res.text, "html.parser", from_encoding = "utf-8")
    return soup


# In[11]:


def main(url, checkRange = 30):
    header.processBegin(url = url)
    header.clearFolder(crawlList=True)

    try:
        soup = request2soup(url, 1)
        
        df_1 = parsingTitle(soup, checkRange)
        if len(df_1) != 0:
            header.outputCsv(df_1, "第一層結果")
            df_2 = parsingDetail(df_1)

            header.outputCsv(df_2, "第二層結果")
            header.RESULT_COUNT = len(df_2)
        header.zipFile()
        header.createInfoFile()
        header.createOKFile()
    except:
        logging.error("執行爬網作業失敗")
        traceback.print_exc()
        header.createInfoFile()
    
    header.processEnd()


# In[12]:


print(header.TIMELABEL)
logging.fatal("FINAL_PATH:"+ header.FINAL_PATH)
url = "https://www.fsc.gov.tw/ch/home.jsp?id=96&parentpath=0,2"


# In[13]:


main(url)


# In[ ]:





# In[ ]:




