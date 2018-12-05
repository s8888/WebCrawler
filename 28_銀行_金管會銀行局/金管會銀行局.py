#!/usr/bin/env python
# coding: utf-8
'''
Project Name: bankingGov project
Crawl Cycle: Daily
Main Website: https://www.banking.gov.tw/ch/home.jsp?id=190&parentpath=0,3&mcustomize=
author      : 林德昌
date        : 2018/10/16
description : 抓取金管會銀行局每天發布的最新法令函釋
'''
# In[42]:


import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
import header
import logging

import sys
import datetime
import pandas as pd
import os
import traceback # 印log
TempPath = "./Temp"  # browser file
FinalPath = "./Result" # project file


# In[43]:


def downloadFile(finalPath, title, fileUrls, fileNames): # for download pdf or doc
    target = finalPath + '/' + title[:30].strip()
    # 若目錄不存在，建立目錄
    if not os.path.isdir(target):
        os.makedirs(target)
    i = 0
    for file_url, fileName in zip(fileUrls, fileNames):
        try:
            response = requests.get(file_url, stream="TRUE")
            downloadFile = target + '/' + fileName.strip() # 放置資料夾路徑 + 檔名
            print(downloadFile + '\r\n')
            with open(downloadFile,'wb') as file:
                for data in response.iter_content():
                    file.write(data)
        except:
            print("爬取檔案失敗")
            print("失敗連結：" + file_url)
            logging.error("爬取檔案失敗")
            logging.error("失敗連結：" + file_url)
        i += 1


# In[44]:


def dataProcess_Detail(soup, fileUrlRoot):
    result = dict()
    result['content'] = [e.text for e in soup.select('.page_content')][0]
    result['fileNames'] = [e.text for e in soup.select('.acces a')]
    print(result['fileNames'])
    result['fileUrls'] = [fileUrlRoot + e.get('href') for e in soup.select('.acces a')]
    str_content = str(soup.select('.page_content')[0])
    result['serno'] = re.findall(r'發文字號.+?\d+.+?', str_content)[0][5:]
    return result


# In[45]:


def parsingDetail(df, finalPath): 
    df2 = pd.DataFrame(columns = ["標題", "全文內容", "附件", "發文字號", "發文日期", "相關法條"])
    fileUrlRoot = 'https://www.banking.gov.tw'
    for index, row in df.iterrows():
        title = row['title']
        print(title)
        link = row['link']
        date = row['date']
        try:
            soup = request2soup(link)
            result = dataProcess_Detail(soup, fileUrlRoot)
        except:
            print("爬取內文失敗")
            print("失敗連結：" + link)
            logging.error("爬取內文失敗")
            logging.error("失敗連結：" + link)
            traceback.print_exc()
        fileNames = result['fileNames'] 
        if len(fileNames) != 0:
            downloadFile(finalPath, title, result['fileUrls'], fileNames)
        d = {'標題': title, '全文內容': result['content'], '附件':','.join(fileNames), '發文字號':result['serno'], '發文日期':date,
             '相關法條':''}
        df2= df2.append(pd.DataFrame(data=d, index=[0]))
    return df2


# In[46]:


def outputCsv(df, fileName, path):
    # 若目錄不存在，建立目錄
    if not os.path.isdir(path):
        os.mkdir(path)
    df.to_csv(path + "/" + fileName + ".csv", index = False, encoding = "utf_8_sig")


# In[47]:


def compareTo(strDate, endDate):
    if int(re.split(r'(/|-|\.)', strDate)[0]) < 1911:
        strDate = datetime.datetime.strptime(str(int(re.sub(r'(/|-|\.)', '', strDate)) + 19110000), "%Y%m%d").strftime("%Y-%m-%d")
    if int(re.split(r'(/|-|\.)', endDate)[0]) < 1911:
        endDate = datetime.datetime.strptime(str(int(re.sub(r'(/|-|\.)', '', endDate)) + 19110000), "%Y%m%d").strftime("%Y-%m-%d")
    try:
        strDate = datetime.datetime.strptime(strDate, "%Y-%m-%d")
        endDate = datetime.datetime.strptime(endDate, "%Y-%m-%d")

    except:
        print("日期格式錯誤：strDate = %s, endDate = %s" %(strDate, endDate))
        logging.error('compareTo(strDate, endDate):')
        logging.error("日期格式錯誤：strDate = %s, endDate = %s" %(strDate, endDate))
        return
    if strDate < endDate:
        return 1
    elif strDate == endDate:
        return 0
    else:
        return -1


# In[48]:


def dataProcess_Title(soup, strDate):
    result = dict()
    nowPage = 1
    preurl = 'https://www.banking.gov.tw/ch/'
    titles_result = []
    dates = []
    links = []
    end = False
    while True:
        try:
            if (nowPage > 1):
                soup = request2soup(url, nowPage)
            titles = [str(e.get('title').strip()) for e in soup.select('.ptitle1 a')]
            if titles == []:
                break
            for index in range(len(titles)):
                try:
                    title = titles[index]
                    date = soup.select('.pdate1')[index].text.strip()
                    if compareTo(date, strDate) > 0: # 若發文日期小於開始日期, 則結束爬取主旨
                        end = True
                        break
                    link = preurl + soup.select('.ptitle1 a')[index].get('href')
                    titles_result.append(title)
                    dates.append(date)
                    links.append(link)
                except:
                    print("爬取第 %s 頁第 %s 筆資料發生錯誤" %(nowPage, index + 1))
                    logging.error("爬取第 %s 頁第 %s 筆資料發生錯誤" %(nowPage, index + 1))
                    traceback.print_exc()
            if end == True:
                break
            nowPage += 1
        except:
            print("爬取第 %s 頁主旨發生錯誤" %(nowPage))
            logging.error("爬取第 %s 頁主旨發生錯誤" %(nowPage))
            traceback.print_exc()
        
    result['titles_result'] = titles_result
    result['dates'] = dates
    result['links'] = links
    result['nowPage'] = nowPage
    result['index'] = index + 1
    return result

        


# In[49]:


def parsingTitle(soup, checkRange):
    try:
        # 取得上次爬網結果
        lastResultPath = "./CrawlList/lastResult.csv"
        if os.path.isfile(lastResultPath):
            lastResult = pd.read_csv(lastResultPath)
        else:
            lastResult = pd.DataFrame()
        
        # 爬網日期區間為一個禮拜
        endDate = datetime.date.today()
        strDate = (endDate - datetime.timedelta(days = checkRange)).isoformat()
        df = pd.DataFrame(columns = ["date", "title", "link"])
        
        # 資料處理
        result = dataProcess_Title(soup, strDate)
        
        d = {'date': result['dates'], 'title': result['titles_result'], 'link': result['links']}
        df = df.append(pd.DataFrame(data = d))    
        # 若與上次發文日期和標題相同，則跳至下一筆
        if not lastResult.empty:
            for index, row in df.iterrows():
                if (row['date'] in list(lastResult.date)) and (row['title'] in list(lastResult.title)):
                    df.drop(index, inplace = True)
                    
        if len(df) == 0:
            print("%s 至 %s 間無資料更新" %(strDate, endDate))
            logging.critical("%s 至 %s 間無資料更新" %(strDate, endDate))
        else:
            df.index = [i for i in range(df.shape[0])] # reset
            outputCsv(df, "lastResult", "./CrawlList")
        
    except:
        print("爬取主旨列表失敗")
        logging.error("爬取主旨列表失敗")
        traceback.print_exc()
    return df
    


# In[50]:


def request2soup(url, page = None):
    if page is None:
        res = requests.get(url)
    else:
        formData = {'id':'190',
                'contentid':'190',
                'parentpath':'0,3',
                'mcustomize':'lawnew_list.jsp',
                'keyword':'請輸入查詢關鍵字',           
                'page':page} 
        res = requests.post(url, data = formData)
        
    res.encoding = "utf-8"
    soup = BeautifulSoup(res.text, "html.parser", from_encoding = "utf-8")
    return soup


# In[51]:


def main(url, checkRange = 18):
    
    logging.critical("\n")
    logging.critical("爬網開始......")
    logging.critical("目標網址：" + url)
    
    strTime = datetime.datetime.now()
    logging.critical("開始時間：" + strTime.strftime("%Y/%m/%d %H:%M:%S"))
    try:
        soup = request2soup(url, 1)
        df_1 = parsingTitle(soup, checkRange)
        if len(df_1) == 0:
            return
        outputCsv(df_1, "第一層結果", FinalPath)

        df_2 = parsingDetail(df_1, FinalPath)
        outputCsv(df_2, "第二層結果", FinalPath)
    except:
        print("執行爬網作業失敗")
        logging.error("執行爬網作業失敗")
        traceback.print_exc()
        
    endTime = datetime.datetime.now()
    logging.critical("結束時間：" + endTime.strftime("%Y/%m/%d %H:%M:%S"))
    logging.critical("執行時間：" + str((endTime - strTime).seconds) + " 秒")
    logging.critical("輸出筆數：" + str(len(df_1)) + " 筆")
    logging.critical("爬網結束......")


# In[52]:


if __name__ == "__main__":
    url = "https://www.banking.gov.tw/ch/home.jsp?id=190&parentpath=0,3"
    main(url)

