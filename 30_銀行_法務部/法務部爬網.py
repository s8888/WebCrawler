#!/usr/bin/env python
# coding: utf-8
'''
Project Name: bankingGov project
Crawl Cycle: Daily
Main Website: https://www.moj.gov.tw/lp-22-001-1-20.html
author      : 林德昌
date        : 2018/10/16
description : 抓取法務部每天發布的最新消息
'''
# In[1]:


import requests
from bs4 import BeautifulSoup
import pandas as pd
import header
import logging
import re
import sys
import datetime
import pandas as pd
import traceback # 印log
import os
TempPath = "./Temp"  # browser file
FinalPath = "./Result" # project file


# In[2]:


def downloadFile(finalPath, title, fileUrls, fileNames): # for download pdf or doc
    target = finalPath + '/' + title[:30].strip()
    # 若目錄不存在，建立目錄
    if not os.path.isdir(target):
        os.makedirs(target)
    i = 0
    for file_url, fileName in zip(fileUrls, fileNames):
        try:
            print(fileName.strip())
            response = requests.get(file_url, stream="TRUE")
            downloadFile = target + '/' + fileName.strip() # 放置資料夾路徑 + 檔名
            print(downloadFile)
            with open(downloadFile,'wb') as file:
                for data in response.iter_content():
                    file.write(data)
        except:
            print("爬取檔案失敗")
            print("失敗連結：" + file_url)
            print('title:' + title)
            logging.error("爬取檔案失敗")
            logging.error("失敗連結：" + file_url)
        i += 1


# In[3]:


def dataProcess_Detail(soup, row, urlmoj, urlGazette, fileUrlRoot_moj):
    title = row['title']
    link = row['link']
    date = row['date']
    result = dict()
    if link.find(urlmoj) >= 0:
        result['content'] = [e.text for e in soup.select('pre')][0]
        result['fileNames'] = [e.text for e in soup.select('#Content a')][3:]
        result['fileUrls'] = [fileUrlRoot_moj + e.get('href') for e in soup.select('#Content a')][3:]
        result['serno'] = ''
    elif link.find(urlGazette) >= 0:
        result['fileUrls'] = [urlGazette + e.get('src') for e in soup.select('.embed-responsive-item')]
        result['fileNames'] = [title + '.pdf'for i in range(len(result['fileUrls']))]
        result['content'] = ''
        result['serno'] = soup.select('div.Item section.Block p span')[2].text
    else:
        print('new Web Type!!')
    return result


# In[4]:


def parsingDetail(df, finalPath): 
    urlmoj = 'https://law.moj.gov.tw' # linkto 全國法規資料庫
    urlGazette = 'http://gazette.nat.gov.tw' # linkto 行政院公報資訊網
    fileUrlRoot_moj = 'https://law.moj.gov.tw/news/'
    df2 = pd.DataFrame(columns = ["標題", "全文內容", "附件", "發文字號", "發文日期", "相關法條"])
    for index, row in df.iterrows():
        title = row['title']
        link = row['link']
        date = row['date']
        try:
            soup = request2soup(link)
            result = dataProcess_Detail(soup, row, urlmoj, urlGazette, fileUrlRoot_moj)
        except:
            print("爬取內文失敗\n")
            print("失敗連結：" + link + '\n')
            logging.error("爬取內文失敗")
            logging.error("失敗連結：" + link)
            traceback.print_exc()
        fileNames = result['fileNames'] 
        if len(fileNames) != 0:
            downloadFile(finalPath, title, result['fileUrls'], fileNames)
        d = {'標題': title, '全文內容': result['content'], '附件':','.join(fileNames), '發文字號':result['serno'], 
             '發文日期':date, '相關法條':''}
        df2= df2.append(pd.DataFrame(data=d, index=[0]))
    return df2


# In[5]:


def outputCsv(df, fileName, path):
    # 若目錄不存在，建立目錄
    if not os.path.isdir(path):
        os.mkdir(path)
    df.to_csv(path + "/" + fileName + ".csv", index = False, encoding = "utf_8_sig")


# In[6]:


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


# In[7]:


def dataProcess_Title(soup, strDate):
    result = dict()
    titles_result = []
    dates = []
    links = []
    nowPage = 1
    end = False
    while True:
        try:
            if nowPage > 1:
                url = 'https://www.moj.gov.tw/lp-22-001-' + str(nowPage) +'-20.html'
                soup = request2soup(url)
            titles = [str(e.get('title')) for e in soup.select('.list a')]
            if titles == []:
                break
            for index in range(len(titles)):
                try:
                    title = titles[index]
                    date = soup.select('td[data-title="張貼日"]')[index].text.strip()
                    if compareTo(date, strDate) > 0:
                        end = True
                        break
                    link = soup.select('.list a')[index].get('href')
                    titles_result.append(title)
                    dates.append(date)
                    links.append(link)
                except:
                    print("爬取第 %s 頁第 %s 筆主旨發生錯誤" %(nowPage, index + 1))
                    logging.error("爬取第 %s 頁第 %s 筆主旨發生錯誤" %(nowPage, index + 1))
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


# In[8]:


def parsingTitle(soup, checkRange):
    
    try:
        # 取得上次爬網結果
        lastResultPath = "./CrawlList/lastResult.csv"
        if os.path.isfile(lastResultPath):
            lastResult = pd.read_csv(lastResultPath)
        else:
            lastResult = pd.DataFrame()
            
        endDate = datetime.date.today()
        strDate = (endDate - datetime.timedelta(days = checkRange)).isoformat()
        df = pd.DataFrame(columns=['date','title','link'])
        
         # 資料處理
        result = dataProcess_Title(soup, strDate)   
        
        d = {'date': result['dates'], 'title': result['titles_result'], 'link': result['links']}
        df = df.append(pd.DataFrame(data=d))
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


# In[9]:


def request2soup(url):
    res = requests.get(url)
    res.encoding = 'utf-8'
    soup = BeautifulSoup(res.text,'html.parser',from_encoding='utf-8')
    return soup


# In[10]:


def main(url, checkRange = 10):
    
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
        print("執行爬網作業失敗")
        logging.error("執行爬網作業失敗")
        traceback.print_exc()
    
    endTime = datetime.datetime.now()
    logging.critical("結束時間：" + endTime.strftime("%Y/%m/%d %H:%M:%S"))
    logging.critical("執行時間：" + str((endTime - strTime).seconds) + " 秒")
    logging.critical("輸出筆數：" + str(len(df_1)) + " 筆")
    logging.critical("爬網結束......")


# In[11]:


if __name__ == "__main__":
    url = 'https://www.moj.gov.tw/lp-22-001-1-20.html'
    main(url)

