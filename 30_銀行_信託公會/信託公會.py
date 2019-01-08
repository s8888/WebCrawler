#!/usr/bin/env python
# coding: utf-8
'''
Project Name: rootlaw project
Crawl Cycle: Daily
Main Website: http://www.trust.org.tw/home/WebNewsList.asp?pno=56
author      : 林德昌
date        : 2018/12/17
description : 爬取每日最新消息
'''
# In[62]:


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


# In[63]:


def downloadFile(finalPath, title, fileUrls, fileNames): # for download pdf or doc
    target = finalPath + '/' + title[:30].strip()
    # 若目錄不存在，建立目錄
    if not os.path.isdir(target):
        os.makedirs(target)
    for file_url, fileName in zip(fileUrls, fileNames):
        try:
            logging.info(fileName.strip())
            response = requests.get(file_url, stream="TRUE")
            downloadFile = target + '/' + fileName.strip() + os.path.splitext(file_url)[-1]# 放置資料夾路徑 + 檔名 + 副檔名
            logging.info(downloadFile)
            with open(downloadFile,'wb') as file:
                for data in response.iter_content():
                    file.write(data)
        except:
            logging.error("爬取檔案失敗")
            logging.error("失敗連結：" + file_url)
            logging.error('title:' + title)


# In[64]:


def dataProcess_Detail(soup, row):
    totalContent = [e.text for e in soup.select('.content .content div')][0]
    serno = re.findall(r'發文字號.+?\d+.?', totalContent)
    result = dict()
    if not bool(serno):
        return result
    fileUrlRoot = 'http://www.trust.org.tw'
    result['fileUrls'] = [re.sub('\.\.',fileUrlRoot,e.get('href')) for e in soup.select('.content .content .content a')]
    result['fileNames'] = [e.text for e in soup.select('.content .content b')]
    result['content'] = re.sub('[\s|\n]', '', re.findall(r'說明：\s[\S|\n|\xa0]+正本', totalContent)[0][3:-2])
    result['abstract'] = re.sub('\xa0 ', ' ', re.findall(r'主旨：.+', totalContent)[0])[3:]
    result['serno'] = re.findall(r'發文字號.+?\d+.?', totalContent)[0][5:]
    result['issue_date'] = re.findall(r'發文日期.+?日', totalContent)[0][5:]
    return result


# In[65]:


def parsingDetail(df, finalPath): 
    df2 = pd.DataFrame(columns = ["標題", "全文內容", "附件", "發文字號", "發文日期", "相關法條"])
    for index, row in df.iterrows():
        try:
            title = row['標題']
            link = row['內文連結']
            logging.info(title)
            soup = request2soup(link)
            result = dataProcess_Detail(soup, row)
            if not bool(result):
                continue
            fileNames = result['fileNames'] 
            if len(fileNames) != 0:
                downloadFile(finalPath, title, result['fileUrls'], fileNames)
            d = {'標題': title, '全文內容': result['content'], '附件':','.join(fileNames), '發文字號':result['serno'], 
                 '發文日期':result['issue_date'], '相關法條':''}
            df2= df2.append(d, ignore_index=True)
        except:
            logging.error("爬取內文失敗\n")
            logging.error("失敗連結：" + link+ '\n')
            traceback.print_exc()
    return df2


# In[66]:


def outputCsv(df, fileName, path):
    # 若目錄不存在，建立目錄
    if not os.path.isdir(path):
        os.mkdir(path)
    df.to_csv(path + "/" + fileName + ".csv", index = False, encoding = "utf_8_sig")


# In[67]:


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
        return 1
    elif strDate == endDate:
        return 0
    else:
        return -1


# In[68]:


def dataProcess_Title(soup, strDate):
    result = dict()
    titles_result = []
    dates = []
    links = []
    end = False
    nowPage = 1
    try:
        titles = [str(e.get('title')) for e in soup.select('.content-link')]
        datelist = []
        for e in soup.select('.content-2')[:-2]:
            if re.findall(r'\d+\/\d+\/\d+', e.text):
                datelist.append(e.text)
        for index in range(len(titles)):
            try:
                title = titles[index]
                date = datelist[index]
                if compareTo(date, strDate) > 0:
                    end = True
                    break
                link = re.sub('\.\.','http://www.trust.org.tw', soup.select('.content-link')[index].get('href'))
                titles_result.append(title)
                dates.append(date)
                links.append(link)
            except:
                logging.error("爬取第 %s 頁第 %s 筆主旨發生錯誤" %(nowPage, index + 1))
    except:
        logging.error("爬取第 %s 頁主旨發生錯誤" %(nowPage))
        traceback.print_exc()
        
    result['titles_result'] = titles_result
    result['dates'] = dates
    result['links'] = links
    return result


# In[69]:


def parsingTitle(soup, checkRange):
    
    try:
        # 取得上次爬網結果
        if os.path.isfile(lastResultPath):
            lastResult = pd.read_csv(lastResultPath)
        else:
            lastResult = pd.DataFrame()
            
        endDate = datetime.date.today()
        strDate = (endDate - datetime.timedelta(days = checkRange)).isoformat()
        df = pd.DataFrame(columns=['爬網日期','發文日期','標題','內文連結'])
        
         # 資料處理
        result = dataProcess_Title(soup, strDate)   
        d = {'爬網日期':endDate,'發文日期': result['dates'], '標題': result['titles_result'], '內文連結': result['links']}
        df = df.append(pd.DataFrame(data=d))
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


# In[70]:


def request2soup(url):
    res = requests.get(url)
    res.encoding = 'big5'
    soup = BeautifulSoup(res.text,'html.parser',from_encoding='utf-8')
    return soup


# In[71]:


def main(url, checkRange = 50):
    
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


# In[72]:


if __name__ == "__main__":
    url = 'http://www.trust.org.tw/home/webnewslist.asp?pno=56'
    main(url)

