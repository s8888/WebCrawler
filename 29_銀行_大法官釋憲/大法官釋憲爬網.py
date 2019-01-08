#!/usr/bin/env python
# coding: utf-8
'''
Project Name: moj project
Crawl Cycle: Daily
Main Website: https://www.judicial.gov.tw/constitutionalcourt/p03_0001.asp
author      : 林德昌
date        : 2018/12/17
description : 抓取大法官釋憲每天發布的最新消息
'''
# In[1]:


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


# In[2]:


def downloadFile(finalPath, title, fileUrls, fileNames): # for download pdf or doc
    target = finalPath + '/' + title[:30].strip()
    # 若目錄不存在，建立目錄
    if not os.path.isdir(target):
        os.makedirs(target)
    for file_url, fileName in zip(fileUrls, fileNames):
        try:
            response = requests.get(file_url, stream="TRUE")
            # 放置資料夾路徑 + 檔名 + 副檔名
            downloadFile = target + '/' + fileName.strip() + re.findall(r'\..+', response.headers['Content-Disposition'])[0]
            logging.info(downloadFile + '\r\n')
            with open(downloadFile,'wb') as file:
                for data in response.iter_content():
                    file.write(data)
        except:
            logging.error("爬取檔案失敗")
            logging.error("失敗連結：" + file_url)


# In[3]:


def dataProcess_Detail(soup):
    result = dict()
    fileUrlRoot = 'https://www.judicial.gov.tw/constitutionalcourt/'
    result['fileNames'] = [e.text for e in soup.select('.content_table a')]
    logging.info(result['fileNames'])
    result['fileUrls'] = [fileUrlRoot + e.get('href') for e in soup.select('.content_table a')]
    result['content0'] = [e.text.strip().replace('\xa0','').replace('\r\n\t\t\t\t\t','') for e in soup.select('.content_table td')][0] # 解釋字號
    result['content1'] = [e.text.strip().replace('\xa0','').replace('\r\n\t\t\t\t\t','') for e in soup.select('.content_table td')][1] # 解釋公布院令
    result['content2'] = [e.text.strip() for e in soup.select('.content_table td')][2] # 解釋爭點
    result['content3'] = [e.text.strip().replace('\u3000','') for e in soup.select('.content_table td')][3] # 解釋文
    result['content4'] = [e.text.strip().replace('\u3000','') for e in soup.select('.content_table td')][4] # 理由書
    result['issue_date'] = re.findall(r'中華民國.+?日', result['content1'])[0]
    
    return result


# In[4]:


def parsingDetail(df, finalPath): 
    df2 = pd.DataFrame(columns = ["標題", "發文日期", "附件", "發文字號(解釋字號)","解釋公布院令","解釋爭點", "解釋文", "理由書", "相關法條"])
    for index, row in df.iterrows():
        try:
            title = row['標題']
            logging.info(title)
            link = row['內文連結']
            soup = request2soup(link)
            result = dataProcess_Detail(soup)
            fileNames = result['fileNames'] 
            if len(fileNames) != 0:
                downloadFile(finalPath, title, result['fileUrls'], fileNames)
            d = {'標題': title, '發文日期':result['issue_date'], '附件':','.join(fileNames), '發文字號(解釋字號)':result['content0'],
                 "解釋公布院令":result['content1'], '解釋爭點':result['content2'], "解釋文": result['content3'], '理由書':result['content4'],
                 '相關法條':''}
            df2= df2.append(d, ignore_index=True)
        except:
            logging.error("爬取內文失敗")
            logging.error("失敗連結：" + link)
            traceback.print_exc()
    return df2


# In[5]:


def outputCsv(df, fileName, path):
    # 若目錄不存在，建立目錄
    if not os.path.isdir(path):
        os.mkdir(path)
    df.to_csv(path + "/" + fileName + ".csv", index = False, encoding = "utf_8_sig")


# In[6]:


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


# In[7]:


def parsingTitle(soup, checkRange):
    try:
        # 取得上次爬網結果
        if os.path.isfile(lastResultPath):
            lastResult = pd.read_csv(lastResultPath)
        else:
            lastResult = pd.DataFrame()
        
        # 爬網日期區間為一個禮拜
        endDate = datetime.date.today()
        strDate = (endDate - datetime.timedelta(days = checkRange)).isoformat()
        
        nowPage = 1
        df = pd.DataFrame(columns=['爬網日期','發文日期','標題','內文連結','大法官解釋字號'])
        while True:
            index = 0 # css selector上的資料序號
            while True:
                try:
                    title = [e.text.strip().replace('\r\r\n','') for e in soup.select('#AutoNumber2 td[width="84%"] p font')][index]
                    if title == []:  # 當title抓不到東西的時候代表抓完資料了
                        break
                    link = [('https://www.judicial.gov.tw/constitutionalcourt/' + e.get('href')) for e in soup.select('#AutoNumber2 td[width="16%"] p a')][index]
                    date = [re.findall(r'\(.+\)', e.text)[0][1:-1] for e in soup.select('#AutoNumber2 td[width="16%"] p a')][index]
                    serno = [e.text for e in soup.select('#AutoNumber2 td[width="16%"] p a')][index]
                    if compareTo(date, strDate) > 0:
                        break
                    tempDf = {'爬網日期': endDate, '發文日期': date,'標題': title, '內文連結': link, '大法官解釋字號':serno}
                    df = df.append(tempDf, ignore_index=True)
                    index += 1
                except:
                    logging.error("爬取第 %s 頁第 %s 筆主旨發生錯誤" %(nowPage, index + 1))
                    traceback.print_exc()
            if compareTo(date, strDate) > 0:   # 當日期起始大於發文日期時就停止
                break
            nowPage += 1
            soup = request2soup(url + "&TPage={}".format(nowPage))  
            
        df.index = [i for i in range(df.shape[0])] # reset Index 
        
        
        if not lastResult.empty:
            for index, row in df.iterrows():
                if (row['發文日期'] in list(lastResult['發文日期'])) and (row['大法官解釋字號'] in list(lastResult['大法官解釋字號'])):
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


# In[8]:


def request2soup(url):
    res = requests.get(url)
    res.encoding = 'big5'
    soup = BeautifulSoup(res.text,'html.parser')
    return soup


# In[9]:


def main(url, checkRange = 35):
    
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


# In[10]:


if __name__ == "__main__":
    url = "https://www.judicial.gov.tw/constitutionalcourt/p03_0001.asp"
    main(url)


# In[ ]:




