#!/usr/bin/env python
# coding: utf-8
'''
Project Name: lia-roc project
Crawl Cycle: Daily
Main Website: http://www.lia-roc.org.tw/index06.asp?mu=show1
author      : 林德昌
date        : 2019/01/10
description : 抓取壽險公會每天發布的最新消息
'''
# In[13]:


import requests
from bs4 import BeautifulSoup
import pandas as pd
import header
import logging
import re
import datetime
import traceback # 印log
import os
import csv
TempPath = "./Temp"  # browser file
FinalPath = "./Result" # project file
lastResultPath = "./CrawlList/lastResult.csv"


# In[14]:


def downloadFile(finalPath, title, fileUrls, fileNames): # for download pdf or doc
    target = finalPath + '/' + title[:30].strip()
    # 若目錄不存在，建立目錄
    if not os.path.isdir(target):
        os.makedirs(target)
    for file_url, fileName in zip(fileUrls, fileNames):
        try:
            response = requests.get(file_url, stream="TRUE")
            downloadFile = target + '/' + fileName.strip() + os.path.splitext(file_url)[-1]# 放置資料夾路徑 + 檔名
            logging.info(downloadFile + '\r\n')
            with open(downloadFile,'wb') as file:
                for data in response.iter_content():
                    file.write(data)
        except:
            logging.error("爬取檔案失敗")
            logging.error("失敗連結：" + file_url)
            traceback.print_exc()


# In[15]:


def dataProcess_Detail(link, year):
    linktype = os.path.splitext(link)[-1]
    result = dict()
    if linktype == '.pdf':
        fileUrls = [link]
        serno = ''
        fileNames = []
        content = ''
        issue_date = ''
    else:
        fileUrlRoot = 'http://www.lia-roc.org.tw/index06/regulation/' + str(year) + 'regu/'
        soup = request2soup(link)
        serno = [e.text for e in soup.select('.MsoDate span')][1]
        fileNames = [''.join(re.findall('\S+', e.text)) for e in soup.select('.MsoNormal')][1:-1]
        fileUrls = list(set([fileUrlRoot + e.get('href') for e in soup.select('p a')]))
        logging.info(fileNames)
        issue_date = [e.text.strip() for e in soup.select('.MsoDate span')][0] # 發文日期
        content = ''
    result['serno'] = serno
    result['fileNames'] = fileNames if bool(fileNames) else []
    result['fileUrls'] = fileUrls
    result['issue_date'] = issue_date
    result['content'] = content
    return result


# In[16]:


# df = pd.DataFrame(columns = ["爬網日期","發文日期", "標題", "內文連結"])
def parsingDetail(df, finalPath): 
    df2 = pd.DataFrame(columns = ["標題", "發文日期", "附件", "發文字號","本文", "相關法條"])
    for index, row in df.iterrows():
        try:
            title = row['標題']
            titlefileName = re.sub(r'\(\d{1,2}\/\d{1,2}\)', '', title)
            logging.info(title)
            link = row['內文連結']
            year = row['發文年度']
            result = dataProcess_Detail(link, year)
            if not bool(result):
                continue
            if bool(result['fileUrls']):
                fileNames = result['fileNames'] if bool(result['fileNames']) else [titlefileName]
            else:
                fileNames = []
            logging.info(fileNames)
            if len(fileNames) != 0:
                downloadFile(finalPath, titlefileName, result['fileUrls'], fileNames)
            d = {'標題': title, '發文日期':result['issue_date'], '附件':','.join(fileNames), '發文字號':result['serno'],
                 "本文":result['content'], '相關法條':''}
            df2= df2.append(d, ignore_index = True)
        except:
            logging.error("爬取內文失敗")
            logging.error("失敗連結：" + link)
            traceback.print_exc()
    return df2


# In[17]:


def outputCsv(df, fileName, path, index=False, encoding="utf_8_sig"):
    # 若目錄不存在，建立目錄
    if not os.path.isdir(path):
        os.mkdir(path)
    # 20190916 新增參數 quoting=csv.QUOTE_NONNUMERIC 以前後雙引號(")包住字串欄位
    df.to_csv(os.path.join(path, fileName+".csv"), index=index, encoding=encoding, quoting=csv.QUOTE_NONNUMERIC)


# In[18]:


def _add1911(matched):
    intStr = matched.group("number"); 
    intValue = int(intStr);
    addedValue = intValue + 1911; 
    addedValueStr = str(addedValue);
    return addedValueStr;


# In[19]:


def compareTo(strDate, endDate):
    strDate = re.sub(r'(/|-|\.)', '-', strDate)
    endDate = re.sub(r'(/|-|\.)', '-', endDate)
    if int(re.split('-', strDate)[0]) < 1911:
        strDate = re.sub("(?P<number>\d+)", _add1911, strDate, 1)
    if int(re.split('-', endDate)[0]) < 1911:
        endDate = re.sub("(?P<number>\d+)", _add1911, endDate, 1)
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


# In[20]:


def dataProcess_Title(strDate, lastResult, endDate):
    result = dict()
    year = 108
    preurl = 'http://www.lia-roc.org.tw/index06/regulation/'
    titles_result = []
    dates = []
    links_result = []
    years = []
    end = False
    error_count = 0
    while True:
        try:
            url = 'http://www.lia-roc.org.tw/index06/regulation/' + str(year) + 'regu.htm'
            soup = request2soup(url)
            titlefilters = []
            for e in soup.select('p')[2:]:
                for title in re.split(r'\n{2,4}', e.text):
                    if len(title) != 0:
                        titlefilters.append(re.sub('\s', '', title)) # 抓取標題
                        
            links = []
            for e in soup.select('a'):
                if e.get('href').find(str(year)) >= 0:
                    links.append(preurl + e.get('href'))  # 抓取內文連結
                    
            if len(links) != len(titlefilters):
                logging.error('標題個數和內文連結個數不相符，爬取第一層資料失敗')
                break
            
            for index in range(len(titlefilters)):
                try:
                    title = titlefilters[index]
                    dateWithoutYear = re.findall(r'\(\d{1,2}\/\d{1,2}\)', title)[0][1:-1]
                    date = str(year) + '/' + dateWithoutYear
                    if compareTo(endDate.isoformat(), date) > 0:
                        date = str(year - 1) + '/' + dateWithoutYear
                    if compareTo(date, strDate) > 0 and not bool(re.findall('補登', title)):
                        end = True
                        if error_count == 10:
                            break
                        error_count += 1
                    link = links[index]
                    titles_result.append(title)
                    dates.append(date)
                    links_result.append(link)
                    years.append(year)
                except:
                    logging.error("爬取 %s 年度第 %s 筆資料發生錯誤" %(year, index + 1))
                    traceback.print_exc()
            if end == True:
                break
            year -= 1
        except:
            logging.error("爬取 %s 年度主旨發生錯誤" %(year))
            traceback.print_exc()
    
    result['titles_result'] = titles_result
    result['dates'] = dates
    result['links'] = links_result
    result['years'] = years
    return result


# In[21]:


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
        df = pd.DataFrame(columns = ["爬網日期","發文日期", "標題", "內文連結", "發文年度"])
        
        # 資料處理
        result = dataProcess_Title(strDate, lastResult, endDate)
        
        d = {'爬網日期':endDate, '發文日期': result['dates'], '標題': result['titles_result'], '內文連結': result['links'], '發文年度':result['years']}
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
    


# In[22]:


def request2soup(url):
    res = requests.get(url)
    res.encoding = 'big5'
    soup = BeautifulSoup(res.text,'html.parser',from_encoding='utf-8')
    return soup


# In[23]:


def main(url, checkRange = 10):
    
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


# In[24]:


if __name__ == "__main__":
    url = "http://www.lia-roc.org.tw/index06.asp?mu=show1"
    main(url)

