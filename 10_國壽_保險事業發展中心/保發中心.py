#!/usr/bin/env python
# coding: utf-8
'''
Project Name: tii project
Crawl Cycle: Daily
Main Website: http://law.tii.org.tw/Fn/Onews.asp
author      : 林德昌
date        : 2019/01/14
description : 抓取每天發布的最新訊息
'''
# In[49]:


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


# In[50]:


def downloadFile(finalPath, title, fileUrls, fileNames): # for download pdf or doc
    target = finalPath + '/' + title[:30].strip()
    # 若目錄不存在，建立目錄
    if not os.path.isdir(target):
        os.makedirs(target)
    for file_url, fileName in zip(fileUrls, fileNames):
        try:
            response = requests.get(file_url, stream="TRUE")
            downloadFile = target + '/' + fileName.strip()# 放置資料夾路徑 + 檔名
            logging.info(downloadFile + '\r\n')
            with open(downloadFile,'wb') as file:
                for data in response.iter_content():
                    file.write(data)
        except:
            logging.error("爬取檔案失敗")
            logging.error("失敗連結：" + file_url)
            traceback.print_exc()


# In[51]:


def dataProcess_Detail(soup, title_type):
    result = dict()
    fileUrlRoot = 'http://law.tii.org.tw/Fn/'
    str_content = [e.text for e in soup.select('pre')][0]
    if title_type == '行政函釋':
        contentlist = re.findall('主 旨：.+', re.sub('\s+',' ', str_content))
        if bool(contentlist):
            content = contentlist[0]
        else:
            content = re.findall('一、.+', re.sub('\s+',' ', str_content))[0]
        serno = re.findall(r'發文字號.+?\d+.+', str_content)[0][5:-1]
        issue_date = re.findall(r'發文日期.+?\d+.+', str_content)[0][5:-1]
    else:
        content = re.sub('\s','', str_content)
        serno = re.findall('日\S+?號[令|函|公告]', content)[0][1:]
        issue_date = re.findall('民國.+?日', str_content)[0]
        
    result['fileNames'] = [e.text for e in soup.select('font font a')]
    logging.info(result['fileNames'])
    result['fileUrls'] = [re.sub(r'\.\/', fileUrlRoot, e.get('href')) for e in soup.select('font font a')]
    result['content'] = content
    result['serno'] = serno
    result['issue_date'] = issue_date
    return result


# In[52]:


def parsingDetail(df, finalPath): 
    df2 = pd.DataFrame(columns = ["標題", "全文內容", "附件", "發文字號", "發文日期", "相關法條", "標題種類"])
    for index, row in df.iterrows():
        try:
            title = row['標題']
            logging.info(title)
            link = row['內文連結']
            title_type = row['標題種類']
            soup = request2soup(link)
            result = dataProcess_Detail(soup, title_type)

            fileNames = result['fileNames'] 
            if len(fileNames) != 0:
                downloadFile(finalPath, title, result['fileUrls'], fileNames)
            d = {'標題': title, '全文內容': result['content'], '附件':','.join(fileNames), '發文字號':result['serno'], '發文日期':result['issue_date'],
                 '相關法條':'', '標題種類':title_type}
            df2= df2.append(d, ignore_index=True)
        except:
            logging.error("爬取內文失敗")
            logging.error("失敗連結：" + link)
            traceback.print_exc()
    return df2


# In[53]:


def outputCsv(df, fileName, path):
    # 若目錄不存在，建立目錄
    if not os.path.isdir(path):
        os.mkdir(path)
    df.to_csv(path + "/" + fileName + ".csv", index = False, encoding = "utf_8_sig")


# In[54]:


def _add1911(matched):
    intStr = matched.group("number"); 
    intValue = int(intStr);
    addedValue = intValue + 1911; 
    addedValueStr = str(addedValue);
    return addedValueStr;


# In[55]:


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


# In[56]:


def dataProcess_Title(strDate):
    result = dict()
    nowPage = 1
    preurl = 'http://law.tii.org.tw/Fn/'
    titles_result = []
    dates = []
    links = []
    title_types = []
    end = False
    while True:
        try:
            url = 'http://law.tii.org.tw/Fn/Onews.asp?pg=' + str(nowPage)
            soup = request2soup(url)
            titles = [e.text for e in soup.select('td[height="25"] font a') if bool(e.text)]
            if not bool(titles):
                break
            hrefs = [preurl + e.get('href') for e in soup.select('td[height="25"] font a') if bool(e.text)]
            rawDates = [e.text.strip() for e in soup.select('div font')[1:]]
            rawTitle_type = [e.text.strip() for e in soup.select('td[width="12%"] font')]
            for index in range(len(titles)):
                try:
                    title = titles[index]
                    date = rawDates[index]
                    if compareTo(date, strDate) > 0: # 若發文日期小於開始日期, 則結束爬取主旨
                        end = True
                        break
                    link = hrefs[index]
                    title_type = rawTitle_type[index]
                    titles_result.append(title)
                    dates.append(date)
                    links.append(link)
                    title_types.append(title_type)
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
    result['nowPage'] = nowPage
    result['crawl_date'] = datetime.date.today()
    result['title_type'] = title_types 
    return result

        


# In[57]:


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
        df = pd.DataFrame(columns = ["爬網日期","發文日期", "標題", "標題種類","內文連結"])
        
        # 資料處理
        result = dataProcess_Title(strDate)
        
        d = {'爬網日期':endDate, '發文日期': result['dates'], '標題': result['titles_result'], '標題種類': result['title_type'],'內文連結': result['links']}
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
    


# In[58]:


def request2soup(url):
    res = requests.get(url)
    res.encoding = 'big5'
    soup = BeautifulSoup(res.text,'html.parser',from_encoding='utf-8')
    return soup


# In[59]:


def main(url, checkRange = 60):
    
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


# In[60]:


if __name__ == "__main__":
    url = "http://law.tii.org.tw/Fn/Onews.asp"
    main(url)

