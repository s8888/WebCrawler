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
# In[81]:


import requests
from bs4 import BeautifulSoup
import pandas as pd
import header
import logging
import re
import datetime
import traceback # 印log
import os
import ssl
import csv
TempPath = "./Temp"  # browser file
FinalPath = "./Result" # project file
lastResultPath = "./CrawlList/lastResult.csv"


# In[82]:


def downloadFile(finalPath, title, fileUrls, fileNames): # for download pdf or doc
    target = finalPath + '/' + title[:30].strip()
    # 若目錄不存在，建立目錄
    if not os.path.isdir(target):
        os.makedirs(target)
    for file_url, fileName in zip(fileUrls, fileNames):
        try:
            logging.info(fileName.strip())
            response = requests.get(file_url, stream="TRUE")
            downloadFile = target + '/' + fileName.strip() # 放置資料夾路徑 + 檔名
            logging.info(downloadFile)
            with open(downloadFile,'wb') as file:
                for data in response.iter_content():
                    file.write(data)
        except:
            logging.error("爬取檔案失敗")
            logging.error("失敗連結：" + file_url)
            logging.error('title:' + title)


# In[83]:


def dataProcess_Detail(soup, row):
    urlmoj = 'https://law.moj.gov.tw' # linkto 全國法規資料庫
    urlGazette = 'http://gazette.nat.gov.tw' # linkto 行政院公報資訊網
    fileUrlRoot_moj = 'https://law.moj.gov.tw/news/'
    urlNTPC = 'http://web.law.ntpc.gov.tw' # linkto 新北市政府網站
    title = row['標題']
    link = row['內文連結']
    result = dict()
    if link.find(urlmoj) >= 0:
        rawcontent = [e.text for e in soup.select('.text-pre')]
        content = rawcontent[0] if bool(rawcontent) else ''
        fileNames = [e.text for e in soup.select('#litFile a')]
        fileUrls = [urlmoj + '/' + e.get('href') for e in soup.select('#Content a')]
        serno = ''
        issue_date = [e.text for e in soup.select('td')][0]
    elif link.find(urlGazette) >= 0:
        fileUrls = [urlGazette + e.get('src') for e in soup.select('.embed-responsive-item')]
        fileNames = [title + '.pdf'for i in range(len(fileUrls))]
        content = ''
        serno = soup.select('div.Item section.Block p span')[2].text
        issue_date = soup.select('div.Item section.Block p span')[1].text
    elif link.find('NewsContent.aspx') >= 0: # 其他地方政府網站內文
        NotFoundmsg = [e.text for e in soup.select('.text-danger')]  
        if NotFoundmsg != []:  # 若查詢結果為錯誤訊息，則回傳空的dict
            return result
        pattern = re.compile(r'http(\:|\w|\.|\-|\/)+\/')
        m = pattern.match(link)
        content = [e.text.strip() for e in soup.select('.ClearCss')]
        content = '' if not bool(content) else content[0]
        fileUrls = [m.group(0) + e.get('href') for e in soup.select('#ctl00_cp_content_ulAnnFiles02 a')]
        fileNames = [e.text for e in soup.select('#ctl00_cp_content_ulAnnFiles02 a')]
        serno = [e.text.strip() for e in soup.select('#ctl00_cp_content_trODWord td')][0]
        issue_date = [e.text.strip() for e in soup.select('#ctl00_cp_content_trAnnDate td')]
    elif link.find(urlNTPC) >= 0:  
        fileUrls = [e.get('href').replace('./', 'http://web.law.ntpc.gov.tw/fn/') for e in soup.select('#Law-Content a')][1:]
        fileNames = [e.text for e in soup.select('#Law-Content a')][1:]
        content = ''
        serno = ''
        issue_date = [e.text.strip() for e in soup.select('td b')][0]
        if fileNames == []:
            contents = [e.text for e in soup.select('.worddisaplay')][0]
    else:
        logging.info('出現新的連結網站，需要新增爬蟲程式')
    result['fileUrls'] = fileUrls
    result['fileNames'] = fileNames 
    result['content'] = content
    result['serno'] = serno
    result['issue_date'] = issue_date
    return result


# In[84]:


def parsingDetail(df, finalPath): 
    df2 = pd.DataFrame(columns = ["標題", "全文內容", "附件", "發文字號", "發文日期", "相關法條"])
    for index, row in df.iterrows():
        try:
            title = row['標題']
            link = row['內文連結']
            logging.info(title)
            soup = request2soup(link)
            result = dataProcess_Detail(soup, row)
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


# In[85]:


def outputCsv(df, fileName, path, index=False, encoding="utf_8_sig"):
    # 若目錄不存在，建立目錄
    if not os.path.isdir(path):
        os.mkdir(path)
    # 20190916 新增參數 quoting=csv.QUOTE_NONNUMERIC 以前後雙引號(")包住字串欄位
    df.to_csv(os.path.join(path, fileName+".csv"), index=index, encoding=encoding, quoting=csv.QUOTE_NONNUMERIC)


# In[86]:


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


# In[87]:


def dataProcess_Title(soup, strDate):
    result = dict()
    titles_result = []
    dates = []
    links = []
    nowPage = 1
    end = False
    while True:
        try:
            if nowPage == 2: # TODO
                break
            if nowPage > 1:
                url = 'https://www.moj.gov.tw/lp-22-001-' + str(nowPage) +'-20.html'
                soup = request2soup(url)
            titles = [str(e.get('title')) for e in soup.select('.list a')]
            if titles == []:
                break
#             for index in range(len(titles)):
            for index in range(15): #TODO
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
                    logging.error("爬取第 %s 頁第 %s 筆主旨發生錯誤" %(nowPage, index + 1))
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
    result['index'] = index + 1
    return result


# In[88]:


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


# In[89]:


def request2soup(url):
    res = requests.get(url)
    res.encoding = 'utf-8'
    soup = BeautifulSoup(res.text,'html.parser',from_encoding='utf-8')
    return soup


# In[90]:


def main(url, checkRange = 5):
    
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


# In[91]:


if __name__ == "__main__":
    url = 'https://www.moj.gov.tw/lp-22-001-1-20.html'
    main(url)


# In[79]:


url = 'http://web.law.ntpc.gov.tw/fn/ShowNews.asp?id=5261'
soup = request2soup(url)

