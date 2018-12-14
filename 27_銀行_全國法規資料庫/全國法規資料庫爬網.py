#!/usr/bin/env python
# coding: utf-8
'''
Project Name: moj project
Crawl Cycle: Daily
Main Website: https://law.moj.gov.tw/News/news_list.aspx?type=olm
author      : 林德昌
date        : 2018/10/16
description : 抓取全國法規資料庫每天發布的最新消息
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


# In[49]:


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
            logging.info(downloadFile + '\r\n')
            with open(downloadFile,'wb') as file:
                for data in response.iter_content():
                    file.write(data)
        except:
            logging.error("爬取檔案失敗")
            logging.error("失敗連結：" + file_url)
            logging.error('title:' + title)


# In[2]:


def dataProcess_Detail(soup, row):
    urlNTPC = 'http://web.law.ntpc.gov.tw' # linkto 新北市政府網站
    urlGazette = 'http://gazette.nat.gov.tw' # linkto 行政院公報資訊網
    urlTaipei = 'http://www.laws.taipei.gov.tw'# linkto 台北市政府網站
    urlmoj = 'https://law.moj.gov.tw' # linkto 全國法規資料庫
    urlmojlaw = 'https://mojlaw.moj.gov.tw' # linkto 法務部
    title = row['標題']
    link = row['內文連結']
    result = dict()
    if link.find(urlNTPC) >= 0:  
        fileUrls = [e.get('href').replace('./', 'http://web.law.ntpc.gov.tw/fn/') for e in soup.select('#Law-Content a')][1:]
        fileNames = [e.text for e in soup.select('#Law-Content a')][1:]
        content = ''
        abstract = ''
        serno = ''
        issue_date = [e.text.strip() for e in soup.select('td b')][0]
        if fileNames == []:
            contents = [e.text for e in soup.select('.worddisaplay')][0]
    elif link.find(urlTaipei) >= 0:
        content = ''
        fileUrls = [e.get('href') for e in soup.select('#ContentPlaceHolder1_gvListF a')]
        serno = [e.text for e in soup.select('#ContentPlaceHolder1_lblCLASSNAME')][0]
        fileNames = [e.text for e in soup.select('#ContentPlaceHolder1_gvListF a')]
        str_content = str(soup.select('#ContentPlaceHolder1_lblREVISIONINFO')[0])
        issue_date = re.findall(r'中華民國.+?日', str_content)[0]
    elif link.find(urlGazette) >= 0: 
        fileUrls = [urlGazette + e.get('src') for e in soup.select('.embed-responsive-item')]
        fileNames = ['eg.pdf' for i in range(len(fileUrls))]
        content = ''
        serno = [e.text for e in soup.select('section.Block p span')][2]
        issue_date = soup.select('div.Item section.Block p span')[1].text
    elif link.find(urlmoj) >= 0:
        content = [e.text for e in soup.select('pre')][0]
        fileNames = [e.text for e in soup.select('#Content a')][3:]
        fileUrls = [urlmoj + '/' + e.get('href') for e in soup.select('#Content a')][3:]
        abstract = title
        serno = ''
        issue_date = [e.text for e in soup.select('#lbDate')][0]
    elif link.find(urlmojlaw) >= 0:
        pattern = re.compile(r'http(\:|\w|\.|\-|\/)+\/')
        m = pattern.match(link)
        content = [e.text.strip() for e in soup.select('.ClearCss')]
        content = '' if not bool(content) else content[0]
        fileUrls = [m.group(0) + e.get('href') for e in soup.select('.tab-edit a')]
        fileNames = [e.text for e in soup.select('.tab-edit a')]
        serno = [e.text.strip() for e in soup.select('#cp_content_trODWord td')]
        issue_date = [e.text.strip() for e in soup.select('#cp_content_trAnnDate td')]
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
        serno = [e.text.strip() for e in soup.select('#ctl00_cp_content_trODWord td')]
        issue_date = [e.text.strip() for e in soup.select('#ctl00_cp_content_trAnnDate td')]
    result['fileUrls'] = fileUrls
    result['fileNames'] = fileNames 
    result['content'] = content
    result['serno'] = serno
    result['issue_date'] = issue_date
    return result


# In[51]:


def parsingDetail(df, finalPath):  #   ex: finalPath = 'D:/國泰世華銀行_全國法規資料庫'
    df2 = pd.DataFrame(columns= ["標題", "全文內容", "附件", "發文字號", "發文日期", "相關法條", "要旨", "內容"])
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
            d = {'標題': title, '全文內容':result['content'], '附件':','.join(fileNames),'發文字號': result['serno'], 
                 '發文日期':result['issue_date']}
            df2= df2.append(pd.DataFrame(data=d, index=[0]))
        except:
            logging.error("爬取內文失敗")
            logging.error("失敗連結：" + link + "\n")
            traceback.print_exc()
    return df2


# In[52]:


def outputCsv(df, fileName, path):
    # 若目錄不存在，建立目錄
    if not os.path.isdir(path):
        os.mkdir(path)
    df.to_csv(path + "/" + fileName + ".csv", index = False, encoding = "utf_8_sig")


# In[53]:


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


# In[54]:


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
        df = pd.DataFrame(columns=['爬網日期','發文日期','標題','內文連結'])
        while True:
            index = 0 # css selector上的資料序號
            while True:
                try:
                    title = [e.text.strip() for e in soup.select('#Repeater1_hlabstract_' + str(index))]
                    if title == []:  # 當title抓不到東西的時候代表抓完資料了
                        break
                    link = [e.get('href') if e.get('href').find('http') >= 0 else ('https://law.moj.gov.tw/News/' + e.get('href')) for e in soup.select('#Repeater1_hlabstract_' + str(index))]
                    date = [e.text.strip() for e in soup.select('#Repeater1_lbDate_' + str(index))][0]
                    if compareTo(date, strDate) > 0:
                        break
                    lbtype = [e.text.strip() for e in soup.select('#Repeater1_lbType_' + str(index))]
                    tempDf = {'爬網日期': endDate, '發文日期': date,'標題': title, '內文連結': link}
                    df = df.append(pd.DataFrame(data = tempDf))
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


# In[4]:


def request2soup(url):
    res = requests.get(url)
    res.encoding = 'utf-8'
    soup = BeautifulSoup(res.text,'html.parser',from_encoding='utf-8')
    return soup


# In[56]:


def main(url, checkRange = 18):
    
    logging.critical("\n") 
    logging.critical("爬網開始......")
    logging.critical("目標網址：" + url)
    
    strTime = datetime.datetime.now()
    logging.critical("開始時間：" + strTime.strftime("%Y/%m/%d %H:%M:%S"))
    try:
        soup = request2soup(url)
    except:
        logging.error("網址錯誤" + url)
        return
    
    df_1 = parsingTitle(soup, checkRange)
    if len(df_1) == 0:
        return
    outputCsv(df_1, "第一層結果", FinalPath)
    
    df_2 = parsingDetail(df_1, FinalPath)
    outputCsv(df_2, "第二層結果", FinalPath)
    
    endTime = datetime.datetime.now()
    logging.critical("結束時間：" + endTime.strftime("%Y/%m/%d %H:%M:%S"))
    logging.critical("執行時間：" + str((endTime - strTime).seconds) + " 秒")
    logging.critical("輸出筆數：" + str(len(df_1)) + " 筆")
    logging.critical("爬網結束......")


# In[ ]:


if __name__ == "__main__":
    url = "https://law.moj.gov.tw/News/news_list.aspx?type=all"
    main(url) 

