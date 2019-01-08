#!/usr/bin/env python
# coding: utf-8
'''
Project Name: rootlaw project
Crawl Cycle: Daily
Main Website: http://www.rootlaw.com.tw/
author      : 林德昌
date        : 2018/12/17
description : 爬取每日最新法規及最新函令
'''
# In[40]:


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


# In[41]:


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


# In[42]:


def dataProcess_Detail_LawList(soup):
    result = dict()
    fileUrlRoot = 'http://www.rootlaw.com.tw/'
    result['fileUrls'] = [fileUrlRoot + e.get('href') for e in soup.select('#ctl00_ContentPlaceHolder1_gvLetter_ctl02_lblAttachment a')]
    result['fileNames'] = [e.text for e in soup.select('#ctl00_ContentPlaceHolder1_gvLetter_ctl02_lblAttachment a')]
    logging.info(result['fileNames'])
    result['issue_date'] = [e.text.strip() for e in soup.select('#ctl00_ContentPlaceHolder1_lblModifyDate')][0] # 發文日期
    result['serno'] = ''
    result['abstract'] = ''
    result['content'] = ''
    result['source'] = [e.text.strip() for e in soup.select('#ctl00_ContentPlaceHolder1_lblClass')][0]
    return result


# In[43]:


def dataProcess_Detail_LetterResult(soup):
    result = dict()
    fileUrlRoot = 'http://www.rootlaw.com.tw/'
    result['serno'] = [e.text for e in soup.select('#ctl00_ContentPlaceHolder1_gvLetter_ctl02_lblSorderTitle')][0]
    result['fileNames'] = [e.text for e in soup.select('#ctl00_ContentPlaceHolder1_gvLetter_ctl02_lblAttachment a')]
    result['fileUrls'] = [fileUrlRoot + e.get('href') for e in soup.select('#ctl00_ContentPlaceHolder1_gvLetter_ctl02_lblAttachment a')]
    logging.info(result['fileNames'])
    result['issue_date'] = [e.text.strip() for e in soup.select('#ctl00_ContentPlaceHolder1_gvLetter_ctl02_lblAnnounceDate')][0] # 發文日期
    result['abstract'] = [e.text.strip() for e in soup.select('#ctl00_ContentPlaceHolder1_gvLetter_ctl02_lblSorderSubject')][0] # 要旨
    result['content'] = [e.text.strip() for e in soup.select('#ctl00_ContentPlaceHolder1_gvLetter_ctl02_pnlContent pre')][0]
    result['source'] = [e.text.strip() for e in soup.select('#ctl00_ContentPlaceHolder1_gvLetter_ctl02_lblSorderDataSource')][0]
    return result


# In[44]:


# df = pd.DataFrame(columns = ["爬網日期","發文日期", "標題", "內文連結"])
def parsingDetail(df, finalPath): 
    df2 = pd.DataFrame(columns = ["標題", "發文日期", "附件", "發文字號","本文","出處", "要旨", "相關法條"])
    for index, row in df.iterrows():
        try:
            title = row['標題']
            title_type = row['標題種類']
            logging.info(title)
            link = row['內文連結']
            soup = request2soup(link)
            if title_type == '最新令函':
                result = dataProcess_Detail_LetterResult(soup)
            elif title_type == '最新法規':
                result = dataProcess_Detail_LawList(soup)
                
            fileNames = result['fileNames'] 
            if len(fileNames) != 0:
                downloadFile(finalPath, title, result['fileUrls'], fileNames)
            d = {'標題': title, '發文日期':result['issue_date'], '附件':','.join(fileNames), '發文字號':result['serno'],
                 "本文":result['content'], '出處':result['source'], "要旨": result['abstract'], '相關法條':''}
            df2= df2.append(d, ignore_index = True)
        except:
            logging.error("爬取內文失敗")
            logging.error("失敗連結：" + link)
            traceback.print_exc()
    return df2


# In[45]:


def outputCsv(df, fileName, path):
    # 若目錄不存在，建立目錄
    if not os.path.isdir(path):
        os.mkdir(path)
    df.to_csv(path + "/" + fileName + ".csv", index = False, encoding = "utf_8_sig")


# In[46]:


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


# In[47]:


def dataProcess_Title_LawList(strDate):
    result = dict()
    nowPage = 1
    preurl = 'http://www.rootlaw.com.tw/'
    titles_result = []
    dates = []
    links = []
    end = False
    while True:
        try:
            url = 'http://www.rootlaw.com.tw/LawList.aspx?Page=' + str(nowPage) + '&LCode='
            soup = request2soup(url)
            titles = [re.sub(r'[\r|\n]+?', '', e.text.strip()) for e in soup.select('#ctl00_ContentPlaceHolder1_gvLaws a')]
            if titles == []:
                break
            for index in range(len(titles)):
                try:
                    title = titles[index]
                    date = soup.select('.gvDate')[index].text.strip()
                    if compareTo(date, strDate) > 0: # 若發文日期小於開始日期, 則結束爬取主旨
                        end = True
                        break
                    link = preurl + soup.select('#ctl00_ContentPlaceHolder1_gvLaws a')[index].get('href')
                    titles_result.append(title)
                    dates.append(date)
                    links.append(link)
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
    result['title_Type'] = '最新法規'
    return result


# In[48]:


def dataProcess_Title_LetterResult(strDate):
    result = dict()
    titles_result = []
    dates = []
    links = []
    nowPage = 1
    preurl = 'http://www.rootlaw.com.tw/'
    end = False
    while True:
        try:
            url = 'http://www.rootlaw.com.tw/LetterList.aspx?Page=' + str(nowPage) + '&LCode='
            soup = request2soup(url)
            titles = [e.text.strip() for e in soup.select('#gvLetter a')]
            if titles == []:
                break
            for index in range(len(titles)):
                try:
                    title = titles[index]
                    date = soup.select('.gvDate')[index].text.strip()
                    if compareTo(date, strDate) > 0:
                        end = True
                        break
                    link = preurl + soup.select('#gvLetter a')[index].get('href')
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
    result['title_Type'] = '最新令函'
    return result


# In[49]:


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
        df = pd.DataFrame(columns = ["爬網日期","發文日期", "標題", "內文連結","標題種類"])
        
        # 資料處理
        result_LetterResult = dataProcess_Title_LetterResult(strDate)
        result_LawList = dataProcess_Title_LawList(strDate)
        
        
        d_LawList = {'爬網日期':endDate, '發文日期': result_LawList['dates'], '標題': result_LawList['titles_result'], 
                     '內文連結': result_LawList['links'], '標題種類':result_LawList['title_Type']}
        d_LetterResult = {'爬網日期':endDate, '發文日期': result_LetterResult['dates'], '標題': result_LetterResult['titles_result'], 
                          '內文連結': result_LetterResult['links'], '標題種類':result_LetterResult['title_Type']}
        
        df = df.append(pd.DataFrame(data = d_LawList)).append(pd.DataFrame(data = d_LetterResult))    
        df.index = [i for i in range(df.shape[0])] # reset
        # 若與上次發文日期和標題相同，則跳至下一筆
        if not lastResult.empty:
            for index, row in df.iterrows():
                if (row['發文日期'] in list(lastResult['發文日期'])) and (row['標題'] in list(lastResult['標題'])):
                    df.drop(index, inplace = True)
                    
        if len(df) == 0:
            logging.critical("%s 至 %s 間無資料更新" %(strDate, endDate))
        else:
            outputCsv(df, "lastResult", "./CrawlList")
        
    except:
        logging.error("爬取主旨列表失敗")
        traceback.print_exc()
    return df
    


# In[50]:


def request2soup(url):
    res = requests.get(url)
    res.encoding = 'utf-8'
    soup = BeautifulSoup(res.text,'html.parser',from_encoding='utf-8')
    return soup


# In[51]:


def main(url, checkRange = 5):
    
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


# In[52]:


if __name__ == "__main__":
    url = "http://www.rootlaw.com.tw/"
    main(url)

