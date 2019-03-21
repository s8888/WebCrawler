#!/usr/bin/env python
# coding: utf-8
'''
Project Name: fsc project
Crawl Cycle: Daily
Main Website: http://law.fsc.gov.tw/law/
author      : 林德昌
date        : 2019/02/25
description : 抓取每天發布的最新訊息
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
FinalPath = header.FINAL_PATH # project file
lastResultPath = header.LAST_RESULT_PATH


# In[2]:


def dataProcess_Detail(soup, row):
    urlGazette = 'https://gazette.nat.gov.tw' # linkto 行政院公報資訊網
    urlGazette1 = 'http://gazette.nat.gov.tw' # linkto 行政院公報資訊網
    urlmoj = 'https://law.moj.gov.tw/News/' # linkto 全國法規資料庫
    urlfsc = 'http://law.fsc.gov.tw/law/' # linkto 主管法規查詢系統
    title = row['TITL']
    link = row['LNK_URL']
    result = dict()
    if link.find(urlGazette) >= 0 or link.find(urlGazette1) >= 0: 
        fileUrls = [urlGazette + e.get('src') for e in soup.select('.embed-responsive-item')]
        fileNames = [title + '.pdf' for i in range(len(fileUrls))]
        content = ''
        serno = [e.text for e in soup.select('section.Block p span')][2]
        issue_date = soup.select('div.Item section.Block p span')[1].text
    elif link.find(urlmoj) >= 0:
        content = [header.spaceAndWrapProcess(e.text) for e in soup.select('.text-pre')][0]
        fileNames = [e.text for e in soup.select('#litFile a')]
        fileUrls = [urlmoj + '/' + e.get('href') for e in soup.select('#Content a')]
        serno = ''
        issue_date = [e.text for e in soup.select('td')][0]
    elif link.find(urlfsc) >= 0:
        if link.find('DraftOpinion.aspx') >= 0:
            content = '預告日期：' + [e.text.strip() for e in soup.select('td')][1]
            serno = [e.text.strip() for e in soup.select('td')][2]
            issue_date = [e.text.strip() for e in soup.select('td')][0][:9]
            
        elif link.find('NewsContent.aspx') >= 0:
            content1 = [header.spaceAndWrapProcess(e.text) for e in soup.select('.ClearCss')]
            content2 = [header.spaceAndWrapProcess(e.text) for e in soup.select('#ctl00_cp_content_trPreamble td')]
            content = content1[0] if content1 else content2[0]
            serno = [e.text.strip() for e in soup.select('#ctl00_cp_content_trODWord td')][0]
            issue_date = [e.text.strip() for e in soup.select('#ctl00_cp_content_trAnnDate td')][0]
        fileUrls = [urlfsc + e.get('href') for e in soup.select('#ctl00_cp_content_ulAnnFiles02 a')]
        fileNames = [e.text for e in soup.select('#ctl00_cp_content_ulAnnFiles02 a')]   
    
    result['fileUrls'] = fileUrls
    result['FILES'] = fileNames
    FILES_NM = [os.path.splitext(ele)[0][:30] + os.path.splitext(ele)[1] for ele in result['FILES']]
    result['FILES_NM'] = header.processDuplicateFiles(FILES_NM)
    result['content'] = content
    result['serno'] = serno
    result['issue_date'] = issue_date
    return result


# In[3]:


def parsingDetail(df, finalPath):  
    df2 = pd.DataFrame(columns= ["ISS_DATE", "TITL", "ISS_CTNT", "ISS_NO", "RLT_RGL", "FILES", 'FOLDER_NM', 'FILES_NM'])
    for index, row in df.iterrows():
        try:
            title = row['TITL']
            link = row['LNK_URL']
            logging.info(title)
            soup = request2soup(link)
            result = dataProcess_Detail(soup, row)
            if not bool(result):
                continue
                
            first_layer_date = row['ISS_DATE']
            FILES = result['FILES'] 
            FILES_NM = result['FILES_NM']
            FOLDER_NM = ''
            if len(FILES_NM) != 0:
                first_layer_date = re.sub(r'(/|-|\.)', '-', first_layer_date)
                FOLDER_NM = first_layer_date + '_' + title[:30].strip() + '_' + str(index) # 有附檔才會有資料夾名稱
                header.downloadFile(FOLDER_NM, finalPath, result['fileUrls'], FILES_NM)
            
            d = {'ISS_DATE':result['issue_date'], 'TITL': title, 'ISS_CTNT':result['content'],'ISS_NO': result['serno'], "RLT_RGL": '', 
                 'FILES':','.join(FILES), 'FOLDER_NM':FOLDER_NM, 'FILES_NM':','.join(FILES_NM)}
            df2= df2.append(d, ignore_index=True)
        except:
            header.EXIT_CODE = -1
            logging.error("爬取內文失敗")
            logging.error("失敗連結：" + link + "\n")
            traceback.print_exc()
    return df2


# In[4]:


def _add1911(matched):
    intStr = matched.group("number"); 
    intValue = int(intStr);
    addedValue = intValue + 1911; 
    addedValueStr = str(addedValue);
    return addedValueStr;


# In[5]:


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


# In[6]:


def dataProcess_Title(strDate):
    urlGazette = 'http://gazette.nat.gov.tw' # linkto 行政院公報資訊網
    urlmoj = 'https://law.moj.gov.tw/News/' # linkto 全國法規資料庫   
    urlfsc = 'http://law.fsc.gov.tw/law/' # linkto 主管法規查詢系統
    result = dict()
    titles_result = []
    dates = []
    links_result = []
    nowPage = 1
    end = False
    nowPage = 1
    while True:
        try:
            url = 'http://law.fsc.gov.tw/law/?page=' + str(nowPage) 
            soup = request2soup(url)
            titles = [e.text for e in soup.select('.tab-news a')]
            if titles == []:
                break
            links = [e.get('href') if e.get('href').find('http') >= 0 else (urlfsc + e.get('href')) for e in soup.select('.tab-news a')]
            for index in range(len(titles)):
                try:
                    link = links[index]
                    title = titles[index]
                    date = [e.text.strip() for e in soup.select('td')][4*index+1]
                    if compareTo(date, strDate) > 0:
                        end = True
                        break
                    titles_result.append(title)
                    dates.append(date)
                    links_result.append(link)
                except:
                    header.EXIT_CODE = -1
                    logging.error("爬取第 %s 頁第 %s 筆主旨發生錯誤" %(nowPage, index + 1))
            if end == True:
                break
            nowPage += 1
        except:
            header.EXIT_CODE = -1
            logging.error("爬取第 %s 頁主旨發生錯誤" %(nowPage))
            traceback.print_exc()
    result['titles_result'] = titles_result
    result['dates'] = dates
    result['links'] = links_result
    result['nowPage'] = nowPage
    result['index'] = index + 1
    return result


# In[7]:


def parsingTitle(url, checkRange):
    
    try:
        # 取得上次爬網結果
        if os.path.isfile(lastResultPath):
            lastResult = pd.read_csv(lastResultPath)
        else:
            lastResult = pd.DataFrame()
        header.lastResult = lastResult
        
        endDate = datetime.date.today()
        strDate = (endDate - datetime.timedelta(days = checkRange)).isoformat()
        df = pd.DataFrame(columns=['WEB_ADDR','CRL_DATE','ISS_DATE','TITL','LNK_URL'])
        
         # 資料處理
        result = dataProcess_Title(strDate)   
        
        d = {'WEB_ADDR': url, 'CRL_DATE':endDate,'ISS_DATE': result['dates'], 'TITL': result['titles_result'], 'LNK_URL': result['links']}
        df = df.append(pd.DataFrame(data=d))
        # 若與上次發文日期和標題相同，則跳至下一筆
        if not lastResult.empty:
            for index, row in df.iterrows():
                if (row['ISS_DATE'] in list(lastResult['ISS_DATE'])) and (row['TITL'] in list(lastResult['TITL'])):
                    df.drop(index, inplace = True)

        if len(df) == 0:
            logging.critical("%s 至 %s 間無資料更新" %(strDate, endDate))
        else:
            # 2019-02-01將原程式包入header的outputLastResult方法
            df.index = [i for i in range(df.shape[0])] # reset

    except:
        header.EXIT_CODE = -1
        logging.error("爬取主旨列表失敗")
        traceback.print_exc()
    
    return df


# In[8]:


def request2soup(url):
    res = requests.get(url)
    res.encoding = 'utf-8'
    soup = BeautifulSoup(res.text,'html.parser',from_encoding='utf-8')
    return soup


# In[9]:


def main(url, checkRange = 30):
    header.processBegin()
    header.clearFolder()
    try:
        df_1 = parsingTitle(url, checkRange)
        if len(df_1) == 0:
            return
        header.outputCsv(df_1, "第一層結果", FinalPath)
        df_2 = parsingDetail(df_1, FinalPath)
        header.outputCsv(df_2, "第二層結果", FinalPath)
        header.RESULT_COUNT = len(df_1)
        header.zipFile()
        header.createInfoFile()
        header.createOKFile()
        header.outputLastResult(df_1, header.lastResult, checkRange)   # 2019-02-01新增產出lastResult方法
    except:
        logging.error("執行爬網作業失敗")
        header.EXIT_CODE = -1
        traceback.print_exc()
    
    header.processEnd()


# In[10]:


if __name__ == "__main__":
    url = "http://law.fsc.gov.tw/law"
    main(url)

