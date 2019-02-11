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
lastResult = pd.DataFrame()


# In[2]:


def dataProcess_Detail(soup, fileUrlRoot):
    result = dict()
    result['content'] = [header.spaceAndWrapProcess(e.text) for e in soup.select('.page_content')][0]
    result['FILES'] = [e.text for e in soup.select('.acces a')]
    FILES_NM = [os.path.splitext(ele)[0][:30] + os.path.splitext(ele)[1] for ele in result['FILES']]
    result['FILES_NM'] = header.processDuplicateFiles(FILES_NM)
    
    logging.info(result['FILES_NM'])
    result['fileUrls'] = [fileUrlRoot + e.get('href') for e in soup.select('.acces a')]
    str_content = str(soup.select('.page_content')[0])
    result['serno'] = re.findall(r'發文字號.+?\d+.+?', str_content)[0][5:]
    result['issue_date'] = re.findall(r'發文日期.+?日', str_content)[0][5:]
    return result


# In[3]:


def parsingDetail(df, finalPath): 
    df2 = pd.DataFrame(columns = ['ISS_DATE', "TITL", "ISS_CTNT", "ISS_NO", "RLT_RGL", "FILES", 'FOLDER_NM', 'FILES_NM'])
    fileUrlRoot = 'https://www.banking.gov.tw'
    for index, row in df.iterrows():
        try:
            title = row['TITL']
            logging.info(title)
            link = row['LNK_URL']
            first_layer_date = row['ISS_DATE']
            if link.find('uploaddowndoc') >= 0:
                FOLDER_NM = first_layer_date + '_' + title[:30]
                FILES_NM = re.findall(r'filedisplay=\w+\.pdf', link)[0][len('filedisplay='):]
                header.downloadFile(FOLDER_NM, finalPath, [link], [FILES_NM])
                continue

            soup = request2soup(link)
            result = dataProcess_Detail(soup, fileUrlRoot)

            FILES = result['FILES'] 
            FILES_NM = result['FILES_NM']
            FOLDER_NM = ''
            if len(FILES_NM) != 0:
                first_layer_date = re.sub(r'(/|-|\.)', '-', first_layer_date)
                FOLDER_NM = first_layer_date + '_' + title[:30].strip() + '_' + str(index) # 有附檔才會有資料夾名稱
                header.downloadFile(FOLDER_NM, finalPath, result['fileUrls'], FILES_NM)
            d = {'ISS_DATE':result['issue_date'], 'TITL': title, 'ISS_CTNT': result['content'], 'ISS_NO':result['serno'], 'RLT_RGL':'', 
                 'FILES':','.join(FILES), 'FOLDER_NM': FOLDER_NM, 'FILES_NM':','.join(FILES_NM)}
            df2= df2.append(d, ignore_index=True)
        except:
            header.EXIT_CODE = -1   # 2019/02/01 爬取內文發生錯誤則重爬
            logging.error("爬取內文失敗")  
            logging.error("失敗連結：" + link)
            traceback.print_exc()
    return df2


# In[4]:


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


# In[5]:


def dataProcess_Title(soup, strDate):
    result = dict()
    nowPage = 1
    preurl1 = 'https://www.banking.gov.tw/ch/'
    preurl2 = 'https://www.banking.gov.tw'
    titles_result = []
    dates = []
    links = []
    end = False
    while True:
        try:
            if nowPage > 1:
                soup = request2soup(url, nowPage)
            titles = [e.text.strip() for e in soup.select('.ptitle1 a')]
            if not bool(titles):
                break
            for index in range(len(titles)):
                try:
                    title = titles[index]
                    date = soup.select('.pdate1')[index].text.strip()
                    if compareTo(date, strDate) > 0: # 若發文日期小於開始日期, 則結束爬取主旨
                        end = True
                        break
                    href = soup.select('.ptitle1 a')[index].get('href')
                    link = (preurl2 + href) if href.find('uploaddowndoc') >= 0 else (preurl1 + href)
                    titles_result.append(title)
                    dates.append(date)
                    links.append(link)
                except:
                    header.EXIT_CODE = -1   # 2019/02/01 爬取任一筆主旨發生錯誤則重爬
                    logging.error("爬取第 %s 頁第 %s 筆資料發生錯誤" %(nowPage, index + 1))
                    traceback.print_exc()
            if end == True:
                break
            nowPage += 1
        except:
            header.EXIT_CODE = -1   # 2019/02/01 爬取任一頁主旨發生錯誤則重爬
            logging.error("爬取第 %s 頁主旨發生錯誤" %(nowPage))
            traceback.print_exc()
        
    result['titles_result'] = titles_result
    result['dates'] = dates
    result['links'] = links
    result['nowPage'] = nowPage
    result['index'] = index + 1
    result['crawl_date'] = datetime.date.today()
    return result

        


# In[6]:


def parsingTitle(url, soup, checkRange):
    try:
        # 取得上次爬網結果
        if os.path.isfile(lastResultPath):
            lastResult = pd.read_csv(lastResultPath)
        
        # 爬網日期區間為一個禮拜
        endDate = datetime.date.today()
        strDate = (endDate - datetime.timedelta(days = checkRange)).isoformat()
        df = pd.DataFrame(columns = ['WEB_ADDR',"CRL_DATE","ISS_DATE", "TITL", "LNK_URL"])
        
        # 資料處理
        result = dataProcess_Title(soup, strDate)
        
        d = {'WEB_ADDR':url, 'CRL_DATE':result['crawl_date'], 'ISS_DATE': result['dates'], 'TITL': result['titles_result'], 'LNK_URL': result['links']}
        df = df.append(pd.DataFrame(data = d))    
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
    


# In[7]:


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


# In[8]:


def main(url, checkRange = 5):
    header.processBegin()
    header.clearFolder()
    try:
        soup = request2soup(url, 1)
        df_1 = parsingTitle(url, soup, checkRange)
        if len(df_1) == 0:
            return
        header.outputCsv(df_1, "第一層結果", FinalPath)

        df_2 = parsingDetail(df_1, FinalPath)
        header.outputCsv(df_2, "第二層結果", FinalPath)
        header.RESULT_COUNT = len(df_1)
        header.zipFile()
        header.createInfoFile()
        header.createOKFile()
        header.outputLastResult(df_1, lastResult, checkRange)   # 2019-02-01新增產出lastResult方法
    except:
        logging.error("執行爬網作業失敗")
        header.EXIT_CODE = -1
        traceback.print_exc()
        
    header.processEnd()


# In[9]:


if __name__ == "__main__":
    url = "https://www.banking.gov.tw/ch/home.jsp?id=190&parentpath=0,3"
    main(url)

