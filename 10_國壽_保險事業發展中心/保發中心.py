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
# In[31]:


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


# In[32]:


def dataProcess_Detail(soup, title_type):
    result = dict()
    fileUrlRoot = 'http://law.tii.org.tw/Fn/'
    str_content = [e.text for e in soup.select('pre')][0]
    if title_type == '行政函釋':
        contentlist = re.findall('主 旨：.+', header.spaceAndWrapProcess(str_content))
        if bool(contentlist):
            content = contentlist[0]
        else:
            content = re.findall('一、.+', header.spaceAndWrapProcess(str_content))[0] 
        serno = re.findall(r'發文字號.+?\d+.+', str_content)[0][5:-1]
        issue_date = re.findall(r'發文日期.+?\d+.+', str_content)[0][5:-1]
    else:
        content = header.spaceAndWrapProcess(str_content) 
        content1 = re.sub('\s','', str_content)
        serno = re.findall('日\S+?號[令|函|(公告)]', content1)[0][1:]
        issue_date = re.findall('民國.+?日', str_content)[0]
        
    result['FILES'] = [e.text for e in soup.select('font font a')]
    FILES_NM = [os.path.splitext(ele)[0][:30] + os.path.splitext(ele)[1] for ele in result['FILES']]
    result['FILES_NM'] = header.processDuplicateFiles(FILES_NM)
    logging.info(result['FILES_NM'])
    result['fileUrls'] = [re.sub(r'\.\/', fileUrlRoot, e.get('href')) for e in soup.select('font font a')]
    result['content'] = content
    result['serno'] = serno
    result['issue_date'] = issue_date
    return result


# In[33]:


def parsingDetail(df, finalPath): 
    df2 = pd.DataFrame(columns = ["ISS_DATE", "TITL", "ISS_CTNT", 'ISS_NO', "RLT_RGL", "FILES", 'FOLDER_NM', 'FILES_NM', "TITLE_TYPE"])
    for index, row in df.iterrows():
        try:
            title = row['TITL']
            logging.info(title)
            link = row['LNK_URL']
            title_type = row['TITLE_TYPE']
            soup = request2soup(link)
            result = dataProcess_Detail(soup, title_type)

            first_layer_date = row['ISS_DATE']
            FILES = result['FILES'] 
            FILES_NM = result['FILES_NM']
            FOLDER_NM = ''
            if len(FILES_NM) != 0:
                first_layer_date = re.sub(r'(/|-|\.)', '-', first_layer_date)
                FOLDER_NM = first_layer_date + '_' + title[:30].strip() + '_' + str(index) # 有附檔才會有資料夾名稱
                header.downloadFile(FOLDER_NM, finalPath, result['fileUrls'], FILES_NM)
            d = {'ISS_DATE':result['issue_date'],'TITL': title, 'ISS_CTNT': result['content'], 'ISS_NO':result['serno'], 'RLT_RGL':'',
                'FILES':','.join(FILES), 'FOLDER_NM':FOLDER_NM, "FILES_NM": ','.join(FILES_NM), 'TITLE_TYPE':title_type}
            df2= df2.append(d, ignore_index=True)
        except:
            header.EXIT_CODE = -1
            logging.error("爬取內文失敗")
            logging.error("失敗連結：" + link)
            traceback.print_exc()
    return df2


# In[34]:


def _add1911(matched):
    intStr = matched.group("number"); 
    intValue = int(intStr);
    addedValue = intValue + 1911; 
    addedValueStr = str(addedValue);
    return addedValueStr;


# In[35]:


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


# In[36]:


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
            print(titles)
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
                    header.EXIT_CODE = -1
                    logging.error("爬取第 %s 頁第 %s 筆資料發生錯誤" %(nowPage, index + 1))
                    traceback.print_exc()
            if end == True:
                break
            nowPage += 1
        except:
            header.EXIT_CODE = -1
            logging.error("爬取第 %s 頁主旨發生錯誤" %(nowPage))
            traceback.print_exc()
        
    result['titles_result'] = titles_result
    result['dates'] = dates
    result['links'] = links
    result['nowPage'] = nowPage
    result['crawl_date'] = datetime.date.today()
    result['title_type'] = title_types 
    return result

        


# In[37]:


def parsingTitle(url, checkRange):
    try:
        # 取得上次爬網結果
        if os.path.isfile(lastResultPath):
            lastResult = pd.read_csv(lastResultPath)
        else:
            lastResult = pd.DataFrame()
        header.lastResult = lastResult
        # 爬網日期區間為一個禮拜
        endDate = datetime.date.today()
        strDate = (endDate - datetime.timedelta(days = checkRange)).isoformat()
        df = pd.DataFrame(columns = ['WEB_ADDR',"CRL_DATE","ISS_DATE", "TITL", "LNK_URL", 'TITLE_TYPE'])
        
        # 資料處理
        result = dataProcess_Title(strDate)
        
        d = {'WEB_ADDR':url, 'CRL_DATE':endDate, 'ISS_DATE': result['dates'], 'TITL': result['titles_result'],'LNK_URL': result['links'], 'TITLE_TYPE': result['title_type']}
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
    


# In[38]:


def request2soup(url):
    res = requests.get(url)
    res.encoding = 'big5'
    soup = BeautifulSoup(res.text,'html.parser',from_encoding='utf-8')
    return soup


# In[39]:


def main(url, checkRange = 14):
    
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
        header.EXIT_CODE = -1
        logging.error("執行爬網作業失敗")
        traceback.print_exc()
        
    header.processEnd()


# In[40]:


if __name__ == "__main__":
    url = "http://law.tii.org.tw/Fn/Onews.asp"
    main(url)

