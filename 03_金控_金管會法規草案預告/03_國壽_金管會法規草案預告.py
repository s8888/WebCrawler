#!/usr/bin/env python
# coding: utf-8
'''
Project Name: 
CTM Name    : 金管會主管法規查詢系統
Crawl Cycle : Daily
Main Website: https://www.fsc.gov.tw/ch/home.jsp?id=128&parentpath=0,3 
Description : 針對金管會-法規資訊-法規草案預告，每日取得法規草案預告
Author      : 林竣昇
Update Date : 2019.02.02
'''
# In[2]:


import header
import logging

import datetime
import pandas as pd
import traceback
from bs4 import BeautifulSoup

import os
import requests
import re


# In[3]:


def getResult(findall, STR_NUM):
    if findall != []:
        result = findall[0][STR_NUM:-1]
    else:
        result = ""
    return result


# In[4]:


def dataProcess_Detail(soup):
    result = dict()
    # 主旨
    result['title'] = [ e.text for e in soup.select("#maincontent h3") ][0]
    # 內容
    result['content'] = [header.spaceAndWrapProcess(e.text) for e in soup.select('.page_content')][0]
    # 附件
    result['FILES'] = [e.get("title").replace("(開啟新視窗)", "") for e in soup.select('.acces a')]
    FILES_NM = [os.path.splitext(ele)[0][:30] + os.path.splitext(ele)[1] for ele in result['FILES']]
    result['FILES_NM'] = header.processDuplicateFiles(FILES_NM)
    
    logging.info(result['FILES_NM'])
    result['fileUrls'] = [ "https://www.ib.gov.tw" + e.get('href') for e in soup.select('.acces a') ]
    str_content = str(soup.select('.page_content')[0])
    # 發文字號
    result['serno'] = getResult(re.findall(r'發文字號.+?<', str_content), 5)
    # 發文日期
    result['issue_date'] = getResult(re.findall(r'發文日期.+?<', str_content), 5)
    # 相關法條
    result['RLT_RGL'] = getResult(re.findall(r'依據.+?<', str_content), 3)

    return result


# In[5]:


def parsingDetail(df):

    df_detail = pd.DataFrame(columns = ["ISS_DATE", "TITL", "ISS_CTNT", "ISS_NO", "RLT_RGL", "FILES",
                                        "FOLDER_NM", "FILES_NM"])#[2019.01.24]新增欄位(截斷後的資料夾名稱及檔名
    for index, row in df.iterrows():
        try:
            first_layer_date = row['ISS_DATE']
            link = row['LNK_URL']
            soup = request2soup(link)
            
            result = dataProcess_Detail(soup)
            title = result['title']#[2019.02.11]不能取第一層title,會有...
            FILES = result['FILES'] 
            FILES_NM = result['FILES_NM']
            FOLDER_NM = ''
            if len(FILES_NM) != 0:
                first_layer_date = re.sub(r'(/|-|\.)', '-', first_layer_date)
                FOLDER_NM = first_layer_date + '_' + title[:30].strip() + '_' + str(index) # 有附檔才會有資料夾名稱
                header.downloadFile(FOLDER_NM, header.FINAL_PATH, result['fileUrls'], FILES_NM)#[2019.02.11]抽共用
            
            d = {'ISS_DATE':result['issue_date'], 'TITL': title, 'ISS_CTNT': result['content'], 
                 'ISS_NO':result['serno'], 'RLT_RGL':result['RLT_RGL'], 'FILES':','.join(FILES), 
                 'FOLDER_NM': FOLDER_NM, 'FILES_NM':','.join(FILES_NM)}
            
            df_detail= df_detail.append(d, ignore_index=True)
    
        except:
            header.EXIT_CODE = -1   #[2019.02.01] 爬取內文發生錯誤則重爬
            logging.error("爬取內文失敗")
            logging.error("失敗連結：" + link)
            traceback.print_exc()
    
    return df_detail
    


# In[6]:


def parsingTitle(soup, checkRange):
    try:
        # 取得上次爬網結果
        lastResultPath = header.LAST_RESULT_PATH # +"/lastResult.csv"#[2019.02.11]
        if os.path.isfile(lastResultPath):
            lastResult = pd.read_csv(lastResultPath)
        else:
            lastResult = pd.DataFrame()
        header.lastResult = lastResult #[2019.02.11]新增全域變數
        
        # 爬網日期區間為一個禮拜
        endDate = datetime.date.today()
        strDate = (endDate - datetime.timedelta(days = checkRange)).isoformat()
        
        totalPage = soup.select(".page")[0].text.split("/")[1] # 總頁數
        ending = False
        
        df = pd.DataFrame(columns = ["WEB_ADDR", "CRL_DATE", "ISS_DATE", "TITL", "LNK_URL"])

        for i in range(int(totalPage)):
            if (i != 0):
                soup = request2soup(url, i + 1)

            try:
                sorts = soup.select(".sort1")
                sorts = [x.text.strip() for x in sorts]
                
                dates = soup.select(".pdate1")
                dates = [x.text.strip() for x in dates]

                titles = soup.select(".ptitle1")
                titles = [x.text.strip() for x in titles] 

                links = soup.select(".ptitle1 a")
                links = ["https://www.fsc.gov.tw/ch/" + x.get("href") for x in links]

                idx = pd.Series([False] * len(dates))
                for j in range(len(dates)):
                    date = dates[j]
                    if date < strDate: # 若發文日期小於開始日期, 則結束爬取主旨
                        ending = True
                        break
                    idx[j] = True
                
                d = {"WEB_ADDR" : url, "CRL_DATE" : endDate, "ISS_DATE" : dates, "TITL" : titles, "LNK_URL" : links}
                df = df.append(pd.DataFrame(data = d)[idx])  # append page

                # 若結束爬取主旨, 停止爬取剩下的 pages
                if ending:
                    break
            except:
                header.EXIT_CODE = -1   #[2019.02.01] 爬取任一頁主旨發生錯誤則重爬
                logging.error("爬取第 %s 頁主旨發生錯誤" %str(i + 1))
                traceback.print_exc()

        df.index = [i for i in range(df.shape[0])] # reset Index 
        #header.outputCsv(df, "lastResult", header.LAST_RESULT_PATH)#[2019.02.11]刪除,以抽於outputLastResult共用

        if not lastResult.empty:
            # 若與上次發文日期和標題相同，則跳至下一筆
            for index, row in df.iterrows():#[2019.02.11]
                if (row['ISS_DATE'] in list(lastResult['ISS_DATE'])) and (row['TITL'] in list(lastResult['TITL'])):
                    df.drop(index, inplace = True)
            
        if len(df) == 0:
            logging.critical("%s 至 %s 間無資料更新" %(strDate, endDate))
        else:
            df.index = [i for i in range(df.shape[0])] # reset 

        return df
    
    except:
        print("爬取主旨列表失敗")
        logging.error("爬取主旨列表失敗")
        traceback.print_exc()
        return pd.DataFrame(columns = ["WEB_ADDR", "CRL_DATE", "ISS_DATE", "TITL", "LNK_URL"])
  


# In[7]:


def request2soup(url, page = None):
    if page is None:
        res = requests.get(url)
    else:
        formData = {"id"         : "133", 
                    "contentid"  : "133", 
                    "parentpath" : "0,3", 
                    "mcustomize" : "lawnotice_list.jsp", 
                    "keyword"    : "請輸入查詢關鍵字", 
                    "page"       : page}
        res = requests.post(url, data = formData)
        
    res.encoding = "utf-8"
    soup = BeautifulSoup(res.text, "html.parser", from_encoding = "utf-8")
    return soup


# In[8]:


def main(url, checkRange = 30):
    header.processBegin(url = url)
    header.clearFolder()#[2019.02.11]

    try:
        soup = request2soup(url, 1)
        
        df_1 = parsingTitle(soup, checkRange)
        if len(df_1) != 0:
            header.outputCsv(df_1, "第一層結果")
        
            df_2 = parsingDetail(df_1)
            header.outputCsv(df_2, "第二層結果")
            header.RESULT_COUNT = len(df_2)
        header.zipFile()
        header.createInfoFile()
        header.createOKFile()
        header.outputLastResult(df_1, header.lastResult, checkRange)   #[2019.02.11]新增產出lastResult方法
    except:
        logging.error("執行爬網作業失敗")
        traceback.print_exc()
        header.createInfoFile()
    
    header.processEnd()


# In[9]:


print(header.TIMELABEL)
logging.fatal("FINAL_PATH:"+ header.FINAL_PATH)
url = "https://www.fsc.gov.tw/ch/home.jsp?id=133&parentpath=0,3"


# In[10]:


main(url)


# In[ ]:





# In[ ]:




