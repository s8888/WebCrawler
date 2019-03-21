#!/usr/bin/env python
# coding: utf-8
'''
Project Name: 法源法律網
CTM Name    : 
Crawl Cycle : Daily
Main Website: http://www.lawbank.com.tw/news/NewsSearch.aspx?TY=19 (法規新訊)
              http://www.lawbank.com.tw/news/NewsSearch.aspx?TY=20 (判解新訊)
              http://www.lawbank.com.tw/news/NewsSearch.aspx?TY=21 (函釋新訊)
              http://www.lawbank.com.tw/news/NewsSearch.aspx?TY=22 (法規草案)
Description : 針對法源法律網-法律新訊，每日取得法規異動資訊
Author      : 林竣昇
Update Date : 2018/12/21
'''
# In[1]:


import header
import logging
from bs4 import BeautifulSoup
from selenium import webdriver 
from selenium.webdriver.support.ui import WebDriverWait
import SeleniumUtil
import re
import datetime
import pandas as pd
import traceback

import os
import requests
FinalPath = header.FINAL_PATH # project file
lastResultPath = header.LAST_RESULT_PATH


# In[2]:


def getDetailFromContent(soup, tabNumber, subColName, title, content):
    
    if tabNumber == 19:
        serialNumber = re.sub('\s','',re.findall(r'日.+?字[\w|\s]+號', content)[0][1:]) if bool(re.findall(r'日.+?字[\w|\s]+號', content)) else ''
        relatedLaw = '|'.join([e.text for e in soup.select("#ctl00_cphMain_relaData a")])
        print(relatedLaw)
    else:
        serialNumber = re.findall(r'字號.+?\d+.+?', content)[0][2:] if bool(re.findall(r'字號.+?\d+.+?', content)) else ''
        relatedLaw = '|'.join([e.text for e in soup.select('.pageNews-Content a')])
    
    
    date = re.findall('民國.+?日', content)[0] if bool(re.findall('民國.+?日', content)) else ''
    tempMap = {"ISS_DATE":date,
              "TITL" : title, 
               "ISS_CTNT" : content,
                "ISS_NO":serialNumber,
                "RLT_RGL":relatedLaw,
              "FILES":"", 
               'FOLDER_NM':'', 
               'FILES_NM':''}      
    
    return tempMap


# In[3]:


def parsingDetail(df, tabNumber, FinalPath):
    
    if tabNumber == 20:
        subColName = "裁判"
    elif tabNumber == 22:
        subColName = "公告"
    else:
        subColName = "發文"
    
        
    
#     df_detail = pd.DataFrame(columns = ["標題", "全文內容", subColName + "字號", subColName + "日期", "相關法條", "附件"])
    df2 = pd.DataFrame(columns= ["ISS_DATE", "TITL", "ISS_CTNT", "ISS_NO", "RLT_RGL", "FILES", 'FOLDER_NM', 'FILES_NM'])
    for link in df["LNK_URL"]:
        try:
            print(tabNumber)
            print(link)
            soup = request2soup(link)
            # 主旨
            title = soup.select("#ctl00_cphMain_lblSubject")[0].text.strip()

            # 全文內容
            content = soup.select("#pageNews")[0].text.strip()
#             content = content.split("第 一 章")[0].strip() # for 2018-11-26 電信管理法之例外處理
            
            tempMap = getDetailFromContent(soup, tabNumber, subColName, title, content)
            
            df2 = df2.append(tempMap, ignore_index = True)

        except:
            header.EXIT_CODE = -1
            logging.error("爬取內文失敗")
            logging.error("失敗連結：" + link + "\n")
            traceback.print_exc()

    return df2


# In[4]:


def parsingTitle(url, driver, checkRange):
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
        
        ending = False
        nowPage = 1
        df = pd.DataFrame(columns=['WEB_ADDR','CRL_DATE','ISS_DATE','TITL','LNK_URL'])
#         df = pd.DataFrame(columns = ["爬文日期", "發文日期", "標題", "網頁連結"])

        # actions
        while True:
            try:
                dates = driver.find_elements_by_css_selector(".tdDate")
                dates = [x.text for x in dates] 

                titles = driver.find_elements_by_css_selector(".tdSubject")
                titles = [x.text for x in titles] 

                links = driver.find_elements_by_css_selector(".tdSubject a")
                links = [x.get_attribute("href") for x in links]

                show = pd.Series([False] * len(dates))
                for idx in range(len(dates)):
                    date = dates[idx]
                    if date < strDate: # 若發文日期小於開始日期, 則結束爬取主旨
                        ending = True
                        break
                    show[idx] = True
                    
                nowDates = [str(endDate.year) + "/" + str(endDate.month) + "/" + str(endDate.day)] * len(dates)
                d = {"WEB_ADDR":url, "CRL_DATE":nowDates, "ISS_DATE":dates, "TITL":titles, "LNK_URL" : links}
                df = df.append(pd.DataFrame(data = d)[show])  # append page

                # 若結束爬取主旨, 停止爬取剩下的 page
                if ending:
                    break
                    
                # 下一頁
                goNext = driver.find_elements_by_css_selector("#ctl00_cphMain_PagerTop_butNext")[0]
                if goNext.get_attribute("href") == None: # 最後一頁不執行點擊下一頁
                    break
                goNext.click() # 下一頁
                nowPage += 1
            except:
                logging.error("爬取第 %s 頁主旨發生錯誤" %str(nowPage + 1))
                traceback.print_exc()
                

        # 若與上次發文日期和標題相同，則跳至下一筆
        if not lastResult.empty:
            for index, row in df.iterrows():
                if (row['ISS_DATE'] in list(lastResult['ISS_DATE'])) and (row['TITL'] in list(lastResult['TITL'])):
                    df.drop(index, inplace = True)

        if len(df) == 0:
            logging.critical("%s 至 %s 間無資料更新" %(strDate, endDate))
        else:
            df.index = [i for i in range(df.shape[0])] # reset 

    except:
        header.EXIT_CODE = -1
        logging.error("爬取主旨列表失敗")
        traceback.print_exc()
    
    return df
  


# In[5]:


def request2soup(url):
    res = requests.get(url)
    res.encoding = "utf-8"
    soup = BeautifulSoup(res.text, "html.parser", from_encoding = "utf-8")
    return soup


# In[6]:


def main(url, tabNumber, checkRange = 15):
    
    header.processBegin()
    header.clearFolder()
    DownloadTool = SeleniumUtil.ChromeDownload()
    DownloadTool.setDownLoadTempPath(header.TEMP_PATH)
    DownloadTool.setDownLoadFinalPath(FinalPath)
    chrome_options = DownloadTool.getChromeOptions()
    driver = webdriver.Chrome(chrome_options = chrome_options) # open chrome browser with Options
    try:
        if tabNumber >= 19 and tabNumber <= 22 and isinstance(tabNumber, int):
            url = url + str(tabNumber)
        else:
            raise ValueError("tabNumber 必須為 19 到 22 的整數")
        
        driver.get(url)
        df_1 = parsingTitle(url, driver, checkRange)
        if len(df_1) != 0:
            header.outputCsv(df_1, "第一層結果", FinalPath)
        
            df_2 = parsingDetail(df_1, tabNumber, FinalPath)
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


# In[7]:


if __name__ == "__main__":
    url = "http://www.lawbank.com.tw/news/NewsSearch.aspx?TY="
    main(url, 19, 3)

