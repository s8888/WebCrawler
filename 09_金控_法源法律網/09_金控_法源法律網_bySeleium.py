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
# In[3]:


import header
import logging

from selenium import webdriver 
from selenium.webdriver.support.ui import WebDriverWait
import SeleniumUtil

import datetime
import pandas as pd
import traceback
from bs4 import BeautifulSoup

import os
import requests


# In[4]:


TempPath = "./Temp/"  # browser file
FinalPath = "./Result/" # project file
lastResultPath = "./CrawlList/"
lastResultName = "lastResult"


# In[5]:


def getDetailFromContent(soup, tempMap, tabNumber, subColName):
    
    temp = soup.select("pre:nth-of-type(1)")[0].text.strip()
    if tabNumber == 19:
        # 發文字號
        serialNumber = temp.replace("\n", "").strip()
        strPos = serialNumber.find("日")
        endPos = serialNumber.rfind("號令")
        if strPos != -1 & endPos != -1:
            strPos += 1
            endPos += 2
            serialNumber = serialNumber[strPos:endPos]
        else:
            serialNumber = ""

        # 發文日期
        date = soup.select("#ctl00_cphMain_lblndate")[0].text.strip()

        # 相關法條
        relatedLaws = soup.select("#ctl00_cphMain_relaData a")
        relatedLaw = ", ".join(e.text.strip() for e in relatedLaws)
            
    else:
        
        # 裁判字號
        strPos = temp.find(subColName + "字號：")
        if strPos != -1:
            strPos += 5
            endPos = strPos + temp[strPos:].find("：") - 4
            serialNumber = temp[strPos:endPos].strip()
        else:
            serialNumber = ""


        # 裁判日期
        strPos = temp.find(subColName + "日期：")
        if strPos != -1:
            strPos += 5
            endPos = strPos + temp[strPos:].find("：") - 4
            date = temp[strPos:endPos]
        else:
            date = ""

        # 相關法條
        strPos = temp.find("相關法條：")
        if strPos != -1:
            strPos += 5
            endPos = strPos + temp[strPos:].find("：") - 4
            relatedLaws = temp[strPos:endPos].split("\n")
            relatedLaw = ", ".join(e.strip() for e in relatedLaws)[:-2]
        else:
            relatedLaws = soup.select("#ctl00_cphMain_relaData a")
            relatedLaw = ", ".join(e.text.strip() for e in relatedLaws)
            
                
    
        
    tempMap["發文字號"] = serialNumber
    tempMap["發文日期"] = date
    tempMap["相關法條"] = relatedLaw
    
    return tempMap


# In[6]:


def request2soup(url):
    res = requests.get(url)
    res.encoding = "utf-8"
    soup = BeautifulSoup(res.text, "html.parser", from_encoding = "utf-8")
    return soup


# In[7]:


def parsingDetail(df, FinalPath, tabNumber):
    
    if tabNumber == 20:
        subColName = "裁判"
    elif tabNumber == 22:
        subColName = "提案"
    else:
        subColName = "發文"
    
    df_detail = pd.DataFrame(columns = ["標題", "全文內容", subColName + "字號", subColName + "日期", "相關法條", "附件"])

    for link in df["網頁連結"]:
        try:
            print("擷取網址：" + link)
            soup = request2soup(link)

            # 主旨
            title = soup.select("#ctl00_cphMain_lblSubject")[0].text.strip()

            # 全文內容
            content = soup.select("#pageNews")[0].text.strip()
            content = content.split("第 一 章")[0].strip() # for 2018-11-26 電信管理法之例外處理
            
            tempMap = {"標題" : title, 
                       "全文內容" : content,
                       "附件" : ""}
            
            tempMap = getDetailFromContent(soup, tempMap, tabNumber, subColName)
            
            df_detail = df_detail.append(tempMap, ignore_index = True)

            print("爬取成功")
        except:
            logging.error("爬取內文失敗")
            logging.error("失敗連結：" + link)
            traceback.print_exc()

        print("\n")
    return df_detail


# In[8]:


def outputCsv(df, fileName, path):
    # 若目錄不存在，建立目錄
    if not os.path.isdir(path):
        os.mkdir(path)
    df.to_csv(path + fileName + ".csv", index = False, encoding = "utf_8_sig")


# In[9]:


def parsingTitle(driver, checkRange):
    try:
        # 取得上次爬網結果
        if os.path.isfile(lastResultPath):
            lastResult = pd.read_csv(lastResultPath)
        else:
            lastResult = pd.DataFrame()

        # 爬網日期區間為一個禮拜
        endDate = datetime.date.today()
        strDate = (endDate - datetime.timedelta(days = checkRange)).isoformat()
        
        ending = False
        nowPage = 1
        df = pd.DataFrame(columns = ["爬文日期", "發文日期", "標題", "網頁連結"])

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
                d = {"爬文日期" : nowDates, "發文日期" : dates, "標題" : titles, "網頁連結" : links}
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
                
        outputCsv(df, lastResultName, lastResultPath)

        if not lastResult.empty:
            # 若與上次發文日期和標題相同，則跳至下一筆
            for i in range(len(df)):
                for j in range(len(lastResult)):
                    if (df["發文日期"][i] == lastResult["發文日期"][j]) & (df["標題"][i] == lastResult["標題"][j]): 
                        df.drop(i, inplace = True)
                        break

        if len(df) == 0:
            logging.critical("%s 至 %s 間無資料更新" %(strDate, endDate))
        else:
            df.index = [i for i in range(df.shape[0])] # reset 

        return df
    
    except:
        logging.error("爬取主旨列表失敗")
        traceback.print_exc()
        return pd.DataFrame(columns = ["爬文日期", "發文日期", "標題", "網頁連結"])
  


# In[10]:


def main(url, tabNumber, checkRange = 7):
    
    logging.critical("\n")
    logging.critical("爬網開始......")
    logging.critical("目標網址：" + url)
    
    strTime = datetime.datetime.now()
    logging.critical("開始時間：" + strTime.strftime("%Y/%m/%d %H:%M:%S"))
    
    DownloadTool = SeleniumUtil.ChromeDownload()
    DownloadTool.setDownLoadTempPath(TempPath)
    DownloadTool.setDownLoadFinalPath(FinalPath)
    chrome_options = DownloadTool.getChromeOptions()
    driver = webdriver.Chrome(chrome_options = chrome_options) # open chrome browser with Options
    
    try:
        if tabNumber >= 19 and tabNumber <= 22 and isinstance(tabNumber, int):
            url = url + str(tabNumber)
        else:
            raise ValueError("tabNumber 必須為 19 到 22 的整數")
        
        driver.get(url)
        df_1 = parsingTitle(driver, checkRange)
        if len(df_1) != 0:
            outputCsv(df_1, "第一層結果", FinalPath)
        
            df_2 = parsingDetail(df_1, tabNumber, FinalPath)
            outputCsv(df_2, "第二層結果", FinalPath)
    except:
        logging.error("執行爬網作業失敗")
        traceback.print_exc()

    endTime = datetime.datetime.now()
    logging.critical("結束時間：" + endTime.strftime("%Y/%m/%d %H:%M:%S"))
    logging.critical("執行時間：" + str((endTime - strTime).seconds) + " 秒")
    logging.critical("輸出筆數：" + str(len(df_1)) + " 筆")
    logging.critical("爬網結束......\n")


# In[11]:


if __name__ == "__main__":
    url = "http://www.lawbank.com.tw/news/NewsSearch.aspx?TY="
    main(url, 19, 30)


# In[ ]:




