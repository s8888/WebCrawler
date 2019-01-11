#!/usr/bin/env python
# coding: utf-8
'''
Project Name: 保險局法規草案預告
CTM Name    : 
Crawl Cycle : Daily
Main Website: https://www.ib.gov.tw/ch/home.jsp?id=38&parentpath=0,3
Description : 保險局法規草案預告
Author      : 謝子嫣
Update Date : 2018.11.20
'''
# In[1]:


import header
import logging

import datetime
import pandas as pd
import traceback
from bs4 import BeautifulSoup

import os
import requests
import re


# In[2]:


def getResult(findall, STR_NUM):
    if findall != []:
        result = findall[0][STR_NUM:-1]
    else:
        result = ""
    return result


# In[3]:


def parsingDetail(df, FinalPath):

    df_detail = pd.DataFrame(columns = ["標題", "全文內容", "附件", "發文字號", "發文日期", "相關法條"])

    for link in df.link:
        try:
            print("擷取網址：" + link)
            soup = request2soup(link)

            # 主旨
            title = soup.select("h3")[1]
            # 內容
            content = soup.select(".page_content")[0]
            # 附件
            attachments = soup.select(".acces a") # n 個附件
            # 日期
            date = soup.select(".contentdate")[0]
            # 新增欄位
            str_content = str(content)
            findall01 = re.findall(r'發文字號.+?<', str_content)
            result01 = getResult(findall01, 5)
            findall02 = re.findall(r'發文日期.+?<', str_content)
            result02 = getResult(findall02, 5)
            findall03 = re.findall(r'依據.+?<', str_content)
            result03 = getResult(findall03, 3)
            
            df_detail = df_detail.append({"標題" : title.text, 
                                          "全文內容" : content.text, 
                                          "附件" : " , ".join(str(e.get("title")).replace("(開啟新視窗)", "") 
                                                            for e in attachments),
                                          "發文字號" : result01,
                                          "發文日期" : result02,
                                          "相關法條" : result03
                                         }, 
                                         ignore_index = True)

            # 附件
            if len(attachments) != 0:
                
                # 建立資料夾
                target = FinalPath + "/" + date.text + title.text[:30].strip()
                
                # 若目錄不存在, 建立目錄
                if not os.path.isdir(target):
                    os.mkdir(target)
                    
                for attach in attachments:
                    fileLink = "https://www.ib.gov.tw" + attach.get("href")
                    # 下載附件
                    response = requests.get(fileLink, stream = "TRUE")
                    fileName = attach.get("title").replace("(開啟新視窗)", "")
                    endLoc = fileName.rfind(".") # 檔名結尾位置
                    extName = fileName[endLoc:]  # 副檔名
                    fileName = fileName[:endLoc] # 檔名
                    fileName = fileName[:30]     # 截短檔名
                    with open(target + "/" + fileName + extName, "wb") as file:
                        for data in response.iter_content():
                            file.write(data)

            #print("爬取成功")
        except:
            #print("爬取內文失敗")
            #print("失敗連結：" + link)
            logging.error("爬取內文失敗")
            logging.error("失敗連結：" + link)
            traceback.print_exc()
        
        #print("\n")
    return df_detail


# In[4]:


def outputCsv(df, fileName, path):
    # 若目錄不存在, 建立目錄
    if not os.path.isdir(path):
        os.mkdir(path)
    df.to_csv(path + "/" + fileName + ".csv", index = False, encoding = "utf_8_sig")


# In[5]:


def parsingTitle(soup, checkRange):
    try:
        # 取得上次爬網結果
        lastResultPath = "./CrawlList/lastResult.csv"
        if os.path.isfile(lastResultPath):
            lastResult = pd.read_csv(lastResultPath)
        else:
            lastResult = pd.DataFrame()
        
        # 爬網日期區間為一個禮拜
        endDate = datetime.date.today()
        strDate = (endDate - datetime.timedelta(days = checkRange)).isoformat()
        
        totalPage = soup.select(".page")[0].text.split("/ ")[1] # 總頁數
        ending = False
        
        df = pd.DataFrame(columns = ["編號", "爬文日期", "發布日期", "標題", "link"])

        for i in range(int(totalPage)):
            if (i != 0):
                soup = request2soup(url, i + 1)

            try:
                sorts = soup.select(".sort1")
                sorts = [x.text.strip() for x in sorts]
                
                strTime = endDate
                searchDates = str(strTime.year) + str(strTime.month) +  str(strTime.day)
                
                dates = soup.select(".pdate1")
                dates = [x.text.strip() for x in dates]

                titles = soup.select(".ptitle1")
                titles = [x.text.strip() for x in titles] 

                links = soup.select(".ptitle1 a")
                links = ["https://www.ib.gov.tw/ch/" + x.get("href") for x in links]

                idx = pd.Series([False] * len(dates))
                for j in range(len(dates)):
                    date = dates[j]
                    if date < strDate: # 若發文日期小於開始日期, 則結束爬取主旨
                        ending = True
                        break
                    idx[j] = True
                
                d = {"編號" : sorts, "爬文日期" : searchDates, "發布日期" : dates, "標題" : titles, "link" : links}
                df = df.append(pd.DataFrame(data = d)[idx])  # append page

                # 若結束爬取主旨, 停止爬取剩下的 pages
                if ending:
                    break
            except:
                #print("爬取第 %s 頁主旨發生錯誤" %str(i + 1))
                logging.error("爬取第 %s 頁主旨發生錯誤" %str(i + 1))
                traceback.print_exc()

        df.index = [i for i in range(df.shape[0])] # reset Index 
        outputCsv(df, "lastResult", "./CrawlList")

        if not lastResult.empty:
            # 若與上次發文日期和標題相同，則跳至下一筆
            for i in range(len(df)):
                for j in range(len(lastResult)):
                    if (df.date[i] == lastResult.date[j]) & (df.title[i] == lastResult.title[j]): 
                        df.drop(i, inplace = True)
                        break

        if len(df) == 0:
            #print("%s 至 %s 間無資料更新" %(strDate, endDate))
            logging.critical("%s 至 %s 間無資料更新" %(strDate, endDate))
        else:
            df.index = [i for i in range(df.shape[0])] # reset 

        return df
    
    except:
        #print("爬取主旨列表失敗")
        logging.error("爬取主旨列表失敗")
        traceback.print_exc()
        return pd.DataFrame(columns = ["date", "source", "title", "link"])
  


# In[6]:


def request2soup(url, page = None):
    if page is None:
        res = requests.get(url)
    else:
        formData = {"id"         : "37", 
                    "contentid"  : "37", 
                    "parentpath" : "0,3", 
                    "mcustomize" : "lawnews_list.jsp", 
                    "keyword"    : "請輸入查詢關鍵字", 
                    "page"       : page}
        res = requests.post(url, data = formData)
        
    res.encoding = "utf-8"
    soup = BeautifulSoup(res.text, "html.parser", from_encoding = "utf-8")
    return soup


# In[7]:


def main(url, checkRange = 30):
    
    logging.critical("\n")
    logging.critical("爬網開始......")
    logging.critical("目標網址：" + url)
    
    strTime = datetime.datetime.now()
    logging.critical("開始時間：" + strTime.strftime("%Y/%m/%d %H:%M:%S"))
    
    TempPath = "./Temp"  # browser file
    FinalPath = "./Result" # project file
    
    try:
        soup = request2soup(url)
        
        df_1 = parsingTitle(soup, checkRange)
        if len(df_1) != 0:
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
    logging.critical("爬網結束......\n")


# In[8]:


if __name__ == "__main__":
    url = "https://www.ib.gov.tw/ch/home.jsp?id=38&parentpath=0,3"
    main(url)

