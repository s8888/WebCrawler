#!/usr/bin/env python
# coding: utf-8
'''
Project Name: 金管會主管法規查詢系統_法規異動資訊
CTM Name    : 
Crawl Cycle : Daily
Main Website: http://law.fsc.gov.tw/law
Description : 針對金管會主管法規查詢系統，每日取得最新法規異動資訊
Author      : 林竣昇
Update Date : 2018/12/18
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


# In[2]:


TempPath = "./Temp/"  # browser file
FinalPath = "./Result/" # project file
lastResultPath = "./CrawlList/"
lastResultName = "lastResult"


# In[3]:


def getDetailFromContent(soup, content, tempMap, link):
    if link.find("law.fsc.gov.tw") != -1:
        # 發文字號
        serialNumber = soup.select("#ctl00_cp_content_trODWord td")
        if len(serialNumber) == 0:
            serialNumber = soup.select("tr:nth-of-type(3) td")
        serialNumber = serialNumber[0].text[:serialNumber[0].text.find("號") + 1].strip()

        # 發文日期
        date = soup.select("#ctl00_cp_content_trAnnDate td")
        if len(date) == 0:
            date = soup.select("td.text-middle")[0].text.strip()
            date = date[:date.find("\r")]
        else: 
            date = date[0].text.strip()

        # 相關法條
        strPos = content.find("據：")
        if strPos != -1:
            strPos = strPos + 2
            endPos = strPos + content[strPos:].find("：")
            stopPos = strPos + content[strPos:endPos].rfind("\r")
            relatedLaw = content[strPos:stopPos].strip()
        else:
            relatedLaw = ""
            
    elif link.find("law.moj.gov.tw"):
        # 發文字號
        strPos = content.find("日")
        if strPos != -1:
            strPos = strPos + 1
            endPos = content.find("號") + 1
            serialNumber = content[strPos:endPos].strip()

        # 發文日期
        date = soup.select("#lbDate")[0].text.strip()

        # 相關法條
        relatedLaw = ""        
        
    tempMap['發文字號'] = serialNumber
    tempMap['發文日期'] = date
    tempMap['相關法條'] = relatedLaw
    
    return tempMap


# In[4]:


def getPdfInsideWebsite(link, df, FinalPath):
    try:
        soup = request2soup(link)
        # 主旨
        title = soup.select(".Block h3")[0].text.strip()
        # 附件
        attachment = soup.select(".embed-responsive-item")[0].get("src")
        if attachment.find("https") == -1:
            attachment = "https://gazette.nat.gov.tw" + attachment

        df = df.append({"標題" : title, 
                        "附件" : title}, 
                       ignore_index = True)

        # 建立資料夾
        target = FinalPath + "/" + title[:30].strip()

        # 若目錄不存在，建立目錄
        if not os.path.isdir(target):
            os.makedirs(target)
    
        # 下載附件
        response = requests.get(attachment, stream = "TRUE")
        with open(target + "/" + title[:30] + ".pdf", "wb") as file:
            for data in response.iter_content():
                file.write(data)
                
        print("爬取成功")
    except:
        print("爬取內文失敗")
        print("失敗連結：" + link)
        logging.error("爬取內文失敗")
        logging.error("失敗連結：" + link)
        traceback.print_exc()
            
    return df


# In[5]:


def parsingDetail(df, FinalPath):

    df_detail = pd.DataFrame(columns = ["標題", "全文內容", "發文字號", "發文日期", "相關法條", "附件"])

    for link in df["網頁連結"]:
        try:
            print("爬取網址：" + link)
            
            linkSplit = link.split("=")[-1]

            # Case1: 內嵌 PDF
            if "detailLog" == linkSplit:
                df_detail = getPdfInsideWebsite(link, df_detail, FinalPath)

            else:
                # 內容連結
                soup = request2soup(link)
                subLink = soup.select("#ctl00_cp_content_hlkAnnTitle")
                
                # Case2: 表格內嵌 PDF
                if len(subLink) > 0:
                    subLink = subLink[0].get("href")
                    df_detail = getPdfInsideWebsite(subLink, df_detail, FinalPath)

                # Case3: 表格板
                else:
                    try:
                        if link.find("law.fsc.gov.tw") != -1:
                            # 主旨
                            title = soup.select("h2")[0].text.strip()
                            # 全文內容
                            content = soup.select(".text-con")[0].text.strip()
                            # 附件
                            attachments = soup.select("#ctl00_cp_content_trAnnFiles02 td a") # n 個附件
                        
                        elif link.find("law.moj.gov.tw") != -1:
                            # 主旨
                            title = soup.select("#lbAbstract")[0].text.strip()
                            # 全文內容
                            content = soup.select("pre")[0].text.strip()
                            # 附件
                            attachments = []
                        
                        tempMap = {"標題" : title, 
                                   "全文內容" : content, 
                                   "附件" : " , ".join(str(e.text) for e in attachments)}

                        tempMap = getDetailFromContent(soup, content, tempMap, link)
                        
                        df_detail = df_detail.append(tempMap, ignore_index = True)
                        
                        if len(attachments) != 0:
                            target = FinalPath + "/" + title[:30].strip() # 資料夾檔名取 title 前 30 

                            # 若目錄不存在，建立目錄
                            if not os.path.isdir(target):
                                os.makedirs(target)

                            # 下載附件
                            for attach in attachments:
                                response = requests.get(url + "/" + attach.get("href"), stream = "TRUE")
                                fileName = attach.text
                                endLoc = fileName.rfind(".") # 檔名結尾位置
                                extName = fileName[endLoc:]  # 副檔名
                                fileName = fileName[:endLoc].strip() # 檔名
                                fileName = fileName[:30]     # 截短檔名

                                with open(target + "/" + fileName + extName, "wb") as file:
                                    for data in response.iter_content():
                                        file.write(data)
                                        
                        print("爬取成功")
                    except:
                        print("爬取內文失敗")
                        print("失敗連結：" + link)
                        logging.error("爬取內文失敗")
                        logging.error("失敗連結：" + link)
                        traceback.print_exc()
                        
        except:
            print("爬取內文失敗")
            logging.error("爬取內文失敗")
            traceback.print_exc()
        print("\n")
    
    return df_detail


# In[6]:


def outputCsv(df, fileName, path):
    # 若目錄不存在，建立目錄
    if not os.path.isdir(path):
        os.mkdir(path)
    df.to_csv(path + fileName + ".csv", index = False, encoding = "utf_8_sig")


# In[7]:


def parsingTitle(soup, checkRange):

    try:
        # 取得上次爬網結果
        if os.path.isfile(lastResultPath + lastResultName):
            lastResult = pd.read_csv(lastResultPath + lastResultName)
        else:
            lastResult = pd.DataFrame()
        
        # 爬網日期區間為一個禮拜
        endDate = datetime.date.today()
        strDate = endDate - datetime.timedelta(days = checkRange)
        
        dates = []
        titles = []
        links = []

        totalPages = int(soup.select(".pageinfo")[0].text.split("\r\n")[4].strip()) # 總頁數
        pageCounts = int(len(soup.select("td")) / 4) # 每頁筆數
        nowPage = 1 
        ending = False # 是否結束主旨爬網
        

        while True:
            for i in range(pageCounts):
                try:
                    date = soup.select("td")[i * 4 + 1].text.strip()
                    dateDT = date.split(".")
                    dateDT = datetime.date(int(dateDT[0]) + 1911, int(dateDT[1]), int(dateDT[2])) # 轉換成西元年
                    
                    # 若發文日期小於開始日期, 則結束爬取主旨
                    if strDate > dateDT:
                        ending = True
                        break
                    
                    # 去除前段文字才會與第二層 title 相符
                    title = soup.select("td a")[i].text.strip()
                    if ("金融監督管理委員會令：" in title) | ("金融監督管理委員會公告：" in title):
                        title = title.split("：")[1]

                    dates.append(date)
                    titles.append(title)
                    link = soup.select("td a")[i].get("href")
                    if link.find("http") == -1:
                        link = url + "/" + link
                    links.append(link)   

                except:
                    print("爬取第 %s 頁第 %s 筆主旨發生錯誤" %(nowPage, i + 1))
                    logging.error("爬取第 %s 頁第 %s 筆主旨發生錯誤" %(nowPage, i + 1))
                    traceback.print_exc()

            # 若結束爬取主旨, 停止爬取剩下的 pages
            if ending:
                break 
                
            nowPage += 1
            if nowPage > totalPages:
                break
            soup = request2soup(url + "/?&page=%s" %(nowPage))  
            
        nowDates = [str(endDate.year - 1911) + "." + str(endDate.month) + "." + str(endDate.day)] * len(dates)

        d = {"爬文日期" : nowDates, "發文日期" : dates, "標題" : titles, "網頁連結" : links}
        df = pd.DataFrame(data = d, columns = ["爬文日期", "發文日期", "標題", "網頁連結"])

        # 將這次爬網儲存以便下次爬網比對
        outputCsv(df, lastResultName, lastResultPath)

        if not lastResult.empty:
            # 若與上次發文日期和標題相同，則刪除此筆資料
            for i in range(len(df)):
                for j in range(len(lastResult)):
                    if (df["發文日期"][i] == lastResult["發文日期"][j]) & (df["標題"][i] == lastResult["標題"][j]): 
                        df.drop(i, inplace = True)
                        break
                        
        if len(df) == 0:
            print("%s 至 %s 間無資料更新" %(strDate, endDate))
            logging.critical("%s 至 %s 間無資料更新" %(strDate, endDate))
            return pd.DataFrame(columns = ["爬文日期", "發文日期", "標題", "網頁連結"])
        
        return df
    except:
        print("爬取主旨列表失敗")
        logging.error("爬取主旨列表失敗")
        traceback.print_exc()
        return pd.DataFrame(columns = ["爬文日期", "發文日期", "標題", "網頁連結"])
  


# In[8]:


def request2soup(url):
    res = requests.get(url)
    res.encoding = "utf-8"
    soup = BeautifulSoup(res.text, "html.parser", from_encoding = "utf-8")
    return soup


# In[9]:


def main(url, checkRange = 7):
    
    logging.critical("\n")
    logging.critical("爬網開始......")
    logging.critical("目標網址：" + url)
    
    strTime = datetime.datetime.now()
    logging.critical("開始時間：" + strTime.strftime("%Y/%m/%d %H:%M:%S"))

    try:
        soup = request2soup(url)
        
        df_1 = parsingTitle(soup, checkRange)
        if len(df_1) != 0:
            outputCsv(df_1, "第一層結果", FinalPath)
            
            df_2 = parsingDetail(df_1, FinalPath)
            outputCsv(df_2, "第二層結果", FinalPath)
    except:
        print("執行爬網作業失敗")
        logging.error("執行爬網作業失敗")
        traceback.print_exc()

    endTime = datetime.datetime.now()
    logging.critical("結束時間：" + endTime.strftime("%Y/%m/%d %H:%M:%S"))
    logging.critical("執行時間：" + str((endTime - strTime).seconds) + " 秒")
    logging.critical("輸出筆數：" + str(len(df_1)) + " 筆")
    logging.critical("爬網結束......\n")    


# In[10]:


if __name__ == "__main__":
    url = "http://law.fsc.gov.tw/law"
    main(url)


# In[ ]:




