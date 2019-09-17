#!/usr/bin/env python
# coding: utf-8
'''
Project Name: 金管會_法規草案預告
CTM Name    : 
Crawl Cycle : Daily
Main Website: https://www.fsc.gov.tw/ch/home.jsp?id=128&parentpath=0,3
Description : 針對金管會-法規資訊-法規草案預告，每日取得法規草案預告
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
# import docx # 需 istall python-docx
import csv


# In[2]:


TempPath = "./Temp/"  # browser file
FinalPath = "./Result/" # project file
lastResultPath = "./CrawlList/"
lastResultName = "lastResult"


# In[3]:


def getDetailFromContent(soup, content, tempMap):
      
    # 發文字號
    strPos = content.find("發文字號：")
    if strPos != -1:
        strPos = strPos + 5
        stopPos = strPos + content[strPos:].find("號") + 1 # 字號的結尾一定是 "號"
        while content[stopPos] == "、": # 以防多個發文字號
            stopPos = stopPos + content[stopPos:].find("號") + 1
        serialNumber = content[strPos:stopPos].strip()
        content = content[stopPos:]
    else:
        serialNumber = "" # 若無發文字號, 預設為空字串
                
    # 發文日期
    date = soup.select(".contentdate")[0].text.strip()

    # 相關法條 or 依據
    strPos = content.find("依據：")
    if strPos != -1:
        strPos = strPos + 3
        endPos = strPos + content[strPos:].find("：")
        stopPos = strPos + content[strPos:endPos].rfind("。") + 1 # 結尾為下一個 "：" 前最後一個 "。"
        relatedLaw = content[strPos:stopPos].strip()
        content = content[stopPos:]
    else:
        relatedLaw = "" # 若無相關法條 or 依據, 預設為空字串        
    
    tempMap['發文字號'] = serialNumber
    tempMap['發文日期'] = date
    tempMap['相關法條'] = relatedLaw
    
    return tempMap


# In[4]:


def parsingDetail(df, FinalPath):

    df_detail = pd.DataFrame(columns = ["標題", "全文內容", "發文字號", "發文日期", "相關法條", "附件"])

    for link in df["網頁連結"]:
        try:
            print("擷取網址：" + link)
            soup = request2soup(link)

            # 主旨
            title = soup.select("#maincontent h3")[0].text.strip()
            # 全文內容
            content = soup.select(".page_content")[0].text.strip()

            # 附件
            attachments = attachments = soup.select(".acces a") # n 個附件
            
            tempMap = {"標題" : title, 
                       "全文內容" : content, 
                       "附件" : ", ".join(str(e.get("title")).replace("(開啟新視窗)", "") for e in attachments)}
            
            tempMap = getDetailFromContent(soup, content, tempMap)
            
            df_detail = df_detail.append(tempMap, ignore_index = True)


            # 建立 docx
            # file = docx.Document() # 不知是否會影響效能?
            # 將內容寫入 word
            # paragraphs = content.text.split("\n")
            # for paragraph in paragraphs:
            #     file.add_paragraph(paragraph.strip())

            # 儲存 docx
            # file.save(target + "/" + "內文.docx")

            # 附件
            if len(attachments) != 0:
                
                # 建立資料夾
                target = FinalPath + "/" + title[:30].strip()

                # 若目錄不存在, 建立目錄
                if not os.path.isdir(target):
                    os.mkdir(target)

                for attach in attachments:
                    fileLink = "https://www.fsc.gov.tw" + attach.get("href")

                    # 下載附件
                    response = requests.get(fileLink, stream = "TRUE")
                    fileName = attach.get("title").replace("(開啟新視窗)", "")
                    endLoc = fileName.rfind(".") # 檔名結尾位置
                    extName = fileName[endLoc:]  # 副檔名
                    fileName = fileName[:endLoc].strip() # 檔名
                    fileName = fileName[:30]     # 截短檔名
                    with open(target + "/" + fileName + extName, "wb") as file:
                        for data in response.iter_content():
                            file.write(data)

            print("爬取成功")
        except:
            logging.error("爬取內文失敗")
            logging.error("失敗連結：" + link)
            traceback.print_exc()
        
        print("\n")
    return df_detail


# In[5]:


def outputCsv(df, fileName, path, index=False, encoding="utf_8_sig"):
    # 若目錄不存在, 建立目錄
    if not os.path.isdir(path):
        os.mkdir(path)
    # 20190916 新增參數 quoting=csv.QUOTE_NONNUMERIC 以前後雙引號(")包住字串欄位
    df.to_csv(os.path.join(path, fileName+".csv"), index=index, encoding=encoding, quoting=csv.QUOTE_NONNUMERIC)


# In[6]:


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
        
        totalPage = soup.select(".page")[0].text.split("/")[1] # 總頁數
        ending = False
        
        df = pd.DataFrame(columns = ["編號", "爬文日期", "發文日期", "來源機關", "標題", "網頁連結"])

        for page in range(int(totalPage)):
            if (page != 0):
                soup = request2soup(url, page + 1)

            try:
                sorts = soup.select(".sort1")
                sorts = [x.text.strip() for x in sorts]
                
                dates = soup.select(".pdate1")
                dates = [x.text.strip() for x in dates]

                sources = soup.select(".souse1")
                sources = [x.text.strip() for x in sources] 

                titles = soup.select(".ptitle1")
                titles = [x.text.strip() for x in titles] 

                links = soup.select(".ptitle1 a")
                links = ["https://www.fsc.gov.tw/ch/" + x.get("href") for x in links]
                
                nowDates = [str(endDate.year) + "/" + str(endDate.month) + "/" + str(endDate.day)] * len(dates)

                show = pd.Series([False] * len(dates))
                for idx in range(len(dates)):
                    date = dates[idx]
                    if date < strDate: # 若發文日期小於開始日期, 則結束爬取主旨
                        ending = True
                        break
                    elif date > endDate:
                        continue
                    show[idx] = True
                
                d = {"編號" : sorts, "爬文日期" : nowDates, "發文日期" : dates, 
                     "來源機關" : sources, "標題" : titles, "網頁連結" : links}
                df = df.append(pd.DataFrame(data = d)[show])  # append page

                # 若結束爬取主旨, 停止爬取剩下的 pages
                if ending:
                    break
            except:
                logging.error("爬取第 %s 頁主旨發生錯誤" %str(page + 1))
                traceback.print_exc()

        df.index = [i for i in range(df.shape[0])] # reset Index 
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
        return pd.DataFrame(columns = ["編號", "爬文日期", "發文日期", "來源機關", "標題", "網頁連結"])
  


# In[7]:


def request2soup(url, page = None):
    
    if page is None:
        res = requests.get(url)
    else:
        formData = {'id'         : '133', 
                    'contentid'  : '133', 
                    'parentpath' : '0,3', 
                    'mcustomize' : 'lawnotice_list.jsp', 
                    'keyword'    : '請輸入查詢關鍵字', 
                    'page'       : page}
        res = requests.post(url, data = formData)
        
    res.encoding = "utf-8"
    soup = BeautifulSoup(res.text, "html.parser", from_encoding = "utf-8")
    return soup


# In[8]:


def main(url, checkRange = 7):
    
    logging.critical("\n")
    logging.critical("爬網開始......")
    logging.critical("目標網址：" + url)
    
    strTime = datetime.datetime.now()
    logging.critical("開始時間：" + strTime.strftime("%Y/%m/%d %H:%M:%S"))
    
    try:
        soup = request2soup(url, 1)
        
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


# In[9]:


if __name__ == "__main__":
    url = "https://www.fsc.gov.tw/ch/home.jsp?id=133&parentpath=0,3"
    main(url)


# In[ ]:




