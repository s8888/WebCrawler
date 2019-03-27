#!/usr/bin/env python
# coding: utf-8
'''
Project Name: EP_WC0101
CTM Name    : 
Crawl Cycle : Weekly
Main Website: https://business.591.com.tw
Description : RPA爬網需求：定期蒐尋仲介網站出售案件
Author      : 張至偉

Update Date : 2019.03.22
'''
# In[1]:

import header

import os
import re
import datetime
import logging
import traceback

import requests
import pandas as pd
from bs4 import BeautifulSoup

TODAY = str(datetime.date.today())

WEB_URL = "https://business.591.com.tw"

# In[2]:

def getBlockInfo(itemSoup, patternKey, patternValue):

    # 使用 CSS 選擇器，選取指定區塊
    # 根據區塊處理，並回傳 dict 的查詢格式 {"標題":"內容"}
    rtnMap = {}
    keyList = itemSoup.select(patternKey)
    valueList = itemSoup.select(patternValue)

    for i, key in enumerate(keyList):
        rtnMap[key.text] = valueList[i].text

    return rtnMap

# In[3]:

def selectValueByPattern(soup, patternStr, index, removeChild=False, strSlice=None):

    selectedList = soup.select(patternStr)

    # 根據 CSS 選擇器，選擇欲爬取的內容文字；若無文字顯示 "None"
    if len(selectedList) > index:

        # 是否移除子元素
        if removeChild:
            for childElement in selectedList[index].find_all():
                childElement.decompose()

        fieldValue = selectedList[index].text.strip()
    else:
        fieldValue = "None"

    # 以空白取代換行
    fieldValue = ' '.join(fieldValue.splitlines())

    return fieldValue

# In[4]:

def removeWord(inputString, removePatterns):

    rtnString = inputString
    for pString in removePatterns:
        pattern = re.compile("(\s*)" + pString + "(\s*)")
        rtnString = pattern.sub('', rtnString)

    return rtnString

# In[5]:

def getItemInfo(itemUrl):
    
    itemSoup = request2soup(itemUrl)
    addrInfo = getBlockInfo(itemSoup, "span.info-addr-key", "span.info-addr-value")
    floorInfo = getBlockInfo(itemSoup, "div.info-floor-value", "div.info-floor-key")
    detailInfo = getBlockInfo(itemSoup, "div.detail-house-key", "div.detail-house-value")

    # 檢核坪數是否大於 1000 坪
    floorArea = removeWord(floorInfo.get("權狀坪數", "None"), ["坪", "\(含車位\)"])
    if float(floorArea) < 1000:
        return None

    rtnMap = {}
    rtnMap["編號"] = selectValueByPattern(itemSoup, "span.breadList-last", 0)
    rtnMap["標題"] = selectValueByPattern(itemSoup, "h1.detail-title-content", 0)
    rtnMap["有效期"] = removeWord(selectValueByPattern(itemSoup, "span.detail-info-span.pull-right", 0), ["有效期："])
    rtnMap["總價"] = selectValueByPattern(itemSoup, "span.info-price-num", 0, removeChild=True)
    rtnMap["金額單位"] = selectValueByPattern(itemSoup, "span.info-price-unit", 0)
    rtnMap["權狀坪數(坪)"] = floorArea
    rtnMap["單價(萬/坪)"] = removeWord(selectValueByPattern(itemSoup, "div.info-price-per", 0), ["單價：", "萬/坪房貸試算"])
    rtnMap["樓層"] = floorInfo.get("樓層", "None")
    rtnMap["屋齡"] = floorInfo.get("屋齡", "None")
    rtnMap["型態"] = addrInfo.get("型態", "None")
    rtnMap["縣市"] = selectValueByPattern(itemSoup, "div.breadList > a", 2)
    rtnMap["區域"] = selectValueByPattern(itemSoup, "div.breadList > a", 3)
    rtnMap["地址"] = addrInfo.get("地址", "None")
    rtnMap["仲介"] = selectValueByPattern(itemSoup, "div.info-host-name > span.info-span-name", 0)
    rtnMap["經紀業"] = selectValueByPattern(itemSoup, "div.info-host-detail > span", 0)
    rtnMap["電話"] = selectValueByPattern(itemSoup, "span.info-host-word", 0)
    rtnMap["現況"] = detailInfo.get("現況", "None")
    rtnMap["帶租約"] = detailInfo.get("帶租約", "None")
    rtnMap["法定用途"] = detailInfo.get("法定用途", "None")
    rtnMap["車位"] = detailInfo.get("車位", "None")
    rtnMap["屋況特色"] = selectValueByPattern(itemSoup, "div.detail-feature-text", 0)
    rtnMap["待售物件連結"] = itemUrl

    return rtnMap

# In[6]:

def parsingDetail(standbyDataFrame):
    
    itemUrlList = list(standbyDataFrame["link"])

    detailDataFrame = pd.DataFrame(columns=["編號", "標題", "有效期", "總價", "金額單位", "權狀坪數(坪)", "單價(萬/坪)", "樓層", 
        "屋齡", "型態", "縣市", "區域", "地址", "仲介", "經紀業", "電話", "現況", "帶租約", "法定用途", "車位", "屋況特色", "待售物件連結"])

    for itemUrl in itemUrlList:
        try:
            itemInfo = getItemInfo(itemUrl)
            detailDataFrame = detailDataFrame.append(itemInfo, ignore_index=True)
        except:
            # 移除未成功的連結
            standbyDataFrame = standbyDataFrame[standbyDataFrame["link"] != itemUrl]
            setErrorMessage("爬取商品內文失敗，失敗連結：" + itemUrl, setEXIT_CODE=False)
    
    return standbyDataFrame, detailDataFrame

# In[7]:

def parsingTitle(reqMap):

    try:
        # 取得上次爬網結果
        historyFile = os.path.join(header.LAST_RESULT_PATH, "crawlHistory.csv")
        if os.path.isfile(historyFile):
            historyDataFrame = pd.read_csv(historyFile)
            historyList = list(historyDataFrame["link"])
        else:
            historyDataFrame = None
            historyList = []

        # 爬網日期, 連結, 標題
        standbyDataFrame = pd.DataFrame(columns=["date", "link", "title"])

        queryParams = reqMap["CONDITIONS"]

        # 設定查詢地區
        for regionId in reqMap["REGION_IDs"]:

            queryParams["firstRow"] = 0

            # 確認總筆數
            tmpSoup = request2soup(WEB_URL, regionId, queryParams)
            totalNumberPattern = tmpSoup.select("div.pull-left.hasData > i")
            totalNumber = int(totalNumberPattern[0].text) if len(totalNumberPattern) > 0 else 0

            # 查無資料，執行下一個查詢條件
            if totalNumber < 1:
                continue

            try:
                # 逐頁處理，每頁30筆
                for itemNo in range(0, totalNumber, 30):
                    
                    # 設定查詢頁數
                    queryParams["firstRow"] = itemNo

                    # 取得商品列表
                    rtnListSoup = request2soup(WEB_URL, regionId, queryParams)
                    items = rtnListSoup.select("li.infoContent > h3 > a")

                    # 過濾已爬取的物品，並記錄此次待查清單
                    for item in items:
                        url = "https:" + ' '.join(item.get("href").split())
                        if url not in historyList:
                            dataInfo = {"date":TODAY, "link":url, "title":item.text}
                            standbyDataFrame = standbyDataFrame.append(dataInfo, ignore_index=True)

            except:
                errMsg = "爬取區域：%s 第 %s 頁商品列表發生錯誤" % (reqMap["REGION_IDs"][region_id], str(int(page_num/30)+1))
                setErrorMessage(errMsg, setEXIT_CODE=False)

        return standbyDataFrame, historyDataFrame

    except:
        setErrorMessage("爬取商品列表失敗")
        return pd.DataFrame(columns=["date", "link", "title"]), None

# In[8]:

def request2soup(url, regionId=None, conditions=None):

    HEADERS = {
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.146 Safari/537.36",
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "en-US,en;q=0.9",
        "connection": "keep-alive",
        "dnt": "1"
    }

    # 設定查詢地區
    regionCookie = None if regionId == None else {"urlJumpIp": regionId}

    res = requests.get(url, headers=HEADERS, cookies=regionCookie, params=conditions)
    res.encoding = "utf-8"

    return BeautifulSoup(res.text, "html.parser")

# In[9]:

def setErrorMessage(errorMsg, setEXIT_CODE=True):

    if setEXIT_CODE:
        header.EXIT_CODE = -1

    print(errorMsg)
    logging.error(errorMsg)
    traceback.print_exc()

# In[10]:

def main():
    header.processBegin(url=WEB_URL)
    header.clearFolder()

    try:
        reqMap = {
            "REGION_IDs": {'1':"台北", '3':"新北", '4':"新竹", '6':"桃園"},
            "CONDITIONS": {
                "is_new_list": '1',
                "type": '2',
                "searchtype": '1',
                "firstRow": '0',
                "kind": '6', # 辦公出售
                "area": "1000," # 最小 1,000 坪
            }
        }

        standbyDataFrame, historyDataFrame = parsingTitle(reqMap)

        if len(standbyDataFrame) < 1:
            # 無資料更新
            logMsg = "無資料更新，爬網日期：" + TODAY
            print(logMsg)
            logging.critical(logMsg)
        else:
            finishDataFrame, detailDataFrame = parsingDetail(standbyDataFrame)

            header.outputCsv(detailDataFrame, header.PROJECT)
            header.RESULT_COUNT = len(detailDataFrame)

            # 更新 crawlHistory 檔案
            updateHistoryDataFrame = pd.concat([historyDataFrame, finishDataFrame], ignore_index=True)
            header.outputCsv(updateHistoryDataFrame, "crawlHistory", header.LAST_RESULT_PATH)
        
        header.zipFile()
        header.createInfoFile()
        header.createOKFile()
        
    except:
        setErrorMessage("執行爬網作業失敗")
        header.createInfoFile()
        header.zipFile(zipFolder=header.LOG_PATH, zipResultWithLog=False)

    header.processEnd()

# In[11]:

if __name__ == "__main__":
    print(header.TIMELABEL)
    logging.fatal("FINAL_PATH:" + header.FINAL_PATH)
    main()

