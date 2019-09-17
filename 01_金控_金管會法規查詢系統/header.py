import logging
import os
import sys
import datetime
import shutil
import re
import requests
import pandas as pd
import csv

#private variable
_path = sys.argv[0]
_head, _tail = os.path.split(_path)
_strTime = datetime.datetime.now()


#public variable for common use
IS_LOCAL = _tail == 'ipykernel_launcher.py' # ipynb vs py 
PATH = "" if IS_LOCAL else os.path.dirname(_path) +"/"  
PROJECT = "" if IS_LOCAL else os.path.splitext(os.path.basename(sys.argv[0]))[0]
TIMELABEL = sys.argv[1] if ( not IS_LOCAL and len(sys.argv)>1) else _strTime.strftime("%Y%m%d%H%M%S") 
PROJECT_TIMELABEL = PROJECT+'_'+TIMELABEL
LOG_PATH = PATH +"./Log"
TEMP_PATH = PATH +"./Temp"  # browser file
FINAL_PATH = PATH +"./Result" #  project file
CRAWL_LIST_PATH =  PATH + "./CrawlList"
LAST_RESULT_PATH = CRAWL_LIST_PATH + "/lastResult.csv"
EXIT_CODE = 0  
RESULT_COUNT = 0
FILE_INDEX = 1
lastResult = pd.DataFrame()

# log setting
logging.basicConfig(level=logging.DEBUG, #DEBUG, INFO, WARNING, ERROR, CRITICAL
                    filename= LOG_PATH + '/'+PROJECT_TIMELABEL+ '_log.txt',
                    filemode='w',
                    format='%(asctime)s - %(levelname)8s - %(name)s[line:%(lineno)d] : %(message)s')


# time record for log
def processBegin(url=''):
    logging.critical("\n")
    logging.critical("爬網開始......")
    logging.critical("目標網址：" + url)
    _strTime = datetime.datetime.now()
    logging.critical("開始時間：" + _strTime.strftime("%Y/%m/%d %H:%M:%S"))

def processEnd():
    endTime = datetime.datetime.now()
    logging.critical("結束時間：" + endTime.strftime("%Y/%m/%d %H:%M:%S"))
    logging.critical("執行時間：" + str((endTime - _strTime).seconds) + " 秒")
    logging.critical("輸出筆數：" + str(RESULT_COUNT) + " 筆")
    logging.critical("爬網結束......\n")

# csv output    
def outputCsv(df, fileName, path= FINAL_PATH, encoding = "utf_8_sig", index = False):
    # check file exist
    if not os.path.isdir(path):
        os.mkdir(path)
    # 20190916 新增參數 quoting=csv.QUOTE_NONNUMERIC 以前後雙引號(")包住字串欄位
    df.to_csv( os.path.join(path,fileName + ".csv"), index=index, encoding=encoding, quoting=csv.QUOTE_NONNUMERIC)
    
    
# clean folder    
def clearFolder():
    removeFile(TEMP_PATH)
    removeFile(FINAL_PATH)
#     removeFile(LOG_PATH)
#     if crawlList:
#         removeFile(CRAWL_LIST_PATH)
        
def removeFile(filePath):
    logging.debug('removeFile path:'+filePath)
    if not os.path.exists(filePath):
        return
    for file in os.listdir(filePath):
        subfile = os.path.join(os.path.abspath(filePath),file)
        logging.debug('subfile:'+subfile)
        if os.path.isfile(subfile):
            os.remove(subfile)
        else:    
            shutil.rmtree(subfile)            

#------------------ customize -------------------------------------------            
            
# zipfile 
def zipFile(fileName = PROJECT_TIMELABEL, targetPath = FINAL_PATH , zipFolder = FINAL_PATH):
    logging.debug('zip fileName:'+fileName)
    logging.debug('zip targetPath:'+targetPath)
    logging.debug('zip zipFolder:'+zipFolder)
    #TODO consider merge fileName, targetPath
    shutil.make_archive(os.path.join(TEMP_PATH,fileName),'zip',zipFolder)
    shutil.move(os.path.join(TEMP_PATH,fileName+'.zip'),os.path.join(targetPath,fileName+'.zip') )

# ok file for process execute successfully    
def createOKFile():
    if EXIT_CODE == 0:    # 若EXIT_CODE為0，亦即爬網成功，才產生ok檔，否則不產出ok檔
        filename = os.path.join(FINAL_PATH, PROJECT_TIMELABEL+'.ok') 
        logging.debug('createOKFile:'+filename)
        open(filename,'a')    

# ok file for process execute successfully    
def createInfoFile():    
    filename = os.path.join(FINAL_PATH, PROJECT_TIMELABEL+'_info.txt')
    logging.debug('createInfoFile:'+filename)
    file = open(filename,'w')    
    
    result = 'FAIL' if EXIT_CODE != 0 else 'SUCCESS'
    
    # PROJECT,START_TIME,END_TIME,(SUCCESS/FAIL),(NUM/0),ZIP_FILE,FILE_PATH
    content = PROJECT+','+ TIMELABEL +','+datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    content += ',' + result + ',' + str(RESULT_COUNT) 
    content += ',,' if result == 'FAIL' else ',' + PROJECT_TIMELABEL + '.zip,'
    logging.debug('file content:'+content)
    file.write(content)
    file.close()

# download file
def downloadFile(FOLDER_NM, finalPath, fileUrls, fileNames): # for download pdf or doc
    target = finalPath + '/' + FOLDER_NM
    # 若目錄不存在，建立目錄
    if not os.path.isdir(target):
        os.makedirs(target)
    
    for file_url, fileName in zip(fileUrls, fileNames):
        try:
            response = requests.get(file_url, stream="TRUE")
            downloadFile = target + '/' + fileName # 放置資料夾路徑 + 檔名
            logging.info(downloadFile + '\r\n')
            with open(downloadFile,'wb') as file:
                for data in response.iter_content():
                    file.write(data)
        except:
            header.EXIT_CODE = -1   # 2019/02/01 爬取檔案發生錯誤則重爬
            logging.error("爬取檔案失敗")
            logging.error("失敗連結：" + file_url)
            traceback.print_exc()

# deal with space and wrap special words
def spaceAndWrapProcess(string):
    return re.sub(r'\s+', ' ', re.sub(r'[\n|\r\n]+', r'\\n', string))

# deal with duplicate files
def processDuplicateFiles(FILES_NM):
    index = 0
    for ele in FILES_NM:
        fileTuple = os.path.splitext(ele)
        FILES_NM[index] = fileTuple[0].strip() + '_' + str(index) + fileTuple[1]
        index += 1
    return FILES_NM

# 2019/02/01 if EXIT_CODE == 0, then output lastResult
def outputLastResult(df_1, lastResult, checkRange):
    if EXIT_CODE == 0:
         # 將新爬到的資料和lastResult合併
        lastResult = pd.concat([df_1, lastResult], ignore_index = True)
        lastResult = lastResult[pd.to_datetime(lastResult['CRL_DATE']) >= (datetime.date.today() - datetime.timedelta(days=checkRange))]
        logging.info(LAST_RESULT_PATH)
        outputCsv(lastResult, "lastResult", CRAWL_LIST_PATH)
        
