import logging
import os
import sys
import datetime
import shutil
import csv

#private variable
_path = sys.argv[0]
_head, _tail = os.path.split(_path)
_strTime = datetime.datetime.now()


#public variable for common use
IS_LOCAL = _tail == 'ipykernel_launcher.py' # ipynb vs py

#TODO 請自訂路徑
# PATH = "" if IS_LOCAL else os.path.dirname(_path) + "/"
PATH = ""

#TODO 請自訂專案名稱
# PROJECT = "" if IS_LOCAL else os.path.splitext(os.path.basename(sys.argv[0]))[0]
PROJECT = "tmp_proj_name"


TIMELABEL = sys.argv[1] if ( not IS_LOCAL and len(sys.argv)>1) else _strTime.strftime("%Y%m%d%H%M%S") 
LOG_PATH = PATH +"./Log"
TEMP_PATH = PATH +"./Temp"  # browser file
FINAL_PATH = PATH +"./Result" #  project file
CRAWL_LIST_PATH =  PATH + "./CrawlList"
LAST_RESULT_PATH = CRAWL_LIST_PATH
EXIT_CODE = 0  
RESULT_COUNT = 0

# log setting
logging.basicConfig(level=logging.DEBUG, #DEBUG, INFO, WARNING, ERROR, CRITICAL
                    filename= LOG_PATH + '/log.txt',
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
def outputCsv(df, fileName, path=FINAL_PATH, encoding="utf_8_sig", index=False):
    # check file exist
    if not os.path.isdir(path):
        os.mkdir(path)
    df.to_csv( os.path.join(path,fileName + ".csv"), index=index, encoding=encoding, quoting=csv.QUOTE_NONNUMERIC)
    
    
# clean folder    
def clearFolder(crawlList = False):
    removeFile(TEMP_PATH)
    removeFile(FINAL_PATH)
    if crawlList:
        removeFile(CRAWL_LIST_PATH)
        
def removeFile(filePath):
    logging.debug('removeFile path:'+filePath)
    for file in os.listdir(filePath):
        subfile = os.path.join(os.path.abspath(filePath),file)
        logging.debug('subfile:'+subfile)
        if os.path.isfile(subfile):
            os.remove(subfile)
        else:    
            shutil.rmtree(subfile)            

#------------------ customize -------------------------------------------            
            
# zipfile 
def zipFile(fileName = PROJECT+'_'+TIMELABEL, targetPath = FINAL_PATH , zipFolder = FINAL_PATH):
    logging.debug('zip fileName:'+fileName)
    logging.debug('zip targetPath:'+targetPath)
    logging.debug('zip zipFolder:'+zipFolder)
    #TODO consider merge fileName, targetPath
    shutil.make_archive(os.path.join(TEMP_PATH,fileName),'zip',zipFolder)
    shutil.move(os.path.join(TEMP_PATH,fileName+'.zip'),os.path.join(targetPath,fileName+'.zip') )

# ok file for process execute successfully    
def createOKFile():    
    filename = os.path.join(FINAL_PATH, PROJECT+'_'+TIMELABEL+'.ok')    
    logging.debug('createOKFile:'+filename)
    open(filename,'a')    

# ok file for process execute successfully    
def createInfoFile():    
    filename = os.path.join(FINAL_PATH, PROJECT+'_'+TIMELABEL+'_info.txt')
    logging.debug('createInfoFile:'+filename)
    file = open(filename,'w')    
    
    result = 'FAIL' if EXIT_CODE != 0 else 'SUCCESS'
    
    # PROJECT,START_TIME,END_TIME,(SUCCESS/FAIL),(NUM/0),ZIP_FILE,FILE_PATH
    content = PROJECT+','+ TIMELABEL +','+datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    content += ',' + result + ',' + str(RESULT_COUNT) 
    content += ',,' if result == 'FAIL' else ',' + PROJECT+'_'+TIMELABEL + '.zip,'
    logging.debug('file content:'+content)
    file.write(content)
    file.close()