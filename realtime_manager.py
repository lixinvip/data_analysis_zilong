#!/usr/bin/python
# -*- coding: UTF-8 -*- 



import os
import sys
import time
import csv
import sys
import datetime
import os
import shutil
import re
import mail
import time
import pymysql
import csv
import subprocess
reload(sys)
sys.setdefaultencoding('utf-8')


import service_json_A
import csv_import_A
import anti_spam

if __name__ == '__main__':
	

	cmd = "rsync -vzrtopg --progress --bwlimit=20480  bilog@10.122.67.166::market/bi2market/data/1479458217005/"+str(datetime.datetime.now().strftime('%Y-%m-%d'))+" 1479458217005/  --password-file=bi-service-rsync.pass"
	subprocess.call(cmd, shell=True)
	cmd = "rsync -vzrtopg --progress --bwlimit=20480  bilog@10.122.67.166::market/bi2market/data/1452827692979/"+str(datetime.datetime.now().strftime('%Y-%m-%d'))+" 1452827692979/  --password-file=bi-service-rsync.pass"
	subprocess.call(cmd, shell=True)
	csv_import_A.delete_csv("ad_action_realtime","")
	csv_import_A.import_csv("1479458217005/"+str(datetime.datetime.now().strftime('%Y-%m-%d'))+"/","market_newuser_10min_"+str(datetime.datetime.now().strftime('%Y-%m-%d'))+".csv",'ad_action_realtime')
	csv_import_A.import_csv("1452827692979/"+str(datetime.datetime.now().strftime('%Y-%m-%d'))+"/","market_newuser_10min_"+str(datetime.datetime.now().strftime('%Y-%m-%d'))+".csv",'ad_action_realtime')
