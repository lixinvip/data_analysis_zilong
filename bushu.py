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

import time
import pymysql
import csv
reload(sys)
sys.setdefaultencoding('utf-8')


import service_json_A
import csv_import_A


if __name__ == '__main__':
	
	csv_import_A.delete_csv("logout_detail",str(datetime.datetime.now().strftime('%Y-%m-%d')))
	for x in xrange(1,46):
		print((datetime.datetime.now()+datetime.timedelta(days=-x)).strftime('%Y-%m-%d'))


	# csv_import_A.delete_csv("ad_action_v2","")
	# csv_import_A.import_csv("/data1/bidata/1452827692979/","market_newuser_"+str((datetime.datetime.now()+datetime.timedelta(days=-1)).strftime('%Y-%m-%d'))+".csv",'ad_action_v2')
	# csv_import_A.import_csv("/data1/bidata/1479458217005/","market_newuser_"+str((datetime.datetime.now()+datetime.timedelta(days=-1)).strftime('%Y-%m-%d'))+".csv",'ad_action_v2')

			
		csv_import_A.import_csv("/data1/bidata/1452827692979/","market_logout_log_"+str((datetime.datetime.now()+datetime.timedelta(days=-x)).strftime('%Y-%m-%d'))+".csv",'logout_detail')
		csv_import_A.import_csv("/data1/bidata/1479458217005/","market_logout_log_"+str((datetime.datetime.now()+datetime.timedelta(days=-x)).strftime('%Y-%m-%d'))+".csv",'logout_detail_1479458217005')





	print("step3 ok")

