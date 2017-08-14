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
	
	csv_import_A.delete_csv("ad_action_v2","")
	csv_import_A.import_csv("/data1/bidata/1452827692979/","market_newuser_"+str((datetime.datetime.now()+datetime.timedelta(days=-2)).strftime('%Y-%m-%d'))+".csv",'ad_action_v2')
	csv_import_A.import_csv("/data1/bidata/1479458217005/","market_newuser_"+str((datetime.datetime.now()+datetime.timedelta(days=-2)).strftime('%Y-%m-%d'))+".csv",'ad_action_v2')



	csv_import_A.delete_csv("ad_logincount_v2","")
	csv_import_A.import_csv("/data1/bidata/1452827692979/","market_logincount_"+str((datetime.datetime.now()+datetime.timedelta(days=-2)).strftime('%Y-%m-%d'))+".csv",'ad_logincount_v2')
	csv_import_A.import_csv("/data1/bidata/1479458217005/","market_logincount_"+str((datetime.datetime.now()+datetime.timedelta(days=-2)).strftime('%Y-%m-%d'))+".csv",'ad_logincount_v2')

	csv_import_A.delete_csv("ad_re_money_v2","")
	csv_import_A.import_csv("/data1/bidata/1452827692979/","market_ltv_"+str((datetime.datetime.now()+datetime.timedelta(days=-2)).strftime('%Y-%m-%d'))+".csv",'ad_re_money_v2')
	csv_import_A.import_csv("/data1/bidata/1479458217005/","market_ltv_"+str((datetime.datetime.now()+datetime.timedelta(days=-2)).strftime('%Y-%m-%d'))+".csv",'ad_re_money_v2')

	csv_import_A.delete_csv("ad_onlinetime_v2","")
	csv_import_A.import_csv("/data1/bidata/1452827692979/","market_onlinetime_"+str((datetime.datetime.now()+datetime.timedelta(days=-2)).strftime('%Y-%m-%d'))+".csv",'ad_onlinetime_v2')
	csv_import_A.import_csv("/data1/bidata/1479458217005/","market_onlinetime_"+str((datetime.datetime.now()+datetime.timedelta(days=-2)).strftime('%Y-%m-%d'))+".csv",'ad_onlinetime_v2')

	csv_import_A.delete_csv("ad_retention_v2","")
	csv_import_A.import_csv("/data1/bidata/1452827692979/","market_retain_"+str((datetime.datetime.now()+datetime.timedelta(days=-2)).strftime('%Y-%m-%d'))+".csv",'ad_retention_v2')
	csv_import_A.import_csv("/data1/bidata/1479458217005/","market_retain_"+str((datetime.datetime.now()+datetime.timedelta(days=-2)).strftime('%Y-%m-%d'))+".csv",'ad_retention_v2')




	print("step3 ok")

