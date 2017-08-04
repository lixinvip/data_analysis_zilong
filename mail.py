#!/usr/bin/python
# -*- coding: UTF-8 -*- 

import os
import sys
import time
import datetime
import os
import smtplib
from email.mime.text import MIMEText 
reload(sys)
sys.setdefaultencoding('utf-8')


def send_mail(sub,content):

	to_list=["liuhao@zlongame.com","lixin@zlongame.com"] 
	mail_host="smtp.163.com"  #设置服务器
	mail_user="18600547032"    #用户名
	mail_pass="Liuhao85310"   #口令 
	mail_postfix="163.com"  #发件箱的后缀

	me="data report"+"<"+mail_user+"@"+mail_postfix+">"   #这里的hello可以任意设置，收到信后，将按照设置显示
	msg = MIMEText(content.decode('utf-8'),_subtype='html',_charset='gb2312')    #创建一个实例，这里设置为html格式邮件
	msg['Subject'] = sub.decode('utf-8')    #设置主题
	msg['From'] = me  
	msg['To'] = ";".join(to_list)  
	try:  
		s = smtplib.SMTP()  
		s.connect(mail_host)  #连接smtp服务器
		s.login(mail_user,mail_pass)  #登陆服务器
		s.sendmail(me, to_list, msg.as_string())  #发送邮件
		s.close()  
		return True  
	except Exception, e:  
		print str(e)  
		return False 


def create_json(word,name):
	file_object = open('/var/www/'+name+'.json','w')
	print('/var/www/'+name+'.json')
	file_object.write(word)
	file_object.close()
	print("create_json_"+name)

if __name__ == '__main__':  
	    if send_mail("【成功】数据分析源数据入库通知"+str((datetime.datetime.now()).strftime('%Y-%m-%d')),"test"):  
	        print "mail success"  
	        create_json('3','success')
	    else:  
	        print "mail fail" 
	        create_json('22','fail')