#!/usr/bin/python
# -*- coding: UTF-8 -*- 

import os
import sys
import time
import datetime
import os
import smtplib
from email.mime.text import MIMEText 
import pymysql
from Config import Config
reload(sys)
sys.setdefaultencoding('utf-8')

global conn
conn = pymysql.connect(host=Config.mysql_conf['host'],port=Config.mysql_conf['port'],user=Config.mysql_conf['user'],password=Config.mysql_conf['password'],database=Config.mysql_conf['dbName'],charset=Config.mysql_conf['charset'])

def send_mail(sub,content):

	to_list=["58254451@qq.com","397211359@qq.com"] 
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


def anti_spam():
	global conn
	cursor=conn.cursor()
	sql='select game_channel,agent,date,spam_1 from `anti_spam_all` where date = left(date_sub(curdate(),interval 1 day),10) and (spam_1<>0 or spam_11<>0)'
	cursor.execute(sql)
	mail_str="""


<table cellpadding="0" style="border-collapse:collapse;border-color:#666666;border-width:1.0px;border-style:solid;width:364.0px;">
 <colgroup><col style="width:111.0px;"> <col style="width:72.0px;"> <col span="2" style="width:83.0px;"> </colgroup>
<tbody>
<tr height="24"> 
 <td class="xl67 " valign="middle" style="padding-top:1.0px;padding-right:1.0px;padding-left:1.0px;color:#ffffff;font-size:16.0px;font-weight:bold;font-style:normal;text-decoration:none solid #ffffff;font-family:微软雅黑,sans-serif;border:1.0px solid #666666;background-image:none;background-position:.0% .0%;background-size:auto;background-attachment:scroll;background-origin:padding-box;background-clip:border-box;background-color:#000000;">渠道</td>
  <td class="xl67 " valign="middle" style="padding-top:1.0px;padding-right:1.0px;padding-left:1.0px;color:#ffffff;font-size:16.0px;font-weight:bold;font-style:normal;text-decoration:none solid #ffffff;font-family:微软雅黑,sans-serif;border:1.0px solid #666666;background-image:none;background-position:.0% .0%;background-size:auto;background-attachment:scroll;background-origin:padding-box;background-clip:border-box;background-color:#000000;">代理商</td>
  <td class="xl67 " valign="middle" style="padding-top:1.0px;padding-right:1.0px;padding-left:1.0px;color:#ffffff;font-size:16.0px;font-weight:bold;font-style:normal;text-decoration:none solid #ffffff;font-family:微软雅黑,sans-serif;border:1.0px solid #666666;background-image:none;background-position:.0% .0%;background-size:auto;background-attachment:scroll;background-origin:padding-box;background-clip:border-box;background-color:#000000;">投放日期</td> 
 <td class="xl67 " valign="middle" style="padding-top:1.0px;padding-right:1.0px;padding-left:1.0px;color:#ffffff;font-size:16.0px;font-weight:bold;font-style:normal;text-decoration:none solid #ffffff;font-family:微软雅黑,sans-serif;border:1.0px solid #666666;background-image:none;background-position:.0% .0%;background-size:auto;background-attachment:scroll;background-origin:padding-box;background-clip:border-box;background-color:#000000;">作弊行为1</td>
 </tr>

	"""	
	rs=cursor.fetchall()

	for r in rs:
		mail_str+=""" <tr height="22"> 
		<td class="xl65 "  valign="middle"style="padding-top:1.0px;padding-right:1.0px;padding-left:1.0px;color:#000000;font-size:13.3px;font-weight:normal;font-style:normal;text-decoration:none solid #000000;font-family:微软雅黑,sans-serif;border:1.0px solid #666666;background-image:none;background-position:.0% .0%;background-size:auto;background-attachment:scroll;background-origin:padding-box;background-clip:border-box;">
		""" +str(r[0])+"""
</td> 
 <td class="xl65 " valign="middle"  style="padding-top:1.0px;padding-right:1.0px;padding-left:1.0px;color:#000000;font-size:13.3px;font-weight:normal;font-style:normal;text-decoration:none solid #000000;font-family:微软雅黑,sans-serif;border:1.0px solid #666666;background-image:none;background-position:.0% .0%;background-size:auto;background-attachment:scroll;background-origin:padding-box;background-clip:border-box;">
        """+str(r[1])+"""
</td> 
<td class="xl65 " valign="middle"  style="padding-top:1.0px;padding-right:1.0px;padding-left:1.0px;color:#000000;font-size:13.3px;font-weight:normal;font-style:normal;text-decoration:none solid #000000;font-family:微软雅黑,sans-serif;border:1.0px solid #666666;background-image:none;background-position:.0% .0%;background-size:auto;background-attachment:scroll;background-origin:padding-box;background-clip:border-box;">
 
        """+str(r[2])+"""

</td> 
<td class="xl65 " valign="middle"  style="padding-top:1.0px;padding-right:1.0px;padding-left:1.0px;color:#000000;font-size:13.3px;font-weight:normal;font-style:normal;text-decoration:none solid #000000;font-family:微软雅黑,sans-serif;border:1.0px solid #666666;background-image:none;background-position:.0% .0%;background-size:auto;background-attachment:scroll;background-origin:padding-box;background-clip:border-box;">
 
        """+str(r[3])+"</td></tr>"
	mail_str+="</tbody></table>"

	cursor.close()
	conn.close()
	if len(rs)>0:
		send_mail("[反作弊日常报告]存在异常账号-"+str((datetime.datetime.now()).strftime('%Y-%m-%d')),mail_str)
	else 
		send_mail("[反作弊日常报告]今日一切正常-"+str((datetime.datetime.now()).strftime('%Y-%m-%d')),"")
if __name__ == '__main__':  
	anti_spam()