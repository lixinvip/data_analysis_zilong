#!/usr/bin/python
# -*- coding: UTF-8 -*- 



import pymysql
import os
import sys
import time
import csv
import sys
import datetime
from Config import Config
reload(sys)
sys.setdefaultencoding('utf-8')

global conn
conn = pymysql.connect(host=Config.mysql_conf['host'],port=Config.mysql_conf['port'],user=Config.mysql_conf['user'],password=Config.mysql_conf['password'],database=Config.mysql_conf['dbName'],charset=Config.mysql_conf['charset'])


def anti_spam_1():
	global conn
	cur_1=conn.cursor()
	sql="""
TRUNCATE anti_spam_1_v0;
INSERT INTO  anti_spam_1_v0 SELECT  game_id,platform ,DATE,game_channel,agent ,SUM(total) AS total ,SUM(less30s) AS less30s,SUM(more10h) AS more10h 
FROM 
(SELECT f.game_id,f.ad_id,f.platform,f.DATE,f.game_channel,f.agent,IFNULL(e.total,0) AS total, IFNULL(e.less30s,0) AS less30s,IFNULL(e.more10h,0) AS more10h  
FROM 
(SELECT game_id,ad_id,platform,DATE,game_channel,agent
FROM `ad_action_v2`  

GROUP BY game_id,ad_id,platform,DATE,game_channel,agent
) f
LEFT JOIN 
(SELECT c.*,IFNULL(d.more10h,0) AS more10h  
FROM
(SELECT a.* ,IFNULL(b.less30s,0) AS less30s
FROM
(SELECT   ad_id,clickday,COUNT(*) AS total
FROM 
(SELECT  ad_id,LEFT(ad_click_time,10) AS clickday ,UNIX_TIMESTAMP(ad_action_time)-UNIX_TIMESTAMP(ad_click_time) AS TIME ,
idfa,imei,mac,androidid
FROM  
(SELECT ad_id, STR_TO_DATE(ad_click_time,'%Y-%m-%d %H:%i:%s') AS ad_click_time,STR_TO_DATE(ad_action_time,'%Y-%m-%d %H:%i:%s') AS ad_action_time,
idfa,imei,mac,androidid
FROM `ad_action_idfa`

) action_idfa_detail 
) sumnumber
GROUP BY ad_id,clickday
) a 
LEFT JOIN 
(SELECT   ad_id,clickday,SUM(VALUE) AS less30s
FROM 
(SELECT ad_id,clickday,timegroup,COUNT(*) AS VALUE  
FROM 
(SELECT * ,
CASE WHEN TIME<=10 THEN 1 
WHEN TIME>10  AND TIME<=30 THEN 2
WHEN TIME>30  AND TIME<=60 THEN 3
WHEN TIME>18000 AND TIME<=36000 THEN 11
WHEN TIME>36000 AND TIME<=54000 THEN 12
WHEN TIME>54000 AND TIME<=72000 THEN 13
WHEN TIME>72000 AND TIME<=86400 THEN 14
  END  AS  timegroup 
FROM  
(SELECT  ad_id,LEFT(ad_click_time,10) AS clickday ,UNIX_TIMESTAMP(ad_action_time)-UNIX_TIMESTAMP(ad_click_time) AS TIME ,
idfa,imei,mac,androidid
FROM  
(SELECT ad_id, STR_TO_DATE(ad_click_time,'%Y-%m-%d %H:%i:%s') AS ad_click_time,STR_TO_DATE(ad_action_time,'%Y-%m-%d %H:%i:%s') AS ad_action_time,
idfa,imei,mac,androidid
FROM `ad_action_idfa`

) action_idfa_detail 
) action_idfa_time ) action_idfa_timegroup 
GROUP BY ad_id,clickday,timegroup ) tempactiontime
WHERE timegroup IN (1,2)
GROUP BY ad_id,clickday 
) b
ON a.clickday = b.clickday AND a.ad_id=b.ad_id
) c 
LEFT JOIN 
(SELECT   ad_id,clickday ,SUM(VALUE) AS more10h
FROM 
(SELECT ad_id,clickday,timegroup,COUNT(*) AS VALUE  FROM 
(SELECT * ,
CASE WHEN TIME<=10 THEN 1 
WHEN TIME>10  AND TIME<=30 THEN 2
WHEN TIME>30  AND TIME<=60 THEN 3

WHEN TIME>18000 AND TIME<=36000 THEN 11
WHEN TIME>36000 AND TIME<=54000 THEN 12
WHEN TIME>54000 AND TIME<=72000 THEN 13
WHEN TIME>72000 AND TIME<=86400 THEN 14
  END  AS  timegroup 
FROM  
(SELECT  ad_id,LEFT(ad_click_time,10) AS clickday,UNIX_TIMESTAMP(ad_action_time)-UNIX_TIMESTAMP(ad_click_time) AS TIME ,
idfa,imei,mac,androidid
FROM  
(SELECT ad_id, STR_TO_DATE(ad_click_time,'%Y-%m-%d %H:%i:%s') AS ad_click_time,STR_TO_DATE(ad_action_time,'%Y-%m-%d %H:%i:%s') AS ad_action_time,
idfa,imei,mac,androidid
FROM `ad_action_idfa`

) action_idfa_detail
) action_idfa_time ) action_idfa_timegroup 
GROUP BY ad_id,clickday,timegroup ) tempactiontime
WHERE timegroup IN (12,13,14)
GROUP BY ad_id,clickday	
) d
ON c.clickday = d.clickday AND c.ad_id=d.ad_id
) e
ON f.date =e.clickday  AND f.ad_id=e.ad_id
) g
GROUP BY platform ,DATE,game_channel,agent 
ORDER BY less30s DESC,more10h DESC



"""


	cur_1.execute(sql)

	conn.commit()	

	sql='UPDATE anti_spam_all a,(SELECT * FROM `anti_spam_1_v0` where date=left(date_sub(curdate(),interval 1 day),10) ) b  SET a.spam_1=(CASE WHEN (b.less30s+b.more10h)/b.total>0.2 AND total>50 THEN 1 WHEN (b.less30s+b.more10h)/b.total>0.4 AND total>50 THEN 2  ELSE 0 END )  WHERE a.game_channel=b.game_channel AND a.agent=b.agent AND a.date=b.date'

	cur_1.execute(sql)
	cur_1.close()
	conn.commit()		

def anti_spam_11():
	global conn
	cur_1=conn.cursor()
	# 用11作为最初维度，假设它是最全的
	sql="""



TRUNCATE anti_spam_11_v0;
insert into `anti_spam_11_v0` select a.*,b.c from 

(select game_id,platform,game_channel,agent,device_type,left(ad_action_time,10) as date,count(*) as c from `ad_action_idfa`  group by game_id,platform,game_channel,agent,device_type,LEFT(ad_action_time,10)) a 
,
(select game_id,platform,game_channel,agent ,left(ad_action_time,10) as date,count(*) as c  from `ad_action_idfa`  group by game_id,platform,game_channel,agent,LEFT(ad_action_time,10)) b
where a.game_channel=b.game_channel and a.agent=b.agent and a.date=b.date and a.game_id=b.game_id and a.platform=b.platform
and b.c>10;

"""


	cur_1.execute(sql)

	conn.commit()	

	sql="""

UPDATE anti_spam_all a,(
select game_id,platform,game_channel,agent,date,max(T) as spam_11 from (
 select game_id,platform,game_channel,agent,date,case when device_count/device_all_count>0.8 then 2 when device_count/device_all_count>0.5 then 1 else 0 end as T from anti_spam_11_v0 where date=left(date_sub(curdate(),interval 1 day),10)
 ) a group by game_id,platform,game_channel,agent,date ) b 
  set a.spam_11=b.spam_11 
  
  where a.game_channel=b.game_channel AND a.agent=b.agent AND a.date=b.date and a.game_id=b.game_id and a.`platform`=b.platform
 
 
 """
	cur_1.execute(sql)
	cur_1.close()
	conn.commit()		

def anti_spam_init():
	global conn
	cur_1=conn.cursor()
	# 用11作为最初维度，假设它是最全的
	sql="""



delete from `anti_spam_all` where date = left(date_sub(curdate(),interval 1 day),10);
insert into `anti_spam_all` (game_id,platform,DATE,game_channel,agent) select game_id,platform,DATE,game_channel,agent  from `ad_action_v2` where date=left(date_sub(curdate(),interval 1 day),10) group by game_id,platform,DATE,game_channel,agent
"""


	cur_1.execute(sql)
	cur_1.close()
	conn.commit()	

 	
if __name__ == '__main__':
	anti_spam_11()
