# -*- coding:utf-8 -*-
import psycopg2
import pandas as pd


conn = psycopg2.connect(host = '10.11.122.127', port = 5432, user = 'gpuser', \
                        password = 'gp@qetuo', database = 'shwygpdb')
cursor = conn.cursor()
#输入SQL语句
#sql = 'select * from day_result'
#SQL语句范例：SELECT ... FROM ...WHERE
#sql = "SELECT status,type FROM day_result WHERE date = '2018-02-22'"
#sql = "SELECT date, type, AVG(up_loss_rate), AVG(down_loss_rate) FROM month_result GROUP BY date, type HAVING type = '全网'"
#sql = "SELECT userlabel, pdcpupoctul, pdcpthrptimeulca, pdcpupoctulca, rrcactivemeannbrpcellul, rrcconfigmeannbrpcellul FROM pm_eutrancelltdd_hour WHERE datetime = '2018-05-21 12:00:00'"
#sql = "SELECT datetime ,userlabel, pdcpupoctul, pdcpupoctdl FROM pm_eutrancelltdd_hour WHERE datetime = '2018-06-17 18:00:00' or datetime = '2018-06-17 19:00:00' or datetime = '2018-06-17 20:00:00' or datetime = '2018-06-17 21:00:00' or datetime = '2018-06-17 22:00:00' or datetime = '2018-06-17 23:00:00'"
#sql = "SELECT datetime ,userlabel, pdcpupoctul, pdcpupoctdl FROM pm_eutrancelltdd_hour WHERE datetime = '2018-06-18 00:00:00' or datetime = '2018-06-18 01:00:00' or datetime = '2018-06-18 02:00:00' or datetime = '2018-06-18 03:00:00'"
#sql = "SELECT datetime ,userlabel, pdcpupoctul, pdcpupoctdl FROM pm_eutrancelltdd_hour WHERE datetime = '2018-06-13 18:00:00' or datetime = '2018-06-13 19:00:00' or datetime = '2018-06-13 20:00:00' or datetime = '2018-06-13 21:00:00' or datetime = '2018-06-13 22:00:00' or datetime = '2018-06-13 23:00:00'"
#sql = "SELECT datetime ,userlabel, pdcpupoctul, pdcpupoctdl FROM pm_eutrancelltdd_hour WHERE datetime = '2018-06-14 00:00:00' or datetime = '2018-06-14 01:00:00' or datetime = '2018-06-14 02:00:00' or datetime = '2018-06-14 03:00:00'"
#sql = "SELECT datetime ,userlabel, rrutotalprbusagemeanul, rrutotalprbusagemeandl, rrcsuccconnestab, rrcattconnestab, erabnbrsuccestab, erabnbrattestab FROM pm_eutrancelltdd_hour WHERE datetime = '2018-06-14 03:00:00'"
#sql = "SELECT datetime ,userlabel, pdcpupoctul, pdcpupoctdl FROM pm_eutrancelltdd_day WHERE datetime = '2018-07-22 00:00:00'"
#sql = "SELECT datetime ,userlabel, pdcpupoctul, pdcpupoctdl FROM pm_eutrancelltdd_day WHERE datetime = '2018-07-20 00:00:00'"
#sql = "SELECT dn, userlabel, pdcpupoctul, pdcpupoctdl, rrutotalprbusagemeanul, rrutotalprbusagemeandl FROM pm_eutrancelltdd_day WHERE datetime = '2018-07-17 00:00:00'"
#sql = "SELECT datetime, avg(rrutotalprbusagemeanul) AS avg_rrutotalprbusagemeanul, avg(rrutotalprbusagemeandl) AS avg_rrutotalprbusagemeanul, avg(pdcpupoctul) AS avg_pdcpupoctul, avg(pdcpupoctdl) AS avg_pdcpupoctdl, avg(rrcconnmax) AS avg_rrcconnmax, avg(rrcconnmean) AS avg_rrcconnmean FROM pm_eutrancelltdd_day WHERE datetime BETWEEN '2018-07-01 00:00:00' AND '2018-07-18 00:00:00' GROUP BY datetime"
#sql = "SELECT * FROM resource_eutrancell WHERE date = '2018-07-25'"
#sql = "SELECT n.dn,n.A2ThresholdInterFcov,n.A4ThresholdInterFcov,r.* FROM (SELECT dn,A2ThresholdInterFcov,A4ThresholdInterFcov FROM nrm_eutrancelltdd WHERE date = '2018-07-09') as n,(select * FROM resource_eutrancell WHERE date = '2018-07-09') as r WHERE n.dn = r.dn"
#sql = "SELECT date, dn, userlabel, A2ThresholdInterFcov, A4ThresholdInterFcov FROM nrm_eutrancelltdd WHERE date = '2018-04-09'"
#sql = "SELECT * FROM irms_wireless_antenna WHERE date = '2018-06-25'"
#sql = "SELECT * FROM irms_wireless_antenna WHERE date = '2018-06-26'"
#sql = "SELECT * FROM irms_wireless_antenna WHERE date = '2018-06-27'"
#sql = "SELECT dn, userlabel, qrxlevmin, referencesignalpower FROM nrm_eutrancelltdd WHERE date = '2018-07-25'"
sql = "SELECT n.dn,n.referencesignalpower,r.* FROM (SELECT dn,referencesignalpower FROM nrm_eutrancelltdd WHERE date = '2018-07-26') as n,(select * FROM resource_eutrancell WHERE date = '2018-07-26') as r WHERE n.dn = r.dn"
cursor.execute(sql)
title = [i[0] for i in cursor.description]
data = cursor.fetchall()
df = pd.DataFrame(data, columns = title)
df.to_csv('nrm_eutrancelltdd_resource_eutrancell_0725.csv', index=True, header=True, encoding='gbk')
#print (data)
cursor.close()
conn.close()
