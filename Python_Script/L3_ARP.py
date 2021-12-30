import csv
import scipy
from paramiko import SSHClient
from scp import SCPClient
import os
import paramiko
import pymysql
import ssl
import urllib.request
import xml.etree.ElementTree as ET
import sys
from urllib import parse

###Cisco L3 Arp Getting###
file ='arp_summary.csv'

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect("***",**, username='***',password='***')


sftp = ssh.open_sftp()
sftp.get('/etc/ansible/cisco/korea/arp/arp_summary.csv','/etc/project/auth_log/arp_summary.csv')
sftp.close()
ssh.close()

with open('/etc/project/auth_log/arp_summary.csv', 'r', newline='') as csv_in_file:
        with open('/etc/project/auth_log/arp_sum.csv', 'w', newline='') as csv_out_file:
                freader = csv.reader(csv_in_file)
                fwriter = csv.writer(csv_out_file)
                next(freader)
                reader = csv.reader(csv_in_file, delimiter = ' ')
                for row in reader:
                        if row[0] != '':
                                fwriter.writerow(row)


##로컬 DB연결##
conn = pymysql.connect(host='127.0.0.1', user='***', password='***',db='***', charset='utf8')
curs = conn.cursor()

f = open('/etc/project/auth_log/arp_sum.csv','r')
csvReader = csv.reader(f)
curs.execute('delete from arp_temp')

i = 0
for rows in csvReader:
        IPADDR = (rows[0])
        MACADDR = (rows[1])
        Device_IP = (rows[2])
        print(rows[0])
        i += 1
        print(i)
        sql = "insert into arp_temp(arp_ip,arp_mac,arp_switchip,num) values (%s, %s, %s, %s)"
        val = (IPADDR, MACADDR, Device_IP, i)
        curs.execute(sql, val)
curs.execute("update arp_temp set arp_mac = replace(arp_mac, '.', '')")

conn.commit()
f.close()
conn.close()


curs.execute("update arp_temp set arp_mac = replace(arp_mac, '.', '')")
curs.execute('insert into arp (arp_ip, arp_mac, arp_switchip) select arp_ip, arp_mac, arp_switchip from arp_temp as t where t.arp_mac not in (select arp_mac from arp)')
curs.execute('update arp as a , arp_temp as t set a.arp_ip = t.arp_ip where a.arp_mac = t.arp_mac and a.arp_ip != t.arp_ip')
curs.execute('delete n1 from arp n1, arp n2 where n1.num < n2.num and n1.arp_mac = n2.arp_mac')
curs.execute('alter table arp auto_increment=1')
curs.execute('set @count = 0')
curs.execute('update arp set num = @count:=@count+1')


conn.commit()
f.close()
conn.close()
