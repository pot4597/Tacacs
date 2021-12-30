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


file ='arp_summary.csv'

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect("172.21.10.224",22, username='root',password='!Yura@@')


sftp = ssh.open_sftp()
sftp.get('/etc/ansible/yura/cisco/korea/arp/arp_summary.csv','/etc/project/auth_log/arp_summary.csv')
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
#                                print(row[1])
                                fwriter.writerow(row)



conn = pymysql.connect(host='127.0.0.1', user='root', password='Dell@1234',
                       db='device_info', charset='utf8')


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



query1 = 'https://172.25.0.1/api/?type=op&cmd=%3Cshow%3E%3Carp%3E%3Centry+name+%3D+%27all%27%2F%3E%3C%2Farp%3E%3C%2Fshow%3E&key=LUFRPT00TUhRbVVZalZxWndsNy9DL1ViVmUvTVF5RGs9U2tweERLd3ZOd3pUSDM2NStnbWMxNHpKbmsxWnNBb1J5RkwwVTQrVisraz0='

context1 = ssl._create_unverified_context()
req1 = urllib.request.urlopen(query1,context=context1).read().decode("utf-8")

doc1 = ET.fromstring(req1)
elem = doc1.findall('result/entries/entry')



conn = pymysql.connect(host='127.0.0.1', user='root', password='Dell@1234',
                       db='device_info', charset='utf8')
curs = conn.cursor()

device_ip = '172.25.0.1'

for user in elem:
    arp = user.find('ip').text
    mac = user.find('mac').text.replace(':','')
    i += 1
    print(i)
    sql = "insert into arp_temp(arp_ip, arp_mac, arp_switchip,num) values (%s, %s, %s, %s)"
    val = (arp, mac, device_ip, i)
    curs.execute(sql, val)


conn.commit()
conn.close()

query1 = 'https://172.21.200.11/api/?type=op&cmd=%3Cshow%3E%3Carp%3E%3Centry+name+%3D+%27all%27%2F%3E%3C%2Farp%3E%3C%2Fshow%3E&key=LUFRPT00TUhRbVVZalZxWndsNy9DL1ViVmUvTVF5RGs9U2tweERLd3ZOd3pUSDM2NStnbWMxNHpKbmsxWnNBb1J5RkwwVTQrVisraz0='

context1 = ssl._create_unverified_context()
req1 = urllib.request.urlopen(query1,context=context1).read().decode("utf-8")

doc1 = ET.fromstring(req1)
elem = doc1.findall('result/entries/entry')



conn = pymysql.connect(host='127.0.0.1', user='root', password='Dell@1234',
                       db='device_info', charset='utf8')
curs = conn.cursor()

device_ip = '172.21.200.11'

for user in elem:
    arp = user.find('ip').text
    mac = user.find('mac').text.replace(':','')
    i += 1
    print(i)
    sql = "insert into arp_temp(arp_ip, arp_mac, arp_switchip,num) values (%s, %s, %s, %s)"
    val = (arp, mac, device_ip, i)
    curs.execute(sql, val)


conn.commit()
conn.close()



file ='server_arp_summary.csv'

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect("172.21.10.224",22, username='root',password='!Yura@@')


sftp = ssh.open_sftp()
sftp.get('/etc/ansible/yura/cisco/korea/arp/server_arp_summary.csv','/etc/project/auth_log/server_arp_summary.csv')
sftp.close()
ssh.close()

with open('/etc/project/auth_log/server_arp_summary.csv', 'r', newline='') as csv_in_file:
        with open('/etc/project/auth_log/server_arp_sum.csv', 'w', newline='') as csv_out_file:
                freader = csv.reader(csv_in_file)
                fwriter = csv.writer(csv_out_file)
                next(freader)
                reader = csv.reader(csv_in_file, delimiter = ' ')
                for row in reader:
                        if row[0] != '':
                                fwriter.writerow(row)


conn = pymysql.connect(host='127.0.0.1', user='root', password='Dell@1234',
                       db='device_info', charset='utf8')


curs = conn.cursor()

f = open('/etc/project/auth_log/server_arp_sum.csv','r')
csvReader = csv.reader(f)


for rows in csvReader:
        IPADDR = (rows[0])
        MACADDR = (rows[1])
        Device_IP = (rows[2])
        i += 1
        print(i)
        sql = "insert into arp_temp(arp_ip,arp_mac,arp_switchip, num) values (%s, %s, %s, %s)"
        val = (IPADDR, MACADDR, Device_IP, i)
        curs.execute(sql, val)

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
