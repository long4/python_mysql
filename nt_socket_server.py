import socket
import pymysql
import sql_process
from sql_process import processSql
from analytic_attri import manual_optimization
from analytic_attri import auto_optimization
import sys
import os
import time
from apscheduler.schedulers.background import BackgroundScheduler
import signal

db_dev_table_list= []
PORT = 9000
sta_mac = []
g_sta_mac = 0
#HOST = sys.argv[1]
HOST = os.popen("ifconfig | grep 'inet ' | grep -v '127.0.0.1' | cut -d: -f2 | awk '{print $1}'").read()
print(HOST)
g_reconnect_value = 0
g_switch_delay_value  = 0
g_database_name = 0
g_optimize_switch_mode_value = 0
g_select_rbt_value = 0

def signal_handler(signalnum, frame):
    print(signalnum)
    if 'one' == g_select_rbt_value:
        for mac in sta_mac:
            delete_sql(HOST,"nt","nufront", mac)
    else:
        delete_ebu_sql(HOST,"nt","nufront", g_select_rbt_value)
    #sys.exit()       
    os._exit(0)   
def create_sql(ipaddr, user, passwd, sta_mac):
    db = pymysql.connect(ipaddr,user,passwd)
    cursor = db.cursor()
    eau_sql = processSql(ipaddr, user, passwd)
    eau_sql.eau_create(db, cursor, sta_mac)
    cursor.close()
    db.close()

def delete_sql(ipaddr, user, passwd, sta_mac):
    db = pymysql.connect(ipaddr,user,passwd)
    cursor = db.cursor()
    eau_sql = processSql(ipaddr, user, passwd)
    eau_sql.delete_sql(db, cursor, sta_mac)
    cursor.close()
    db.close()

def create_ebu_sql(ipaddr, user, passwd, dbname):
    db = pymysql.connect(ipaddr,user,passwd)
    cursor = db.cursor()
    ebu_sql = processSql(ipaddr, user, passwd)
    ebu_sql.ebu_create(db, cursor, dbname)
    cursor.close()
    db.close()

def delete_ebu_sql(ipaddr, user, passwd, dbname):
    db = pymysql.connect(ipaddr,user,passwd)
    cursor = db.cursor()
    ebu_sql = processSql(ipaddr, user, passwd)
    ebu_sql.delete_sql(db, cursor, dbname)
    cursor.close()
    db.close()
def para_table(ipaddr, user, passwd, dbname):
    try:
        db = pymysql.connect(ipaddr,user,passwd, dbname)
        cursor = db.cursor()
        #查询表名
        query = ("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA='%s'" %(dbname))
        cursor.execute(query)
        for (table_name) in cursor:
            #key = table_name[0]
            #if key not in db_dev_table_dict.keys():
            db_dev_table_list.append(table_name[0])
                #db_dev_table_dict[key] = 0
        cursor.close()
        db.close()
    except:
        print("para err!")

def run():
    print(db_dev_table_list)
    new_db_dev_table_list = []
    print(sta_mac)
    for mac in sta_mac:
        for table in db_dev_table_list:
            if table.find(mac) >=0:
                if table not in new_db_dev_table_list:
                    new_db_dev_table_list.append(table)
    print(new_db_dev_table_list)
    if g_database_name:
        if 'manual' == g_optimize_switch_mode_value and 'one' == g_select_rbt_value:
            para_sql.manual_analyze(sta_mac,g_database_name, new_db_dev_table_list, g_select_rbt_value, g_reconnect_value, g_switch_delay_value)
        elif 'manual' == g_optimize_switch_mode_value and 'all' == g_select_rbt_value:
            para_sql.manual_analyze(sta_mac,g_database_name, db_dev_table_list, g_select_rbt_value, g_reconnect_value, g_switch_delay_value)

if __name__ == '__main__':
    # BackgroundScheduler: 适合于要求任何在程序后台运行的情况，当希望调度器在应用后台执行时使用。
    scheduler = BackgroundScheduler()
    # 采用非阻塞的方式
    # 采用固定时间间隔（interval）的方式，每隔3秒钟执行一次
    #scheduler.add_job(run, 'interval', seconds=10)
    # 这是一个独立的线程
    scheduler.add_job(run, 'interval', seconds=60)
    scheduler.start()
    # 其他任务是独立的线程
    #para_table(HOST,"nt","nufront",g_database_name)
    para_sql=manual_optimization(HOST,"nt", "nufront")
    for sig in [signal.SIGINT, signal.SIGTERM]:
        signal.signal(sig, signal_handler)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
        s.bind((HOST, PORT))
        while True:
            while True == s.listen():
                pass
            conn, addr = s.accept()
            with conn:
                print('Connected by', addr)
                while True:
                    data = conn.recv(1024)
                    new_data = data.decode("utf-8").split(',')
                    #print(new_data)
                    for string in new_data:
                        if string.find("sample_dev1_cm_value") >=0:
                            #print(string)
                            string0=string.split('=')
                            if string0[1] not in sta_mac:
                                sta_mac.append(string0[1])
                                #delete_sql(HOST,"nt","nufront", string0[1])
                                create_sql(HOST,"nt","nufront", string0[1])
                        if string.find("sample_dev2_cm_value") >=0:
                            #print(string)
                            string1=string.split('=')
                            if string1[1] not in sta_mac:
                                sta_mac.append(string1[1])
                                create_sql(HOST,"nt","nufront", string1[1])
                        if string.find("remote_database_name") >=0:
                            #print(remote_database_name)
                            string1=string.split('=')
                            g_database_name = string1[1]
                            para_table(HOST,"nt","nufront",string1[1])
                        if string.find("reconnect_value") >=0:
                            string1=string.split('=')
                            g_reconnect_value = string1[1]
                        if string.find("switch_delay_value") >=0:
                            string1=string.split('=')
                            g_switch_delay_value = string1[1]
                        if string.find("optimize_switch_mode_value") >=0:
                            string1=string.split('=')
                            g_optimize_switch_mode_value = string1[1]
                        if string.find("select_rbt_value") >=0:
                            string1=string.split('=')
                            g_select_rbt_value = string1[1]
                            if 'all' == g_select_rbt_value:
                                create_ebu_sql(HOST,"nt","nufront", g_select_rbt_value)
                    #解析命令
                    if not data:
                        conn.close()
                        break
    s.close()
