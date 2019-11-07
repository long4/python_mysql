import pymysql
class processSql():
    def __init__(self, ipaddr, user, passwd):
        self.ipaddr = ipaddr
        self.user = user
        self.passwd = passwd
    def eau_create(self, db, cursor, dbname):
        print ("start create eau %s" %dbname)
        flag = 0
        try:
            #db = pymysql.connect(self.ipaddr,self.user,self.passwd)
            #cursor = connectinfo.cursor()
            dbname = dbname.strip()
            dbname = 'result_' + dbname
            try:
                cursor.execute("CREATE DATABASE IF NOT EXISTS %s" %dbname)
            except:
                flag = 1
                db.rollback()
                print ("%s DATABASE EXISTS %s" %dbname)

            if 0 == flag:
                cursor.execute("USE %s" %dbname)
                tablename = ['dir_00', 'dir_01', 'dir_10', 'dir_11']
                #for table in tablename:
                   # try:
                      #  cursor.execute("drop table %s" %table)  
                    #except:
                        #db.rollback()
                for table in tablename:
                    sql = "CREATE TABLE IF NOT EXISTS %s" %(table) + "(timestamp BIGINT, cm_sta_dir TEXT, reconnect_flag TEXT, over_hotime TEXT,\
                        non_hopoint TEXT, side_of_hopoint TEXT, \
                        hotime TEXT, before_ta TEXT, before_rssi TEXT, after_ta TEXT, after_rssil TEXT, eau_mac TEXT, mac TEXT, destmac TEXT, chn TEXT,\
                        nb_chn TEXT, nb_tath TEXT, nb_rssith TEXT, new_tath TEXT, new_rssith TEXT,before_recnct_rssi TEXT, before_recnct_ta TEXT, join_flag TEXT, recnct_reason TEXT, opt_method TEXT)"
                    cursor.execute(sql)
            print ("create success %s" %dbname)
        except:
            print ("create fail %s" %dbname)
    def ebu_create(self, db, cursor, dbname):
        print ("start create ebu %s" %(dbname))
        flag = 0
        try:
            #db = pymysql.connect(self.ipaddr,self.user,self.passwd)
            #cursor = db.cursor()
            dbname  = dbname.strip()
            dbname  = 'result_' + dbname
            try:
                cursor.execute("CREATE DATABASE IF NOT EXISTS %s" %dbname)
            except:
                flag = 1
                db.rollback()
                print ("%s DATABASE EXISTS %s" %dbname)
                
            if 0 == flag:
                cursor.execute("USE %s" %dbname)
                tablename = ['dir_00', 'dir_01', 'dir_10', 'dir_11']
                #for table in tablename:
                    #try:
                     #   cursor.execute("drop table %s" %table)  
                   # except:
                      #  db.rollback()
                for table in tablename:
                    sql = "CREATE TABLE IF NOT EXISTS  %s" %(table) + "(mac VARCHAR (10) primary key , before_hopoint_n1 TEXT, after_hopoint_n2 TEXT, not_need_hopoint_n3 TEXT, non_hopoint_n4 TEXT,\
                        adjust_hopoint TEXT)"
                    cursor.execute(sql)
                #db.commit()
                #cursor.close()
                #db.close()
            print ("create success %s" %dbname)
        except:
            print ("create fail %s" %dbname)
    def ebu_insert(self, db, cursor, dbname, tablename, key_values:dict):
        try:
            #db = pymysql.connect(self.ipaddr,self.user,self.passwd, dbname)
            #cursor = db.cursor()
            dbname  = dbname.strip()
            dbname  = 'result_' + dbname
            cursor.execute("USE %s" %dbname)
            sql = "INSERT INTO %s (mac, before_hopoint_n1, after_hopoint_n2, not_need_hopoint_n3, non_hopoint_n4, \
                    adjust_hopoint)" %(tablename) + "VALUES('%s', '%s', '%s', '%s', '%s', '%s')" \
                   %(key_values['mac'], key_values['before_hopoint_n1'], key_values['after_hopoint_n2'], key_values['not_need_hopoint_n3'],\
                    key_values['non_hopoint_n4'], key_values['adjust_hopoint'])
            cursor.execute(sql)
            db.commit()
            #cursor.close()
            #db.close()
            print ("%s insert %s success to MySQL" %(dbname,tablename))
        except:
            print ("%s insert %s fail to MySQL" %(dbname,tablename))
    def eau_insert(self, db, cursor, dbname, tablename, key_values:dict):
        try:
            #db = pymysql.connect(self.ipaddr,self.user,self.passwd)
            #cursor = db.cursor()
            dbname  = dbname.strip()
            dbname  = 'result_' + dbname
            #print(key_values)
            try:
                cursor.execute("USE %s" %dbname)
            except:
                print("%s use fail to MySQL" %(dbname))
            try:
                sql = "INSERT INTO %s (timestamp, cm_sta_dir, reconnect_flag, over_hotime ,\
                        non_hopoint , side_of_hopoint, \
                        hotime , before_ta , before_rssi, after_ta , after_rssil , eau_mac, mac , destmac , chn ,\
                        nb_chn , nb_tath , nb_rssith ,new_tath , new_rssith , before_recnct_rssi, before_recnct_ta, join_flag, recnct_reason , opt_method)" %(tablename) + "VALUES('%d', '%s', '%s', '%s', '%s', '%s', '%s', '%s', \
                        '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s','%s', '%s', '%s', '%s')" \
                        %(key_values['timestamp'], key_values['cm_sta_dir'], key_values['reconnect_flag'], key_values['over_hotime'], key_values['non_hopoint'],\
                          key_values['side_of_hopoint'], key_values['hotime'], key_values['before_ta'], key_values['before_rssi'],\
                          key_values['after_ta'], key_values['after_rssil'], key_values['eau_mac'], key_values['mac'], key_values['destmac'],\
                          key_values['chn'], key_values['nb_chn'], key_values['nb_tath'], key_values['nb_rssith'],key_values['new_tath'], key_values['new_rssith'],\
                          key_values['before_recnct_rssi'], key_values['before_recnct_ta'], key_values['join_flag'], key_values['recnct_reason'], key_values['opt_method'])
                cursor.execute(sql)
            except:
                print("%s insert %s fail" %(dbname,tablename))

            db.commit()
            #cursor.close()
            #db.close()
            #print ("%s insert %s success to MySQL" %(dbname,tablename))
        except:
            print ("%s insert %s fail to MySQL" %(dbname,tablename))
    def eau_update(self, db, cursor, dbname, tablename, key_values:dict):
        try:
            db = pymysql.connect(self.ipaddr,self.user,self.passwd, dbname)
            cursor = db.cursor()
            '''
            sql = "UPDATE %s SET (reconnect_flag, over_hotime ,\
                    non_hopoint , side_of_hopoint, \
                    hotime , before_ta , before_rssi, after_ta , after_rssil , mac , destmac , chn ,\
                    nb_chn , nb_tath , nb_rssith ) WHERE timestamp = 1" %(tablename) + "VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', \
                '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" \
                   %(key_values['reconnect_flag'], key_values['over_hotime'], key_values['non_hopoint'],\
                    key_values['side_of_hopoint'], key_values['hotime'], key_values['before_ta'], key_values['before_rssi'],\
                    key_values['after_ta'], key_values['after_rssil'], key_values['mac'], key_values['destmac'],\
                    key_values['chn'], key_values['nb_chn'], key_values['nb_tath'], key_values['nb_rssith'])'''
            sql="UPDATE %s SET stadir = 1 ,cm_dir = 1 WHERE id < 5" %(tablename)
            cursor.execute(sql)
            db.commit()
            cursor.close()
            db.close()
            print ("%s update %s success connecting to MySQL" %(dbname,tablename))
        except:
            print ("%s update %s fail connecting to MySQL" %(dbname,tablename))
    def ebu_update_adjust_hopoint(self, db, cursor, dbname):
        dbname  = 'result_' + dbname
        adjust_hopoint = '0'
        cursor.execute("USE %s" %dbname)
        tablename = ['dir_00', 'dir_01', 'dir_10', 'dir_11']
        for table in tablename:
            sql = "SELECT * FROM %s" %(table)
            try:
                cursor.execute(sql)
                results = cursor.fetchall()
                for row in results:
                    if row[0]:
                        before_hopoint_n1 = int(row[1])
                        after_hopoint_n2 = int(row[2])
                        not_need_hopoint_n3 = int(row[3])
                        #non_hopoint_n4 = int(row[4])
                        if (before_hopoint_n1 + after_hopoint_n2 + not_need_hopoint_n3) == 0:
                            adjust_hopoint = '--'
                        elif (before_hopoint_n1 - after_hopoint_n2 - not_need_hopoint_n3)/(before_hopoint_n1 + after_hopoint_n2 + not_need_hopoint_n3) >0.5:
                            adjust_hopoint = '0'
                        elif (after_hopoint_n2 - before_hopoint_n1 - not_need_hopoint_n3)/(before_hopoint_n1 + after_hopoint_n2 + not_need_hopoint_n3) >0.5:
                            adjust_hopoint = '1'
                        else:
                            adjust_hopoint = '--'
                        
                        #cursor.execute("USE %s" %dbname)
                        sql="UPDATE %s SET adjust_hopoint = '%s' WHERE mac = '%s'" %(table, adjust_hopoint, row[0])
                        cursor.execute(sql)
                        #db.commit()
            
            except Exception as ex:
                print("出现如下异常%s"%ex)
            #except:
                print ("%s ebu_update_adjust_hopoint %s fail connecting to MySQL" %(dbname,tablename))
        db.commit()
    def ebu_update(self, db, cursor, dbname, tablename, key_values:dict):
        try:
            #db = pymysql.connect(self.ipaddr,self.user,self.passwd, dbname)
            #cursor = db.cursor()

            dbname  = 'result_' + dbname
            try:
                cursor.execute("USE %s" %dbname)
            except:
                print("%s use fail to MySQL" %(dbname))

            sql="UPDATE %s SET before_hopoint_n1 = '%s', after_hopoint_n2 = '%s' ,not_need_hopoint_n3 = '%s' ,non_hopoint_n4 = '%s' ,adjust_hopoint = '%s' WHERE mac = '%s'" %(tablename, key_values['before_hopoint_n1'], \
                key_values['after_hopoint_n2'], key_values['not_need_hopoint_n3'], key_values['non_hopoint_n4'], key_values['adjust_hopoint'],key_values['mac'])
            cursor.execute(sql)
            db.commit()

            #cursor.close()
           # db.close()
            print ("%s update %s success connecting to MySQL" %(dbname,tablename))
        except:
            print ("%s update %s fail connecting to MySQL" %(dbname,tablename))
    def delete_sql(self, db, cursor, dbname):
        #db = pymysql.connect(self.ipaddr,self.user,self.passwd, dbname)
        #cursor = db.cursor()
        dbname = dbname.strip()
        dbname = 'result_' + dbname
        sql = "DROP DATABASE IF EXISTS %s" %(dbname)
        try:
            cursor.execute(sql)
            db.commit()
        except:
            db.rollback()

        print ("%s  success delete" %(dbname))
    def sql_query(self, db, cursor, dbname, tablename, key_values:dict):
        before_hopoint_n1 = 0
        after_hopoint_n2 = 0
        not_need_hopoint_n3 = 0
        non_hopoint_n4 = 0
        flag = 0
        #db = pymysql.connect(self.ipaddr,self.user,self.passwd, dbname)
        #cursor = db.cursor()
        dbname_s  = 'result_' + dbname
        cursor.execute("USE %s" %dbname_s)
        sql = "SELECT * FROM %s" %(tablename)
        try:
            cursor.execute(sql)
            results = cursor.fetchall()
            for row in results:
                if row[0] and  key_values['mac'] == row[0]:
                    before_hopoint_n1 = int(row[1])
                    after_hopoint_n2 = int(row[2])
                    not_need_hopoint_n3 = int(row[3])
                    non_hopoint_n4 = int(row[4])
                    flag = 1
                    break
        except:
              print ("%s query %s fail connecting to MySQL" %(dbname,tablename))
        #cursor.close()
        #db.close()
        print ("%s query %s success connecting to MySQL" %(dbname,tablename))
        if 1 == flag:
            key_values['before_hopoint_n1'] = str(int(key_values['before_hopoint_n1']) + before_hopoint_n1)
            key_values['after_hopoint_n2'] = str(int(key_values['after_hopoint_n2']) + after_hopoint_n2)
            key_values['not_need_hopoint_n3'] = str(int(key_values['not_need_hopoint_n3']) + not_need_hopoint_n3)
            key_values['non_hopoint_n4'] = str(int(key_values['non_hopoint_n4']) + non_hopoint_n4)
            self.ebu_update(db, cursor, dbname, tablename, key_values)
        else:
            self.ebu_insert(db, cursor, dbname, tablename, key_values)

    def para_table(self, dbname, db_dev_table_list):
        #db_dev_table_list = []
        try:
            db = pymysql.connect(self.ipaddr,self.user,self.passwd, dbname)
            cursor = db.cursor()
            #查询表名
            query = ("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA='%s'" %(dbname))
            cursor.execute(query)
            for (table_name) in cursor:
                db_dev_table_list.append(table_name[0])
            print(db_dev_table_list)
            cursor.close()
            db.close()
        except:
            print("para err!")