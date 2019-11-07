from sql_process import processSql
import pymysql
import schedule
import time
eau_key_values = {'timestamp':'0','cm_sta_dir':'0','reconnect_flag':'0', 'over_hotime':'0', 'non_hopoint':'0', 'side_of_hopoint':'0',\
'hotime':'0', 'before_ta':'0', 'before_rssi':'0', 'after_ta':'0',\
'after_rssil':'0', 'eau_mac':'0','mac':'0', 'destmac':'0', 'chn':'0',\
'nb_chn':'0', 'nb_tath':'0', 'nb_rssith':'0','new_tath':'0', 'new_rssith':'0',\
'before_recnct_rssi':'0', 'before_recnct_ta':'0','join_flag':'0', 'recnct_reason':'0', 'opt_method':'0'}
ebu_key_values = {'mac':'0', 'before_hopoint_n1':'0', 'after_hopoint_n2':'0', 'not_need_hopoint_n3':'0', 'non_hopoint_n4':'0', 'adjust_hopoint':'/'}
db_dev_table_dict = {}
db_dev_table_dict_all = {}
prev_cur_ta = 0
g_hopoint = 0
g_flag_update = 0
class auto_optimization():
    def __init__(self, ipaddr, user, passwd):
        self.ipaddr = ipaddr
        self.user = user
        self.passwd= passwd

    def auto_analyze(self, sta_mac:[], dbname, tablename):
        #optiz_obj= manual_optimization(self.ipaddr,self.user, self.passwd)
        #optiz_obj.process_sql(sta_mac,dbname, tablename)
        pass
class manual_optimization():
    def __init__(self, ipaddr, user, passwd):
        self.ipaddr = ipaddr
        self.user = user
        self.passwd= passwd
        self.db = pymysql.connect(self.ipaddr,self.user,self.passwd)
        self.cursor = self.db.cursor()
        
    def manual_analyze(self, sta_mac:[], dbname, tablename, select_rbt_value, reconnect_value, switch_delay_value):
        self.process_sql(sta_mac,dbname, tablename, select_rbt_value, reconnect_value, switch_delay_value)

    def record_attri_to_opt(self,reconnect, prev_recnid, cur_recnid, db, cursor, sta_mac:[], dbname, tablename, delta):
        print('record reconnect attri, %s, %s' %(prev_recnid, cur_recnid))
        nb_chn = 0
        nb_mac = 0
        nb_rssith = 0
        join_id = 0
        cm_id = 0
        time_stamp2 = 0
        new_mac = 0
        '''
        cursor.execute("USE %s" %dbname)
        sql = "SELECT * FROM  %s WHERE id < %s and id > %s order by id desc" %(tablename, cur_recnid, prev_recnid)
        cursor.execute(sql)
        results = cursor.fetchall()
        for row in results:
            #print('record join cap')
            STA_JOIN_CAP_chn = row[31]
            #print('STA_JOIN_CAP_chn, %s' %STA_JOIN_CAP_chn)

            if STA_JOIN_CAP_chn:
                #time_stamp2 = row[2]
                #new_mac = row[4]
                join_id = row[0]
            CM_START = row[27]
            if CM_START:
                cm_id = row[0]

        if int(join_id) > int(cm_id):
            eau_key_values['join_flag'] = '1'
        '''
        sql = "SELECT * FROM  %s WHERE id < %s and id > %s order by id desc" %(tablename, cur_recnid, prev_recnid)
        cursor.execute("USE %s" %dbname)
        cursor.execute(sql)
        src_mac = 0
        results = cursor.fetchall()
        first_count = 0
        last_count = 0
        first_cur_ta = 0
        last_cur_ta = 0
        for row in results:
            CM_START = row[27]
            if row[6] and 0 == first_count:
                first_cur_ta = int(row[6])
                first_count = 1

            if CM_START and last_count == 0:
                last_count = 1

            if row[6] and 1 == last_count:
                last_cur_ta = int(row[6])
                break
        if first_cur_ta > 0 and last_cur_ta > 0:
            if delta == 16:
                if abs(first_cur_ta - last_cur_ta) < 60:
                    eau_key_values['non_hopoint'] = '1'
            else:
                if abs(first_cur_ta - last_cur_ta) < 30:
                    eau_key_values['non_hopoint'] = '1'

        sql = "SELECT * FROM  %s WHERE id < %s order by id desc" %(tablename, cur_recnid)
        cursor.execute("USE %s" %dbname)
        cursor.execute(sql)
        results = cursor.fetchall()
        time_stamp3 = 0
        for row in results:
            CM_START = row[27]
            drift_ho = row[30]
            if CM_START: 
                time_stamp3 = row[2]
                #cm_id = row[0]
                #print(int(time_stamp3))
                #print(int(eau_key_values['timestamp']))
                if eau_key_values['non_hopoint'] == '1':
                    if int(time_stamp3) >= int(eau_key_values['timestamp']) - 20000:

                        reconnect_reason =['3','5','6','8','12','15','16','7','9','10','14']
                        if reconnect in reconnect_reason:
                            eau_key_values['side_of_hopoint'] = '1'
                    else:
                        reconnect_reason =['7','9','10','14']
                        if reconnect in reconnect_reason:
                            eau_key_values['side_of_hopoint'] = '0'
                    
                    if drift_ho and '1' == drift_ho:
                        print('drift_ho == 1')
                        eau_key_values['side_of_hopoint'] = '0'
                break

        sql = "SELECT * FROM  %s WHERE id < %s and id > %s order by id desc" %(tablename, cur_recnid, prev_recnid)
        cursor.execute("USE %s" %dbname)
        cursor.execute(sql)
        results = cursor.fetchall()
        for row in results:
            if row[40]:
                th_id = row[0]
                eau_key_values['nb_tath'] = row[40]
                eau_key_values['nb_chn'] = row[38]
                eau_key_values['destmac'] = row[39]
                eau_key_values['nb_rssith'] = row[41]
                break
        '''
        if eau_key_values['join_flag'] =='1':
            sql = "SELECT * FROM  %s WHERE id < %s and id > %s order by id desc" %(tablename, join_id, prev_recnid)
        else:
            sql = "SELECT * FROM  %s WHERE id < %s and id > %s order by id desc" %(tablename, cur_recnid, prev_recnid)
        cursor.execute("USE %s" %dbname)
        cursor.execute(sql)
        bak_mac = 0
        count = 0
        results = cursor.fetchall()
        for row in results:
            #print(row[4])
            mac = row[4]
            prssi = row[9]
            if prssi and mac:
                if count >= 3:
                    eau_key_values['mac'] = mac
                    break
                if bak_mac and bak_mac == mac:
                    count = count + 1
                bak_mac = mac
                eau_key_values['before_recnct_rssi'] = prssi
                eau_key_values['before_recnct_ta'] = row[6]
        '''
        cm_dir_last = -1
        sta_dir_last = -1
        sql = "SELECT * FROM  %s WHERE id < %s and id > %s order by id desc" %(tablename, cur_recnid, prev_recnid)
        cursor.execute("USE %s" %dbname)
        cursor.execute(sql)
        results = cursor.fetchall()
        for row in results:
            cm_dir = row[23]
            if cm_dir:
                cm_dir_last = cm_dir[-1]
                break
        
        sql = "SELECT * FROM  %s WHERE id < %s and id > %s order by id desc" %(tablename, cur_recnid, prev_recnid)
        cursor.execute("USE %s" %dbname)
        cursor.execute(sql)
        results = cursor.fetchall()
        for row in results:
            stadir = row[37]
            if stadir:
                sta_dir_last = stadir[-1]
                break

        if int(cm_dir_last) > -1 and int(sta_dir_last) > -1:
            eau_key_values['cm_sta_dir'] = cm_dir_last + sta_dir_last

        cur_ta_list = []
        src_mac = 0
        sql = "SELECT * FROM  %s WHERE id < %s and id > %s order by id desc" %(tablename, cur_recnid, prev_recnid)
        cursor.execute("USE %s" %dbname)
        cursor.execute(sql)
        results = cursor.fetchall()
        for row in results:
            if row[4]:
                if src_mac != 0 and src_mac != row[4]:
                    break
                src_mac = row[4]
            if row[6]:
                cur_ta_list.append(int(row[6]))

        sql = "SELECT * FROM  %s WHERE id < %s and id > %s order by id desc" %(tablename, cur_recnid, prev_recnid)
        cursor.execute("USE %s" %dbname)
        cursor.execute(sql)
        results = cursor.fetchall()
        for row in results:
            if row[26] in sta_mac:
                eau_key_values['eau_mac'] = row[26]
                break
            
        '''insert db'''
        if (eau_key_values['timestamp']) > 0:
            result = ['00', '01', '10', '11']
            if eau_key_values['cm_sta_dir'] in result and eau_key_values['mac'] != '0':
                traindir = 'dir_' + eau_key_values['cm_sta_dir']
                print('traindir, %s' %traindir)
                print('staMac, %s' %eau_key_values['eau_mac'])
                if eau_key_values['non_hopoint'] == '1':
                    if eau_key_values['cm_sta_dir'] == '01' or eau_key_values['cm_sta_dir'] == '10':
                        if eau_key_values['side_of_hopoint'] == '0':
                            eau_key_values['new_tath'] = int(eau_key_values['nb_tath']) - delta
                        else:
                            eau_key_values['new_tath'] = int(eau_key_values['nb_tath']) + delta
                        eau_key_values['new_rssith'] = '--'

                    if eau_key_values['cm_sta_dir'] == '00' or eau_key_values['cm_sta_dir'] == '11':
                        if len(cur_ta_list):
                            if min(cur_ta_list) > int(eau_key_values['nb_tath']):
                                eau_key_values['new_tath'] = min(cur_ta_list)
                            else:
                                eau_key_values['new_tath'] = eau_key_values['nb_tath']
                        if eau_key_values['side_of_hopoint'] == '0':
                            eau_key_values['new_rssith'] = int(eau_key_values['nb_rssith']) + 5
                        else:
                            eau_key_values['new_rssith'] = int(eau_key_values['nb_rssith']) - 5
                else:
                    eau_key_values['new_tath'] = '--'
                    eau_key_values['new_rssith'] = '--'
                eau_sql = processSql(self.ipaddr, self.user, self.passwd)
                eau_sql.eau_insert(db, cursor, eau_key_values['eau_mac'], traindir, eau_key_values)

            for key, value in eau_key_values.items():
                eau_key_values[key] = '0'
    def record_all_attri_to_opt(self,reconnect, timestamp, prev_recnid, cur_recnid, db, cursor, dbname, tablename, delta, select_rbt_value):
        global g_hopoint
        print('record reconnect attri, %s, %s' %(prev_recnid, cur_recnid))
        sql = "SELECT * FROM  %s WHERE id < %s and id > %s order by id desc" %(tablename, cur_recnid, prev_recnid)
        cursor.execute("USE %s" %dbname)
        cursor.execute(sql)
        src_mac = 0
        results = cursor.fetchall()
        first_count = 0
        last_count = 0
        first_cur_ta = 0
        last_cur_ta = 0
        for row in results:
            CM_START = row[27]
            if row[6] and 0 == first_count:
                first_cur_ta = int(row[6])
                first_count = 1

            if CM_START and last_count == 0:
                last_count = 1

            if row[6] and 1 == last_count:
                last_cur_ta = int(row[6])
                break
        if first_cur_ta > 0 and last_cur_ta > 0:
            if 0 == ebu_key_values['not_need_hopoint_n3']:
                if delta == 16:
                    if abs(first_cur_ta - last_cur_ta) < 60:
                        g_hopoint = 1
                else:
                    if abs(first_cur_ta - last_cur_ta) < 30:
                        g_hopoint = 1

        sql = "SELECT * FROM  %s WHERE id < %s order by id desc" %(tablename, cur_recnid)
        cursor.execute("USE %s" %dbname)
        cursor.execute(sql)
        results = cursor.fetchall()
        time_stamp3 = 0
        for row in results:
            CM_START = row[27]
            drift_ho = row[30]
            if CM_START: 
                time_stamp3 = row[2]
                if 1 == g_hopoint:
                    if int(time_stamp3) >= timestamp - 20000:

                        reconnect_reason =['3','5','6','8','12','15','16','7','9','10','14']
                        if reconnect in reconnect_reason:
                            ebu_key_values['after_hopoint_n2'] = '1'
                    else:
                        reconnect_reason =['7','9','10','14']
                        if reconnect in reconnect_reason:
                            ebu_key_values['before_hopoint_n1'] = '1'
                    
                    if drift_ho and '1' == drift_ho:
                        print('drift_ho == 1')
                        eau_key_values['before_hopoint_n1'] = '1'
                break

        cm_sta_dir = 0
        cm_dir_last = -1
        sta_dir_last = -1
        sql = "SELECT * FROM  %s WHERE id < %s and id > %s order by id desc" %(tablename, cur_recnid, prev_recnid)
        cursor.execute("USE %s" %dbname)
        cursor.execute(sql)
        results = cursor.fetchall()
        for row in results:
            cm_dir = row[23]
            if cm_dir:
                cm_dir_last = cm_dir[-1]
                break
        
        sql = "SELECT * FROM  %s WHERE id < %s and id > %s order by id desc" %(tablename, cur_recnid, prev_recnid)
        cursor.execute("USE %s" %dbname)
        cursor.execute(sql)
        results = cursor.fetchall()
        for row in results:
            stadir = row[37]
            if stadir:
                sta_dir_last = stadir[-1]
                break

        if int(cm_dir_last) > -1 and int(sta_dir_last) > -1:
            cm_sta_dir = cm_dir_last + sta_dir_last

        if '0' == ebu_key_values['not_need_hopoint_n3'] and '0' ==  ebu_key_values['before_hopoint_n1'] and '0' ==  ebu_key_values['after_hopoint_n2']:
            ebu_key_values['non_hopoint_n4'] = '1'
        
        '''insert db'''
        if '0' != ebu_key_values['mac']:
            result = ['00', '01', '10', '11']
            if cm_sta_dir in result:
                traindir = 'dir_' + cm_sta_dir
                print('traindir, %s' %traindir)
                print('capMac, %s' %ebu_key_values['mac'])
                ebu_sql = processSql(self.ipaddr, self.user, self.passwd)
                #ebu_sql.ebu_insert(db, cursor, select_rbt_value, traindir, ebu_key_values)
                ebu_sql.sql_query(db, cursor, select_rbt_value, traindir, ebu_key_values)
        for key, value in ebu_key_values.items():
            ebu_key_values[key] = '0'
            if key == 'adjust_hopoint':
                ebu_key_values[key] = '/'
    def analyze_one_reconnet_attri(self, db, cursor, sta_mac:[], dbname):
            global prev_cur_ta
            for tablename, value in db_dev_table_dict.items():
                print(dbname)
                print(tablename)
                sql_table_max_id = db_dev_table_dict[tablename]

                for key, value in eau_key_values.items():
                    eau_key_values[key] = '0'
                cursor.execute("USE %s" %dbname)
                sql = "SELECT max(id) FROM %s" %(tablename)
                cursor.execute(sql)
                row = cursor.fetchone()

                if row[0] and row[0] != db_dev_table_dict[tablename]:
                    db_dev_table_dict[tablename] = row[0]
                    print(db_dev_table_dict)
                    if sql_table_max_id == 0:
                        sql = "SELECT * FROM  %s" %(tablename)
                    else:
                        sql = "SELECT * FROM  %s WHERE id > %s" %(tablename, sql_table_max_id)
                    try:
                        recnct_prev_id = sql_table_max_id
                        recnct_cur_id = 0
                        delta = 0
                        blk_mac = 0
                        mac = 0
                        record_tath_timer_val = 0
                        count = 0
                        cursor.execute(sql)
                        results = cursor.fetchall()
                        for row in results:
                            hotime = row[36]
                            if hotime and int(hotime) > 30:
                                eau_key_values['over_hotime'] = '1'
                                eau_key_values['hotime'] = hotime
                                eau_key_values['before_ta'] = '0'
                                eau_key_values['before_rssi'] = '0'
                                eau_key_values['after_ta'] = '0'
                                eau_key_values['after_rssi'] = '0'
                            if row[3]:
                                eau_key_values['chn'] = row[3]
                                if int(row[3]) == 153 or int(row[3]) == 137 or int(row[3]) == 120:
                                    delta = 16
                                else:
                                    delta = 8
                            if row[6]:
                                if int(row[6]) > 0:
                                    prev_cur_ta = int(row[6])
                            #tath
                            if row[40]:
                                    record_tath_timer_val = int(row[40])
                            #mac
                            if row[4]:
                                mac = row[4]
                                eau_key_values['mac'] = mac

                                if  blk_mac != row[4]:
                                    blk_mac = row[4]
                                    count = 0
                                    
                            if row[42]:
                                if count == 0 and mac == blk_mac:
                                    
                                    recnct_cur_id = row[0]
                                    print('recnct_prev_id = %s' %recnct_prev_id)
                                    print('recnct_cur_id = %s' %recnct_cur_id)
                                    eau_key_values['timestamp'] = int(row[2])
                                    eau_key_values['reconnect_flag'] = '1'
                                    eau_key_values['recnct_reason'] = row[42]

                                    if delta == 16:
                                        if abs(prev_cur_ta - record_tath_timer_val) < 60:
                                            eau_key_values['non_hopoint'] = '1'
                                    else:
                                        if abs(prev_cur_ta - record_tath_timer_val) < 30:
                                            eau_key_values['non_hopoint'] = '1'
                                    self.record_attri_to_opt(row[42], recnct_prev_id, recnct_cur_id, db, cursor, sta_mac, dbname, tablename, delta)
                                    recnct_prev_id = recnct_cur_id
                                    count = count + 1
                                    continue

                                if mac != 0 and mac == blk_mac:
                                    recnct_prev_id = row[0]
                            
                    except:
                        print ("Error: unable to fetch data")

                else:
                    print("%s id no increase!" %tablename)

    def analyze_all_reconnet_attri(self, db, cursor, dbname, select_rbt_value):
            global g_hopoint
            global g_flag_update
            for tablename, value in db_dev_table_dict_all.items():
                print(dbname)
                print(tablename)
                sql_table_max_id = db_dev_table_dict_all[tablename]
                cursor.execute("USE %s" %dbname)
                sql = "SELECT max(id) FROM %s" %(tablename)
                cursor.execute(sql)
                row = cursor.fetchone()

                if row[0] and row[0] != db_dev_table_dict_all[tablename]:
                    g_flag_update = 0
                    db_dev_table_dict_all[tablename] = row[0]
                    print(db_dev_table_dict_all)
                    if sql_table_max_id == 0:
                        sql = "SELECT * FROM  %s" %(tablename)
                    else:
                        sql = "SELECT * FROM  %s WHERE id > %s" %(tablename, sql_table_max_id)
                    try:
                        recnct_prev_id = sql_table_max_id
                        recnct_cur_id = 0
                        mac = 0
                        delta = 0
                        blk_mac = 0
                        record_tath_timer_val = 0
                        count = 0
                        prev_cur_ta = 0
                        g_hopoint = 0
                        cursor.execute(sql)
                        results = cursor.fetchall()
                        for row in results:
                            if row[3]:
                                if int(row[3]) == 153 or int(row[3]) == 137 or int(row[3]) == 120:
                                    delta = 16
                                else:
                                    delta = 8
                            if row[6]:
                                if int(row[6]) > 0:
                                    prev_cur_ta = int(row[6])
                            #tath
                            if row[40]:
                                    record_tath_timer_val = int(row[40])
                            #mac
                            if row[4]:
                                mac = row[4]
                                ebu_key_values['mac'] = mac
                                if  blk_mac != row[4]:
                                    blk_mac = row[4]
                                    count = 0
                                    
                            if row[42]:
                                if count == 0 and mac == blk_mac:
                                    
                                    recnct_cur_id = row[0]
                                    print('recnct_prev_id = %s' %recnct_prev_id)
                                    print('recnct_cur_id = %s' %recnct_cur_id)
                                    reconnect_reason =['1','2']
                                    if row[42] in reconnect_reason:
                                        ebu_key_values['not_need_hopoint_n3'] = '1'

                                    if '0' == ebu_key_values['not_need_hopoint_n3']:
                                        if delta == 16:
                                            if abs(prev_cur_ta - record_tath_timer_val) < 60:
                                                g_hopoint = 1
                                        else:
                                            if abs(prev_cur_ta - record_tath_timer_val) < 30:
                                                g_hopoint = 1

                                    self.record_all_attri_to_opt(row[42], int(row[2]), recnct_prev_id, recnct_cur_id, db, cursor, dbname, tablename, delta, select_rbt_value)
                                    g_hopoint = 0
                                    recnct_prev_id = recnct_cur_id
                                    count = count + 1
                                    continue

                                if mac != 0 and mac == blk_mac:
                                    recnct_prev_id = row[0]
                            
                    except:
                        print ("Error: unable to fetch data")

                else:
                    
                    print("%s id no increase!" %tablename)
            if 0 == g_flag_update:
                ebu_sql = processSql(self.ipaddr, self.user, self.passwd)
                ebu_sql.ebu_update_adjust_hopoint(db, cursor, select_rbt_value)
                g_flag_update = 1

    def process_sql(self, sta_mac:[], dbname, tablename, select_rbt_value, reconnect_value, switch_delay_value):
        self.list_to_dict(tablename,select_rbt_value, reconnect_value, switch_delay_value)

        if '1' == reconnect_value and 'one' == select_rbt_value:
            self.analyze_one_reconnet_attri(self.db, self.cursor, sta_mac, dbname)
        elif '1' == reconnect_value and 'all' == select_rbt_value:
            self.analyze_all_reconnet_attri(self.db, self.cursor, dbname, select_rbt_value)
    def list_to_dict(self, list,select_rbt_value, reconnect_value, switch_delay_value):
        if '1' == reconnect_value and 'one' == select_rbt_value:
            for name in list:
                key = name
                if key not in db_dev_table_dict.keys():
                    db_dev_table_dict[key] = 0
            print(db_dev_table_dict)
        elif '1' == reconnect_value and 'all' == select_rbt_value:
            for name in list:
                key = name
                if key not in db_dev_table_dict_all.keys():
                    db_dev_table_dict_all[key] = 0
            print(db_dev_table_dict_all)