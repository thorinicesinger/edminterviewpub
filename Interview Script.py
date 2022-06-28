# -*- coding: utf-8 -*-

"""
Created on Wed Dec 22 10:51:27 2021
FOR DEV
Usage:       python.exe filename.py -env XXXXX -db YYYYY -log ZZZZZ
             where XXXXX = server (ie. ADMSdev\ADMSdev)
                   YYYYY = database (ie. ADMS_dev)
                   ZZZZZ = path for logfile (ie \ADMSShare\Dev\Data\In\logs)
@author: dequinal
"""
import sys
import os
import datetime
import pymssql
import pandas as pd
import argparse
 
#GLOBAL VARIABLES
today = datetime.datetime.today()
lv_sql1 = '''SELECT * FROM DBO.FILE_PATHS'''


def fx_create_log(path):
    if not os.path.isdir(path):
        os.mkdir(path)
    else:
        print("Directory already exists")


def fx_write_log(message, log_path):
    ts = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S - ')
    file = open(log_path + 'log_delete_files.txt', 'a')
    file.write(ts + message)
    file.close()


def fx_connection_string_open(lv_server, lv_database, lv_sql):
    """ The function will establish a connection with SQL Database"""
    cnxn = pymssql.connect(server=lv_server, database=lv_database)
    lv_query = pd.read_sql(lv_sql1, cnxn)
    cnxn.close()
    return lv_query


def main(args=None):
    #INPUT PARAMETERS
    parser = argparse.ArgumentParser(description='Connection to Database')
    parser.add_argument('-env', action='store', dest='server',
                        help='Server Name', required='True',)
    parser.add_argument('-db', action='store', dest='db',
                        help='Database Environment', required='True',)
    parser.add_argument('-log', action='store', dest='logs',
                        help='LogFile Path', required='True',)
    results = parser.parse_args(args)
    lv_server = results.server
    lv_database = results.db
    lv_logpath = results.logs
    
    # CONNECTION TO DATABASE AND SCRIPT EXECUTION
    df_path = fx_connection_string_open(lv_server, lv_database, lv_sql1)
    fx_create_log(lv_logpath)
    for path in df_path.itertuples(index=True, name='Pandas'):
        lv_path = getattr(path, "FILE_PATH")
        lv_days = getattr(path, "RETENTION_DAYS")
        if os.path.isdir(lv_path):
            try:
                for file_name in os.listdir(lv_path):
                    file_path = os.path.join(lv_path, file_name)
                    file_time = os.stat(file_path).st_mtime
                    file_time = (datetime.datetime.fromtimestamp(file_time) - 
                                 today)
                    if file_time.days <= -lv_days:
                        print(file_path, file_time.days)
                        fx_write_log('[SUCCESS]' + file_path + ' Created: ' +
                                     str(-1*file_time.days) + ' days ago \n',
                                     lv_logpath)
                        os.remove(file_path)
            except:
                print("[ERROR] Invalid Directory " + lv_path)
                fx_write_log('[ERROR] Invalid Directory ' + lv_path,
                             lv_logpath)
        else:
            print("[ERROR] Invalid Directory " + lv_path)
            fx_write_log('[ERROR] Invalid Directory ' + lv_path + '\n',
                         lv_logpath)

if __name__ == '__main__':
    main(sys.argv[1:])
    print(pd.Timestamp("today").strftime("%m/%d/%Y %H:%M:%S") +
          ": Script has been successfully executed")