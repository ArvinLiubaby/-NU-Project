# -*- coding: utf-8 -*-
"""
Created on Wed Jun 27 14:09:05 2018

@author: Arvin
"""
import os
import cx_Oracle
import csv
import sys
from datetime import datetime
from subprocess import Popen,PIPE

class Orcl_to_CSV(object):
    
    def __init__(self):
        pass
    
    # 生成路经
    def get_csv_file(self,pt_d,table):
        try:
            os.makedirs('./test')
        except Exception as e:
            pass
        
        csv_file = '%s_%s.csv'%(table,pt_d)
        return csv_file
            
    # 从Oracle获取数据
    def get_orcl(self,pt_d,table):
        # 创建数据库连接
        db = cx_Oracle.connect('test', '12345', '127.0.0.1:1521/orcl')
        # 创建游标对象
        cursor = db.cursor()
        # 插入语句
        cell_pd_sql = 'select t.lat,t.lon,t.gender,t.age,t.pd_cnt,t.hr_id,t.clndr_dt_id from CELL_SIGN_PD_LIST_ZQ t where t.clndr_dt_id = :clndr_dt_id'
        cell_list_sql = 'select t.lat,t.lon,t.pd_cnt,t.hr_id,t.clndr_dt_id from CELL_SIGN_LIST t where t.clndr_dt_id = :clndr_dt_id'
        cell_d_sql = 'select t.cell_seq,t.cell_id,t.lac,t.city_id,t.city_nm,t.sector_id,t.sector_nm,t.lat,t.lon,t.clndr_dt_id from CELL_D_ZQ t where t.clndr_dt_id = :clndr_dt_id'
        
        table_sql = {'CELL_SIGN_PD_LIST_ZQ':cell_pd_sql
                     ,'CELL_SIGN_LIST':cell_list_sql
                     ,'CELL_D_ZQ':cell_d_sql
                     }       
        sql = table_sql[table]
        
        row = cursor.execute(sql,{'clndr_dt_id':pt_d})
        
        csv_file = self.get_csv_file(pt_d,table)
        
        # Python3用newline
#        with open(csv_file,'w', newline='') as outputFile:
        # Python2用'wb'
        with open(csv_file,'wb') as outputFile:
#        outputFile = open(csv_file,'w')
            output = csv.writer(outputFile,dialect='excel')
            
            # 获取列信息
            cols = []
            for col in cursor.description:
                cols.append(col[0])
#                print(cols)
            output.writerow(cols)
            
            # 写入csv中
            num = 0
            for row_data in cursor:
                output.writerow(row_data)
                num = num + 1
    #            print(row_data)
            print('导出: %s'%(num))
                
        # 提交事务
        db.commit()
        # 关闭相关进程
#        outputFile.close()
        cursor.close()
        db.close()
        
    
################################## 主程序 ######################################
if __name__ == '__main__':
    
    # 读文件形式读取时间
    # 时间
    with open('conf.txt') as inputFile:
        pt_d = inputFile.readline().replace('\r\n','')
        table = inputFile.readline().replace('\r\n','')
    
#    table = 'CELL_SIGN_PD_LIST_ZQ'
#    pt_d = '20180528'
    start_time = datetime.now()
    
    # 生成实例
    otc = Orcl_to_CSV()
    otc.get_orcl(pt_d,table)
    
    # 计算运行时间
    end_time = datetime.now()
#    print(end_time)
    running_time = round((end_time-start_time).seconds/60.0,2)
    print('Running: %s mins   Start:%s   End:%s '%(running_time,datetime.strftime(start_time,"%m-%d %H:%M"),datetime.strftime(end_time,"%m-%d %H:%M")))
    
    # 在程序中压缩文件
    print('Start compress...')
    # test路经到时候可以更换为其他,如专门创建一个用于保存csv的路经
    p = Popen('tar -zcvf test/%s.tar %s --remove-files'%(otc.get_csv_file(pt_d,table),otc.get_csv_file(pt_d,table)),shell=True,stdout=PIPE,stderr=PIPE)
    p.wait()
    
    if p.returncode != 0:  
        print("Error.")  
    print('Done:%s===========================================================\n'%(otc.get_csv_file(pt_d,table)))
        
# tar -zcvf aaa.log.tar.gz aaa.log --remove-files 
    
