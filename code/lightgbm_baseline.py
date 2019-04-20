#!/usr/bin/env python
# coding=utf-8
import pandas as pd
import numpy as np
from datetime import datetime,timedelta
import lightgbm as lgb
import logging
logging.basicConfig(level=logging.INFO)
import os
import gc
import warnings 
warnings.filterwarnings("ignore")

def group_data(file1_dir,file2_dir,group_file):
    '''
        这个函数的功能是将日志文件 聚合成 每个广告每天的曝光数量数据
    '''
    if os.path.exists(group_file):
        logging.info(group_file + 'already exists')
        return
    
    
    dir_list = []
    read_dir = os.listdir(file1_dir)
    for file in read_dir:
        dir_list.append(file)
        
    write_file_list = ['exposure_group_'+str(i) + '.csv' for i in range(len(dir_list))]
        
    def handle_one_file(file_1,file_2):
        if not os.path.exists(file_1):
            logging.info(file_1 + ' not exits')
            return
        if os.path.exists(file_2):
            logging.info(file_2 + 'already exists')
            return
    
        names = ['request_id','request_time','location_id','user_id','ad_id','ad_size',\
                 'ad_bid','ad_pctr','ad_quality_ecpm','ad_totalEcpm']

        exposure_df = pd.read_csv(file_1,delimiter = '\t',\
                                  parse_dates = ['request_time'],header=None,names = names)
        logging.info('read data success')

        exposure_df['request_time'] = pd.to_datetime(exposure_df['request_time'],unit='s')

        exposure_df['request_day'] = exposure_df['request_time'].dt.strftime('%Y-%m-%d')


        logging.info('handle time success')

        # 聚合数据
        
        group_df = exposure_df.groupby(['request_day','ad_id'])['request_id'].count()
        del exposure_df
        gc.collect()
        
        group_df = group_df.reset_index()
        group_df = group_df.rename(columns = {'request_id':'exp_num'})

        logging.info('group data success')

        group_df.to_csv(file_2, header=True, index=None, sep=';', mode='w')
        
        del group_df
        gc.collect()
        
        logging.info(file_2 + ' dump success')
        
    
    for i,read_file in enumerate(dir_list):
        handle_one_file(file1_dir + read_file,file2_dir + write_file_list[i])
        
        
    # 接下来应该合并groupby数据
    logging.info('start handle merge group data')
    df1 = pd.read_csv(file2_dir + write_file_list[0],delimiter = ';')
    df1 = df1.set_index(['request_day','ad_id'])
    for i in range(1,len(write_file_list)):
        df = pd.read_csv(file2_dir + write_file_list[i],delimiter = ';')
        df = df.set_index(['request_day','ad_id'])
        df1 = df1.add(df,fill_value=0)
        del df
        gc.collect()
    logging.info('merge success')
    df1.to_csv(group_file)
    logging.info(group_file + ' dump success')
    
    
    
# def process_data(from_file,to_file):
#     '''
#         这个函数是简单的添加lagging特征，只考虑lagging特征
#     '''
#     if not os.path.exists(from_file):
#         logging.info(from_file + ' not exists')
#         return
#     if os.path.exists(to_file):
#         logging.info(to_file + ' already exists')
#         return
    
#     df = pd.read_csv(from_file,delimiter = ';',parse_dates = ['request_day'])
    
    
#     def add_days(x,i):
#         ss = datetime.strptime(x,'%Y-%m-%d') + timedelta(days = i)
#         return ss.strftime('%Y-%m-%d')
    
#     def create_lagging(df,df_origin,i):
#         df1 = df_origin.copy()
#         df1['request_day'] = df1['request_day'].apply(add_days,args = (i,))
#         df1 = df1.rename(columns = {'exp_num':'lagging' + str(i)})
#         df2 = pd.merge(df,df1[['ad_id','request_day','lagging' + str(i)]],on = ['request_day','ad_id'],\
#                       how = 'left')
#         del df1
#         gc.collect()
#         return df2
    
#     lagging = 5
#     df1 = create_lagging(df, df, 1)
#     for i in range(2, 2 + 1):
#         df1 = create_lagging(df1, df, i)
        
    # 现在的df1已经有了lagging特征，其中ad_id 统计有73万个，如果one hot 就太稀疏了，采用label_encode
    
    

if __name__ == '__main__':
    group_data('../data/testA/sep_data/','../data/testA/group_data/','../data/testA/exposure_group_all.csv')
