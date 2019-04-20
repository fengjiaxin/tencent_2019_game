#!/usr/bin/env python
# coding=utf-8
import os
import pandas as pd
import numpy as np
from datetime import datetime,timedelta
import gc
import logging
logging.basicConfig(level=logging.INFO)


def handle_ad_op(ad_static_path,ad_operation_path,ad_op_mid_path):
    if not os.path.exists(ad_static_path):
        logging.info(ad_static_path + ' not exits')
        return
    if not os.path.exists(ad_operation_path):
        logging.info(ad_operation_path + ' not exits')
        return
    if os.path.exists(ad_op_mid_path):
        logging.info(ad_op_mid_path + ' already exits')
        return
    
    ## static ad
    static_feature_names = ['ad_id','create_time','ad_acc_id','good_id','good_class','ad_trade_id','ad_size']
    ad_static_df = pd.read_csv(ad_static_path,delimiter = '\t',\
                              parse_dates = ['create_time'],header=None,names = static_feature_names,dtype={'ad_id':int,"ad_acc_id": int,\
                              "good_id": str, "good_class": str, "ad_trade_id": str,'ad_size':str}) 
    
    logging.info(ad_static_path +' load success')

    ad_static_df['create_datetime'] = pd.to_datetime(ad_static_df['create_time'],unit='s')
    ad_static_df['create_datetime'] = ad_static_df['create_datetime'].astype(str)

    
    # operation ad
    operation_names = ['ad_id','create_update_time','op_type','op_set','op_value']

    ad_op_df = pd.read_csv(ad_operation_path,delimiter = '\t',header=None,names = operation_names,\
                           dtype={"ad_id": int,'create_update_time':int,"op_type": int,"op_set": int, "op_value": object})

    logging.info(ad_operation_path +' load success')
    
    '''
        将create_update_time不为0的时间修改成‘2015-04-07 09:43:55’格式
    '''
    def convert_time(x):
        x = str(x)
        return x[0:4] + '-' + x[4:6] + '-' + x[6:8] + ' ' + x[8:10] + ':' + x[10:12] + ':' + x[12:14]

    ad_op_df.loc[ad_op_df['create_update_time'] != 0,'create_update_time']=ad_op_df['create_update_time'].apply(convert_time)
    
    '''
    发现很多新建的时间都为0，所以先填充时间
    '''
    ad_op_df.loc[ad_op_df['create_update_time'] == 0,'create_update_time']=ad_op_df['ad_id'].map(dict(zip(ad_static_df.ad_id,ad_static_df.create_datetime)))


    ad_op_df.sort_values(by = ['ad_id','create_update_time'],inplace=True)

    ad_op_df = ad_op_df.reset_index()
    ad_op_df.drop(columns = ['index'],inplace=True)
    
    ad_op_df.to_csv(ad_op_mid_path,sep='\t',index=False)
    logging.info(ad_op_mid_path + ' dump success')
    del ad_static_df
    del ad_op_df
    gc.collect()
    
    
    
def handle_ad_op_by_line(ad_op_mid_path,ad_op_mid_path2):
    if not os.path.exists(ad_op_mid_path):
        logging.info(ad_op_mid_path + ' not exits')
        return

    if os.path.exists(ad_op_mid_path2):
        logging.info(ad_op_mid_path2 + ' already exits')
        return
    
    
    '''
        接下来整合广告的静态数据和动态数据
        新建的dataframe需要的列有
        ad_id
        create_time
        ad_acc_id
        good_id
        good_class
        ad_trade_id
        ad_size
        target_people:目标人群
        deliver_time:投放时段
        ad_bid:出价
        valid_time:广告该设置的有效时间,字符串格式 2019-03-09 13:40:03-2019-03-09 14:40:03
        其中唯一索引是ad_id 和 valid_time


        在整合动态，静态数据之前，首先需要整合动态数据
        动态数据的列有：
        ad_id
        ad_bid
        deliver_time
        target_people
        valid_time
    '''
    
    ad_op_df = pd.read_csv(ad_op_mid_path,delimiter='\t')
    with open(ad_op_mid_path2,'w') as w:
        w.write('ad_id\tad_bid\tdeliver_time\ttarget_people\tvalid_time\n')

        # 遍历广告的动态流水数据
        # ad_id	create_update_time	op_type	op_set	op_value

        ad_id = ''
        ad_bid = None
        deliver_time = ''
        target_people = ''
        valid_start_time = ''

        index = 0

        logging.info('begin handle ad operation data')

        for print_index,row in ad_op_df.iterrows():
            if print_index % 10000 == 0:
                logging.info('read %d lines'%print_index)
            #logging.info(print_index)
            # 如果是新建类型
            if row['op_type'] == 2:
                # 广告出价
                if row['op_set'] ==2:
                    ad_bid = int(row['op_value'])
                # 人群定向
                elif row['op_set'] == 3:
                    target_people = row['op_value']
                # 投放时段
                elif row['op_set'] == 4:
                    #deliver_time = convertStr2Interval(row['op_value'])
                    deliver_time = row['op_value']
                # time\id
                valid_start_time = row['create_update_time']
                ad_id = row['ad_id']
                continue
            # 修改类型
            elif row['op_type'] == 1:
                # 是同一个ad_id
                if row['ad_id'] == ad_id:
                    # 如果修改的是广告出价
                    if row['op_set'] == 2:
                        # 需要将之前的数据插入到新的data中
                        w.write(str(ad_id) + '\t' + str(ad_bid) + '\t' + str(deliver_time) + '\t' + str(target_people) + '\t' + str(valid_start_time) + '-' + str(row['create_update_time']) + '\n')
#                         new_op_df.loc[index,'ad_id'] = ad_id
#                         new_op_df.loc[index,'ad_bid'] = ad_bid
#                         new_op_df.loc[index,'deliver_time'] = deliver_time
#                         new_op_df.loc[index,'target_people'] = target_people
#                         new_op_df.loc[index,'valid_time'] = valid_start_time + '-' + row['create_update_time']

                        # 重新设置时间段
                        valid_start_time = row['create_update_time']
                        ad_bid = int(row['op_value'])
                        index += 1

                    # 如果修改的是投放时段
                    if row['op_set'] == 4:
                        # 需要将之前的数据插入到新的data中
#                         new_op_df.loc[index,'ad_id'] = ad_id
#                         new_op_df.loc[index,'ad_bid'] = ad_bid
#                         new_op_df.loc[index,'deliver_time'] = deliver_time
#                         new_op_df.loc[index,'target_people'] = target_people
#                         new_op_df.loc[index,'valid_time'] = valid_start_time + '-' + row['create_update_time']
                        w.write(str(ad_id) + '\t' + str(ad_bid) + '\t' + str(deliver_time) + '\t' + str(target_people) + '\t' + str(valid_start_time) + '-' + str(row['create_update_time']) + '\n')
                        # 重新设置时间段
                        valid_start_time = row['create_update_time']
                        #deliver_time = convertStr2Interval(row['op_value'])
                        deliver_time = row['op_value']
                        index += 1

                    # 如果修改的是人群定向
                    if row['op_set'] == 3:
                        # 需要将之前的数据插入到新的data中
#                         new_op_df.loc[index,'ad_id'] = ad_id
#                         new_op_df.loc[index,'ad_bid'] = ad_bid
#                         new_op_df.loc[index,'deliver_time'] = deliver_time
#                         new_op_df.loc[index,'target_people'] = target_people
#                         new_op_df.loc[index,'valid_time'] = valid_start_time + '-' + row['create_update_time']
                        w.write(str(ad_id) + '\t' + str(ad_bid) + '\t' + str(deliver_time) + '\t' + str(target_people) + '\t' + str(valid_start_time) + '-' + str(row['create_update_time']) + '\n')
                        # 重新设置时间段
                        valid_start_time = row['create_update_time']
                        target_people = row['op_value']
                        index += 1


                    # 如果修改的是广告状态
                    if row['op_set'] == 1:
                        # 如果设置为无效
                        if row['op_value'] == '0':
#                             new_op_df.loc[index,'ad_id'] = ad_id
#                             new_op_df.loc[index,'ad_bid'] = ad_bid
#                             new_op_df.loc[index,'deliver_time'] = deliver_time
#                             new_op_df.loc[index,'target_people'] = target_people
#                             new_op_df.loc[index,'valid_time'] = valid_start_time + '-' + row['create_update_time']
                            w.write(str(ad_id) + '\t' + str(ad_bid) + '\t' + str(deliver_time) + '\t' + str(target_people) + '\t' + str(valid_start_time) + '-' + str(row['create_update_time']) + '\n')
                            # 重新设置时间段
                            valid_start_time = ''
                            index += 1
                        else:
                            valid_start_time = row['create_update_time']
                            
                    w.flush()
        logging.info('end handle ad operation data by line')
        del ad_op_df
        gc.collect()

def merge(ad_static_path,ad_op_mid_path,ad_merge_path):
    if not os.path.exists(ad_static_path):
        logging.info(ad_static_path + ' not exits')
        return
    if not os.path.exists(ad_op_mid_path):
        logging.info(ad_op_mid_path + ' not exits')
        return
    if os.path.exists(ad_merge_path):
        logging.info(ad_merge_path + ' already exits')
        return
    
    ## static ad
    static_feature_names = ['ad_id','create_time','ad_acc_id','good_id','good_class','ad_trade_id','ad_size']
    ad_static_df = pd.read_csv(ad_static_path,delimiter = '\t',\
                              parse_dates = ['create_time'],header=None,names = static_feature_names,dtype={'ad_id':object,"ad_acc_id": int,\
                              "good_id": str, "good_class": str, "ad_trade_id": str,'ad_size':str}) 
    
    logging.info(ad_static_path +' load success')
    
    
    new_op_df = pd.read_csv(ad_op_mid_path,delimiter = '\t')
    new_op_df['ad_id'] = new_op_df['ad_id'].astype(object)
    #pd.DataFrame(columns = ['ad_id','ad_bid','deliver_time','target_people','valid_time'])
    logging.info(ad_op_mid_path +' load success')
                    

    # 看起来处理的好像比较成功，因为都是新建后修改
    # 接下来就是处理时间，将字符串转换成范围
    #ad_static_df['ad_id'] = ad_static_df['ad_id'].astype(object)
    # 将这个数据与静态数据合并
    new_op_df = new_op_df.merge(ad_static_df,on = 'ad_id',how = 'left')
    new_op_df.drop(columns = ['create_time'],inplace=True)

    logging.info('merge success')
    new_op_df.to_csv(ad_merge_path,sep='\t')
    logging.info(ad_merge_path + ' dump success')
    
    '''
        可以看出每天曝光广告的基本数据，接下来需要构建必要的信息
        创建时间
        广告行业id
        商品类型
        商品ID
        广告账户id
        投放时段
        人群定向
        出价

        其中投放时段，人群定向，出价属于动态数据，不同的时间其可能不同

        接下来就是要确定曝光的时刻，这些动态的具体数值

        怎么确定？
        需要将动态数据和静态数据进行结合
    '''

def main():
    ad_static_path = '../data/testA/ad_static_feature.out'
    ad_operation_path = '../data/testA/ad_operation.dat'
    ad_op_mid_path = '../data/testA/ad_op_mid.txt'
    ad_op_mid_path2 = '../data/testA/ad_op_mid2.txt'
    ad_merge_path = '../data/testA/ad_static_dynamic_merge.csv'
    
    handle_ad_op(ad_static_path,ad_operation_path,ad_op_mid_path)
    handle_ad_op_by_line(ad_op_mid_path,ad_op_mid_path2)
    merge(ad_static_path,ad_op_mid_path2,ad_merge_path)




# 将时间转换测试
def convertOneStr2Interval(x):
    x = int(x)
    bin_str = bin(x)[2:]
    bin_len = len(bin_str)
    r_pos = bin_str.rfind('1')
    if bin_len % 2 == 0:
        end_date = str(bin_len//2) + ':00'
    else:
        end_date = str(bin_len//2) + ':30'

    interval = bin_len - r_pos - 1
    if interval % 2 == 0:
        begin_date = str(interval//2) + ':00'
    else:
        begin_date = str(interval//2) + ':30'
    return begin_date + '-' + end_date 

def convertStr2Interval(x):
    res_str = ''
    time_list = x.split(',')
    for time in time_list:
        res_str += convertOneStr2Interval(time) + ','
    return res_str[:-1]

    
    
if __name__ == '__main__':
    main()
