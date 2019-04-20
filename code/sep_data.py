#!/usr/bin/env python
# coding=utf-8
from datetime import datetime
import logging
logging.basicConfig(level=logging.INFO)

'''
    由于数据量太大，所以目前的想法是切割数据
    按照天数切割数据
    统计当前行与之前行天数不同的行数
'''

def get_day_time(x):
    d = datetime.fromtimestamp(x)
    str_d = d.strftime('%Y-%m-%d')
    return str_d

def process(file,group_file,gener_dir):
    # 统计出现的天数
    '''
    day_set = set()
    with open(group_file,'r') as r:
        for line in r:
            day = line.strip().split(',')[0]
            day_set.add(day)

    logging.info(str(day_set))
    '''
    day_list = ['2019-03-03', '2019-02-18', '2019-02-24', '2019-02-19', '2019-03-14', \
    '2019-03-15', '2019-03-18', '2019-03-16', '2019-02-20', '2019-03-05', '2019-03-04', \
    '2019-03-17', '2019-03-07', '2019-03-10', '2019-02-23', '2019-03-11', '2019-02-21', \
    '2019-03-19', '2019-03-12', '2019-02-16', '2019-02-27', '2019-03-08', '2019-02-25', \
    '2019-03-06', '2019-02-28', '2019-03-02', '2019-02-22', '2019-03-09', '2019-02-26', \
    '2019-02-17', '2019-03-01', '2019-03-13']

    # 逐个逐个
    for i,day in enumerate(day_list):
        with open(file,'r') as rf,open(gener_dir + day+ 'expLog.csv','w') as w:
            for line in rf:
                vec = line.strip().split('\t')
                day_str = get_day_time(int(vec[1]))
                if day_str == day:
                    w.write(line)
            logging.info('generate ' + gener_dir + day+ 'expLog.csv success')
    logging.info('generate all success')

if __name__ == '__main__':
    process('../data/testA/totalExposureLog.out','../data/testA/exposure_group_all.csv','../data/testA/sep_data/')

