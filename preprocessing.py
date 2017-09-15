#coding:utf-8
import pandas as pd
import numpy as np
import os
import utils
import geohash
import csv
import datetime

train_path = '../data/train.csv'
test_path = '../data/test.csv'
loc_info_path = '../data/loc_info.csv'

train_filtered_path = '../data/train_processed.csv'
train_filtered_no_path = '../data/train_processed_others.csv'

test_merged_path = '../data/test_processed.csv'
test_old_user_path = '../data/test_processed_old.csv'
test_new_user_path = '../data/test_processed_newer.csv'


def getDistanceDF(raw_df):
    lat_lng_start = raw_df['lat_lng_start'].values
    lat_lng_end = raw_df['lat_lng_end'].values


    dt = np.dtype('float,float')
    start_arr = np.array(lat_lng_start,dt)
    end_arr = np.array(lat_lng_end,dt)

    raw_df['euli_dist'] = utils.cal_euli_dist(start_arr['f0'],start_arr['f1'],end_arr['f0'],end_arr['f1'])
    raw_df['dist'] = utils.cal_distance(start_arr['f0'],start_arr['f1'],end_arr['f0'],end_arr['f1'])

def getCityCode(raw_df,type,loc_info_df):
    loc_info_df = loc_info_df[['geohashed_loc', 'city_code','lat_lng']]
    if type==0:#train file
        loc_info_df.rename(columns={'geohashed_loc': 'geohashed_start_loc'},inplace=True)
        sloc_merge = pd.merge(raw_df, loc_info_df, on='geohashed_start_loc', how='left')
        loc_info_df.rename(columns={'geohashed_start_loc': 'geohashed_end_loc'},inplace=True)
        merged_df = pd.merge(sloc_merge, loc_info_df, on='geohashed_end_loc', how='left',suffixes=('_start', '_end'))
    else:
        loc_info_df.rename(columns={'geohashed_loc': 'geohashed_start_loc'}, inplace=True)
        merged_df = pd.merge(raw_df, loc_info_df, on='geohashed_start_loc', how='left')
        merged_df.rename(columns={'city_code': 'city_code_start','lat_lng':'lat_lng_start'}, inplace=True)

    return merged_df


#将测试集中不曾出现过的城市数据从训练集合中过滤掉
def filter_by_city(raw_df,cities):
    filtered_exists = raw_df[(raw_df['city_code_start'].apply(lambda x: x in cities)) | (raw_df['city_code_end'].apply(lambda x: x in cities))]
    filtered_no_exists = raw_df[(raw_df['city_code_start'].apply(lambda x: x not in cities)) & (raw_df['city_code_end'].apply(lambda x: x not in cities))]

    return filtered_exists,filtered_no_exists

#将测试集按照用户是否在训练集中出现过进行分类：new和old
def filter_by_users(raw_df,users_list):
    filtered_old = raw_df[raw_df['userid'].apply(lambda x:x in users_list)]
    filtered_new = raw_df[raw_df['userid'].apply(lambda x:x not in users_list)]

    return filtered_old,filtered_new

def loc2latlng(raw_df):
    raw_df['lat_lng'] = pd.Series(raw_df['geohashed_loc'].apply(lambda x: geohash.decode_exactly(x)[:2]))


def preprocess():
    print 'Loading loc_info data...'
    loc_info_df = pd.read_csv(loc_info_path)
    #print 'get latitude and longitude...'
    #loc2latlng(loc_info_df)
    #loc_info_df.to_csv(loc_info_path,index=False)

    print 'Loading train and test...'
    train = pd.read_csv(train_path)
    test = pd.read_csv(test_path)
    train_users = list(set(train['userid']))


    print 'Merge city code with train and test files'
    train_merged = getCityCode(train,0,loc_info_df)
    test_merged = getCityCode(test,1,loc_info_df)
    test_merged.to_csv(test_merged_path,index=False)

    print 'Get distance info of train'
    getDistanceDF(train_merged)


    print 'Filter train data by cities'
    cities_in_test = list(set(test_merged['city_code_start']))
    train_filtered_city,train_filtered_city_no = filter_by_city(train_merged,cities_in_test)
    train_filtered_city.to_csv(train_filtered_path,index=False)
    train_filtered_city_no.to_csv(train_filtered_no_path,index=False)

    print 'Divide test data by users'
    test_old,test_newer = filter_by_users(test_merged,train_users)
    test_old.to_csv(test_old_user_path,index=False)
    test_newer.to_csv(test_new_user_path,index=False)

def del_time(stime):
    stime = stime.split(' ')
    mydate = stime[0].split('-')
    mytime = stime[1].split(':')
    holiday = 0
    holiday_list = [13,14,20,21,28,29,30]
    if int(mydate[2] in holiday_list):
        holiday = 1


    if int(mytime[0]) < 6 and int(mytime[0]) >= 1:
        time_zone = 1
    elif int(mytime[0]) < 10 and int(mytime[0]) >= 6:
        time_zone = 2
    elif int(mytime[0]) < 11 and int(mytime[0]) >= 10:
        time_zone = 3
    elif int(mytime[0]) < 14 and int(mytime[0]) >= 11:
        time_zone = 4
    elif int(mytime[0]) < 17 and int(mytime[0]) >= 14:
        time_zone = 5
    else:
        time_zone = 6

    minutes = int(mytime[0]) * 60 + int(mytime[1])
    return holiday,time_zone,minutes

if __name__ == '__main__':
    #preprocess()
    #test = pd.read_csv(test_merged_path)
    print 'load data'
    train = csv.DictReader(open(train_filtered_path))
    test = csv.DictReader(open(test_merged_path))

    train_users = {}

    columns = train.fieldnames
    columns.extend(['holiday', 'time_zone', 'minutes'])
    print 'deal with train starttime'
    i = 0
    with open('../data/train_processed_1.csv', 'wb') as trainfile:
        writer_train = csv.DictWriter(trainfile, fieldnames=columns)
        writer_train.writeheader()
        for rec in train:
            i += 1
            if i%20000==0:
                print i
            rec['holiday'],rec['time_zone'],rec['minutes'] = del_time(rec['starttime'])
            if rec['userid'] not in train_users:
                train_users['userid'] = 0
            writer_train.writerow(rec)
    trainfile.close()


    #train_users = list(set(train_users))
    columns = test.fieldnames
    columns.extend(['holiday', 'time_zone', 'minutes','newer'])
    print 'deal with test'
    i = 0
    with open('../data/test_processed_1.csv', 'wb') as testfile:
        writer_test = csv.DictWriter(testfile, fieldnames=columns)
        writer_test.writeheader()
        for rec in test:
            i += 1
            if i % 20000 == 0:
                print i
            if rec['userid'] in train_users:
                rec['newer'] = 0
            else:
                rec['newer'] = 1
            rec['holiday'],rec['time_zone'],rec['minutes'] = del_time(rec['starttime'])

            writer_test.writerow(rec)
    testfile.close()