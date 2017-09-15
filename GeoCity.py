# -*- coding: utf-8 -*-
# 第一行必须有，否则报中文字符非ascii码错误

import hashlib
import json
from urllib import quote_plus,quote
from urllib2 import  urlopen
import pandas as pd
import geohash
import os

print 'Loading data...'
train_locs_path = '../data/train_locs.csv'
test_locs_path = '../data/test_locs.csv'
'''
if os.path.exists(train_locs_path):
    train_locs_df = pd.read_csv(train_locs_path)
    train_locs = train_locs_df['train_locs'].values.tolist()
else:
    train_path = '../data/train.csv'
    train_data = pd.read_csv(train_path)
    train_locs = list(set(train_data['geohashed_start_loc']) & set(train_data['geohashed_end_loc']))
    train_locs_df = pd.DataFrame({'train_locs':train_locs})
    train_locs_df.to_csv(train_locs_path,index=False)
'''
if os.path.exists(test_locs_path):
    test_locs_df = pd.read_csv(test_locs_path)
    test_locs = test_locs_df['test_locs'].values.tolist()
else:
    test_path = '../data/test.csv'
    test_data = pd.read_csv(test_path)
    test_locs = list(set(test_data['geohashed_start_loc']))
    test_locs_df = pd.DataFrame({'test_locs':test_locs})
    test_locs_df.to_csv(test_locs_path,index=False)

#new_locs = list(set(test_locs).difference(set(train_locs)))

train_path = '../data/train.csv'
train_data = pd.read_csv(train_path)
new_locs = list((set(train_data['geohashed_start_loc']) | set(train_data['geohashed_end_loc'])).difference (((set(train_data['geohashed_start_loc']) & set(train_data['geohashed_end_loc']))|set(test_locs) )))

url_header = 'http://api.map.baidu.com'
#wo
myAK = 'K1M3knALfRgjzOHToOPsVYrctgTRP5yu'
mySK = 'MULotMFtO8t1sMxXxOElEkTBXcGrgE2V'
#kai
#myAK = 'e5biE28Dgg25tKNdbURRVwKZ3eRb3nNr'
#mySK = 'jrubgvepzcg2GX7k8967oM7tSZRQloek'
#mao
#myAK = 'OG1M4Ua03SGXUcaGsQLR75UfogHhelHG'
#mySK = 'k1owCVNGhfdg5VThfyBaGSW0K87r87iX'

error_locs = []
succ_locs = []
citys = []
provinces = []
districts = []
print 'Starting....:',len(new_locs)
batch_count = 0
for i in range(len(new_locs)):
    batch_count += 1
    if (i+1) % 500 == 0:
        print 'i:',i+1
    lat, lng = geohash.decode(new_locs[i])
    queryStr = '/geocoder/v2/?location='+str(lat)+','+str(lng)+'&output=json&ak='+myAK
    encodedStr = quote(queryStr, safe="/:=&?#+!$,;'@()*[]")
    rawStr = encodedStr + mySK
    mySN = hashlib.md5(quote_plus(rawStr).encode('utf-8')).hexdigest()

    url = url_header + queryStr + '&sn=' + mySN
    req = urlopen(url)
    res = req.read()
    res_json = json.loads(res)

    if res_json['status'] == 0:
        succ_locs.append(new_locs[i])
        citys.append( res_json['result']['addressComponent']['city'])
        provinces.append(res_json['result']['addressComponent']['province'])
        districts.append(res_json['result']['addressComponent']['district'])
    else:
        error_locs.append(new_locs[i])


    if batch_count == 1000:
        print 'ok:', len(succ_locs)
        batch_count = 0
        if len(succ_locs) > 0:
            loc_info = pd.DataFrame({'geohashed_loc':succ_locs,'city':citys,'province':provinces,'district':districts})
            loc_info.to_csv('loc_info.csv',index=False,encoding='gbk',mode='a',header=False)
            citys = []
            provinces = []
            districts = []
            succ_locs = []
            print 'save success!'
        if len(error_locs) > 0:
            error_df = pd.DataFrame({'error_locs':error_locs})
            error_df.to_csv('err_locs.csv',index=False,mode='a',header=False)
            error_locs = []

if len(succ_locs) > 0 :
    loc_info = pd.DataFrame({'geohashed_loc': succ_locs, 'city': citys, 'province': provinces, 'district': districts})
    loc_info.to_csv('loc_info.csv', index=False, encoding='gbk', mode='a', header=False)

if len(error_locs) > 0:
    error_df = pd.DataFrame({'error_locs': error_locs})
    error_df.to_csv('err_locs.csv', index=False, mode='a', header=False)