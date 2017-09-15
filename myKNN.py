#encoding:utf-8
#import pandas as pd
import numpy as np
import csv
import joblib
import os
import utils
import geohash
import time

# 还可以考虑biketype对路程的影响
user_dict_path = '../cache/user_dict.pkl'
sloc_dict_path = '../cache/sloc_dict.pkl'
eloc_dict_path = '../cache/eloc_dict.pkl'
loc_volumn_dict_path = '../cache/loc_volume_dict.pkl'


def process_train(train_file):
    print 'process train...'
    flag = ''


    if os.path.exists(user_dict_path):
        user_dict = joblib.load(user_dict_path)
        flag = '1'
    if os.path.exists(sloc_dict_path):
        sloc_dict = joblib.load(sloc_dict_path)
        flag += '-2'
    if os.path.exists(eloc_dict_path):
        eloc_dict = joblib.load(eloc_dict_path)
        flag += '-3'
    if os.path.exists(loc_volumn_dict_path):
        loc_volumn_dict = joblib.load(loc_volumn_dict_path)
        flag += '-4'

    if flag == '1-2-3-4':
        return user_dict,sloc_dict,eloc_dict,loc_volumn_dict

    user_dict = {}
    sloc_dict = {}
    eloc_dict = {}
    loc_volumn_dict = {}

    train = csv.DictReader(open(train_file))
    for rec in train:
        if rec['userid'] in user_dict:
            user_dict[rec['userid']].append(rec)
        else:
            user_dict[rec['userid']] = [rec]
        if rec['geohashed_start_loc'] in sloc_dict:
            sloc_dict[rec['geohashed_start_loc']].append(rec)
        else:
            sloc_dict[rec['geohashed_start_loc']] = [rec]
        if rec['geohashed_end_loc'] in eloc_dict:
            eloc_dict[rec['geohashed_end_loc']].append(rec)
        else:
            eloc_dict[rec['geohashed_end_loc']] = [rec]

        if rec['geohashed_start_loc'] in loc_volumn_dict:
            if rec['holiday'] == 0:#workday
                loc_volumn_dict[rec['geohashed_start_loc']]['out_workday'] += 1
            else:#holiday
                loc_volumn_dict[rec['geohashed_start_loc']]['out_holiday'] += 1
        else:
            loc_volumn_dict[rec['geohashed_start_loc']] = {}
            if rec['holiday'] == 0:#workday
                loc_volumn_dict[rec['geohashed_start_loc']]['out_workday'] = 1
                loc_volumn_dict[rec['geohashed_start_loc']]['in_workday'] = 0
                loc_volumn_dict[rec['geohashed_start_loc']]['in_holiday'] = 0
                loc_volumn_dict[rec['geohashed_start_loc']]['out_holiday'] = 0
            else:
                loc_volumn_dict[rec['geohashed_start_loc']]['out_workday'] = 0
                loc_volumn_dict[rec['geohashed_start_loc']]['in_workday'] = 0
                loc_volumn_dict[rec['geohashed_start_loc']]['in_holiday'] = 0
                loc_volumn_dict[rec['geohashed_start_loc']]['out_holiday'] = 1
        if rec['geohashed_end_loc'] in loc_volumn_dict:
            if rec['holiday'] == 0:#workday
                loc_volumn_dict[rec['geohashed_end_loc']]['in_workday'] += 1
            else:#holiday
                loc_volumn_dict[rec['geohashed_end_loc']]['in_holiday'] += 1
        else:
            loc_volumn_dict[rec['geohashed_end_loc']] = {}
            if rec['holiday'] == 0:#workday
                loc_volumn_dict[rec['geohashed_end_loc']]['in_workday'] = 1
                loc_volumn_dict[rec['geohashed_end_loc']]['out_workday'] = 0
                loc_volumn_dict[rec['geohashed_end_loc']]['in_holiday'] = 0
                loc_volumn_dict[rec['geohashed_end_loc']]['out_holiday'] = 0
            else:
                loc_volumn_dict[rec['geohashed_end_loc']]['out_workday'] = 0
                loc_volumn_dict[rec['geohashed_end_loc']]['in_workday'] = 0
                loc_volumn_dict[rec['geohashed_end_loc']]['in_holiday'] = 1
                loc_volumn_dict[rec['geohashed_end_loc']]['out_holiday'] = 0

    joblib.dump(user_dict, user_dict_path,compress=9)
    joblib.dump(sloc_dict, sloc_dict_path, compress=9)
    joblib.dump(eloc_dict, eloc_dict_path, compress=9)
    joblib.dump(loc_volumn_dict, loc_volumn_dict_path, compress=9)
    return user_dict, sloc_dict, eloc_dict, loc_volumn_dict


def predict(test_file,weights,rst_file,user_dict,sloc_dict,eloc_dict,loc_volumn_dict):
    test = csv.DictReader(open(test_file))
    fo_rst = open(rst_file, 'w')

    print 'begin predict ...'
    i = 0
    for rec in test:
        i +=1
        if i%2000 ==0:
            print 'test:',i
        user = rec['userid']
        lat_lng = rec['lat_lng_start'][1:-1].split(',')
        result = {}
        eloc_counts = {}
        sloc = rec['geohashed_start_loc']

        if user in user_dict:#存在的用户
            hist_recs = user_dict[user]
            for hist_rec in hist_recs:
                score = 0
                ##########还可考虑反向的情况
                #起点的距离
                if hist_rec['geohashed_start_loc'] == rec['geohashed_start_loc']:
                    score += 0
                else:
                    hist_lat_lng_s = hist_rec['lat_lng_start'][1:-1].split(',')
                    slocs_dist = utils.cal_distance(float(lat_lng[0]),float(lat_lng[1]),float(hist_lat_lng_s[0]),float(hist_lat_lng_s[1]))
                    score += weights['slocs_dist'] * slocs_dist

                #骑行距离
                hist_lat_lng_e = hist_rec['lat_lng_end'][1:-1].split(',')
                seloc_dist = utils.cal_distance(float(lat_lng[0]),float(lat_lng[1]),float(hist_lat_lng_e[0]),float(hist_lat_lng_e[1]))
                score += weights['seloc_dist'] * abs(seloc_dist - float(hist_rec['dist']))

                #假期信息
                score += weights['holiday'] * abs(float(rec['holiday']) - float(hist_rec['holiday']))

                #开始时间差
                score += weights['deltatime'] * (abs(float(rec['minutes']) - float(hist_rec['minutes']))/60)

                #time_zone影响
                if rec['time_zone'] != hist_rec['time_zone']:
                    score += weights['time_zone']

                #biketype
                score += weights['biketype'] * abs(float(rec['biketype']) - float(hist_rec['biketype']))

                #到达该目的地的频次
                hist_eloc = hist_rec['geohashed_end_loc']
                if hist_eloc in eloc_counts:
                    eloc_counts[hist_eloc] += 1
                else:
                    eloc_counts[hist_eloc] = 1

                if hist_eloc in result:
                    if result[hist_eloc] > score:
                        result[hist_eloc] = score
                else:
                    result[hist_eloc] = score


            if len(result) < 3:#不足3个目的地
                expand_locs = geohash.expand(sloc)
                for expand_loc in expand_locs:
                    result[expand_loc] = 2000
                    if expand_loc in eloc_counts:
                        eloc_counts[expand_loc] += 1
                    else:
                        eloc_counts[expand_loc] = 1

        else:#新用户
            sloc = rec['geohashed_start_loc']



            if sloc in sloc_dict:

                hist_recs = sloc_dict[sloc]
                for hist_rec in hist_recs:
                    score = 0
                    hist_eloc = hist_rec['geohashed_end_loc']

                    # 骑行距离
                    hist_lat_lng_e = hist_rec['lat_lng_end'][1:-1].split(',')
                    seloc_dist = utils.cal_distance(float(lat_lng[0]),float(lat_lng[1]), float(hist_lat_lng_e[0]), float(hist_lat_lng_e[1]))
                    score += weights['seloc_dist'] * abs(seloc_dist - float(hist_rec['dist']))

                    # 假期信息
                    score += weights['holiday'] * abs(float(rec['holiday']) - float(hist_rec['holiday']))

                    # 流量信息
                    if rec['holiday'] == 1:#假期
                        in_workday = float(loc_volumn_dict[hist_eloc]['in_workday'])/11.0
                        in_holiday = float(loc_volumn_dict[hist_eloc]['in_holiday'])/4.0
                        in_workday_holiday = in_workday - in_holiday
                        score += weights['in_holiday'] * (in_workday_holiday/in_workday)
                    else:#工作日
                        in_workday = float(loc_volumn_dict[hist_eloc]['in_workday']) / 11.0
                        score += weights['in_workday'] * in_workday * -1.0

                    # 开始时间差
                    score += weights['deltatime'] * (abs(float(rec['minutes']) - float(hist_rec['minutes'])) / 60)

                    # time_zone影响
                    if rec['time_zone'] != hist_rec['time_zone']:
                        score += weights['time_zone']

                    # biketype
                    score += weights['biketype'] * abs(float(rec['biketype']) - float(hist_rec['biketype']))

                    # 到达该目的地的频次
                    if hist_eloc in eloc_counts:
                        eloc_counts[hist_eloc] += 1
                    else:
                        eloc_counts[hist_eloc] = 1

                    if hist_eloc in result:
                        if result[hist_eloc] > score:
                            result[hist_eloc] = score
                    else:
                        result[hist_eloc] = score

            if sloc in eloc_dict:

                hist_recs = eloc_dict[sloc]
                for hist_rec in hist_recs:
                    score = 0
                    hist_eloc = hist_rec['geohashed_start_loc']

                    # 骑行距离
                    hist_lat_lng_e = hist_rec['lat_lng_start'][1:-1].split(',')
                    seloc_dist = utils.cal_distance(float(lat_lng[0]), float(lat_lng[1]), float(hist_lat_lng_e[0]), float(hist_lat_lng_e[1]))
                    score += weights['seloc_dist'] * abs(seloc_dist - float(hist_rec['dist'])) * weights['reverse']

                    # 假期信息
                    score += weights['holiday'] * abs(float(rec['holiday']) - float(hist_rec['holiday'])) * weights['reverse']

                    # 流量信息
                    if rec['holiday'] == 1:#假期
                        in_workday = float(loc_volumn_dict[hist_eloc]['out_workday'])/11.0
                        in_holiday = float(loc_volumn_dict[hist_eloc]['out_holiday'])/4.0
                        in_workday_holiday = in_workday - in_holiday
                        score += weights['in_holiday'] * (in_workday_holiday/in_workday) * weights['reverse']
                    else:#工作日
                        in_workday = float(loc_volumn_dict[hist_eloc]['out_workday']) / 11.0
                        score += weights['in_workday'] * in_workday * -1.0 * weights['reverse']

                    # 开始时间差
                    score += weights['deltatime'] * (abs(float(rec['minutes']) - float(hist_rec['minutes'])) / 60) * weights['reverse']

                    # time_zone影响
                    if rec['time_zone'] != hist_rec['time_zone']:
                        score += weights['time_zone'] * weights['reverse']

                    # biketype
                    score += weights['biketype'] * abs(float(rec['biketype']) - float(hist_rec['biketype'])) * weights['reverse']

                    # 到达该目的地的频次
                    if hist_eloc in eloc_counts:
                        eloc_counts[hist_eloc] += 1
                    else:
                        eloc_counts[hist_eloc] = 1

                    if hist_eloc in result:
                        if result[hist_eloc] > score:
                            result[hist_eloc] = score
                    else:
                        result[hist_eloc] = score

            expand_locs = geohash.expand(sloc)
            for expand_loc in expand_locs:
                result[expand_loc] = 2000
                if expand_loc in eloc_counts:
                    eloc_counts[expand_loc] += 1
                else:
                    eloc_counts[expand_loc] = 1


        for rst in result:
            result[rst] = result[rst] / (eloc_counts[rst] ** weights['eloc_counts'])  # 0

        bestResult = sorted(result.items(), key=lambda d: d[1])
        string = rec['orderid']
        num = 0
        for item in bestResult:
            string += ',' + item[0]
            num += 1
            if num == 3:
                break
        fo_rst.write(string + '\n')

    fo_rst.close()



if __name__ == '__main__':
    weights = {
        'holiday':3.5,
        'seloc_dist':2.5,
        'slocs_dist':1.2,
        'deltatime':2.5,
        'in_holiday':0.5,
        'in_workday':0.4,
        'biketype':0.5,
        'time_zone':3,
        'reverse':1.2,
        'eloc_counts':1.1,
    }

    t0 = time.time()
    train_file = '../data/train_processed_1.csv'
    test_file = '../data/test_processed_1.csv'
    rst_file = '../data/result_'+str(t0) +'.csv'
    user_dict, sloc_dict, eloc_dict, loc_volumn_dict = process_train(train_file)

    predict(test_file,weights,rst_file,user_dict,sloc_dict,eloc_dict,loc_volumn_dict)

    print('一共用时{}秒'.format(time.time() - t0))