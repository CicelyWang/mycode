#coding:utf-8
import geohash
import time
import numpy as np
import math
import pandas as pd

# 计算两点之间距离
def cal_distance(lat1,lng1,lat2,lng2):
    #lat1 = float(lat1)
    #lat2 = float(lat2)
    #lon1 = float(lng1)
    #lon2 = float(lng2)
    dx = np.abs(lng1 - lng2)  # 经度差
    dy = np.abs(lat1 - lat2)  # 维度差
    b = (lat1 + lat2) / 2.0
    Lx = 6371004.0 * (dx / 57.2958) * np.cos(b / 57.2958)
    Ly = 6371004.0 * (dy / 57.2958)
    L = (Lx**2 + Ly**2) ** 0.5
    return L

#弧度转换
def rad(tude):
    return (math.pi/180.0)*tude


#返回 欧式距离  （其实还可以返回南北方向距离,东西方向距离,曼哈顿距离,方向(-0.5:0.5)，但是删了，没啥吊用）
def cal_euli_dist(lat1, lng1,lat2, lng2):
    radLat1 = rad(lat1)
    radLat2 = rad(lat2)
    a = radLat1-radLat2
    b = rad(lng1)-rad(lng2)
    R = 6378137
    d = R*2*np.arcsin(np.sqrt(np.power(np.sin(a/2),2)+np.cos(radLat1)*np.cos(radLat2)*np.power(np.sin(b/2),2)))
    #detallat = abs(a)*R
    #detalLon = math.sqrt(d**2-detallat**2)
    '''
    if b==0:
        direction = 1/2 if a*b>0 else -1/2
    else:
        direction = math.atan(detallat/detalLon*(1 if a*b>0 else -1))/math.pi
    '''
    return d

# 分组排序
def rank(data, feat1, feat2, ascending):
    data.sort_values([feat1,feat2],inplace=True,ascending=ascending)
    data['rank'] = range(data.shape[0])
    min_rank = data.groupby(feat1,as_index=False)['rank'].agg({'min_rank':'min'})
    data = pd.merge(data,min_rank,on=feat1,how='left')
    data['rank'] = data['rank'] - data['min_rank']
    del data['min_rank']
    return data

def apply_euli_dist(row):
    lat1,lng1 = row['lat_lng_start']
    lat2,lng2 = row['lat_lng_end']
    return cal_euli_dist(lat1,lng1,lat2,lng2)

def apply_dist(row):
    lat1, lng1 = row['lat_lng_start']
    lat2, lng2 = row['lat_lng_end']
    return cal_distance(lat1, lng1, lat2, lng2)


#将时间分段
def starttime2tag(shour):
    if shour <= 5:
        return 1
    elif shour > 5 and shour <= 10:
        return 2
    elif shour > 10 and shour <= 16:
        return 3
    else:
        return 4

# 相差的分钟数
def diff_of_minutes(time1, time2):
    d = {'5': 0, '6': 31, }
    try:
        days = (d[time1[6]] + int(time1[8:10])) - (d[time2[6]] + int(time2[8:10]))
        try:
            minutes1 = int(time1[11:13]) * 60 + int(time1[14:16])
        except:
            minutes1 = 0
        try:
            minutes2 = int(time2[11:13]) * 60 + int(time2[14:16])
        except:
            minutes2 = 0
        return (days * 1440 - minutes2 + minutes1)
    except:
        return np.nan
