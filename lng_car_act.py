import pandas as pd
import datetime
from tqdm import tqdm
import multiprocessing
from multiprocessing import freeze_support, RLock
import math

def judge(carlist, df_car, label):
    actcar_list = []
    for carno in tqdm(carlist, ascii=True, desc=label):
        if (df_car[df_car['车牌号'] == carno]['车载终端速度'] != 0).any():
            actcar_list.append(carno)
    return actcar_list

if __name__ == "__main__":
    freeze_support()
    df_roadlist = []
    print("******读取数据*****")
    for i in range(24):
        if i < 10:
            ti = '0' + str(i)
        else:
            ti = str(i)

        # path = "E:\\LNG供需系统\\数据\\15\\" + \
        #       ti + "\\trajectory-201908" + '15' + ti + "41.txt"
        path = "/home/my/lng_supply/20/" + \
               ti + "/trajectory-201908" + '20' + ti + "42.txt"

        df_temp = pd.read_csv(path, header=None)
        df_temp.columns = ['车牌号', '车牌颜色编码', '车辆归属省行政区域编码',
                           '车辆归属运输行业编码', '车辆归属地市编码', '车辆当前归属省行政区域编码'
            , '定位时间', '系统接收时间', '经度', '纬度', '车载终端速度',
                           '行驶记录仪速度', '总里程', '方向角', '海拔', '车辆状态码', '报警状态码']

        # df_temp = df_temp[(df_temp['车辆当前归属省行政区域编码'] >= 330000)&
        #           (df_temp['车辆当前归属省行政区域编码'] < 340000)]

        df_temp = df_temp[(df_temp['车辆当前归属省行政区域编码'] >= 370000)
                          & (df_temp['车辆当前归属省行政区域编码'] < 380000)]

        df_roadlist.append(df_temp)

    df_road = pd.concat(df_roadlist)
    df_road.drop_duplicates(keep='first', inplace=True)
    df_road.reset_index(drop=True, inplace=True)

    df_car = df_road[['车牌号', '定位时间', '经度', '纬度', '车载终端速度']]
    car_list = df_car['车牌号'].unique().tolist()
    print("******读取完毕*****")
    # car_list = car_list[:400]
    n = 16
    s = math.ceil(len(car_list)/n)
    cargroup = [car_list[i*s:(i+1)*s] for i in range(n)]
    actcar_list = []
    pool = multiprocessing.Pool(processes=n) # 创建4个进程
    for i in range(n):
        temp = pool.apply_async(judge, (cargroup[i],df_car,str(i+1),))
        actcar_list.append(temp)
    pool.close()
    pool.join()
    res_act = []
    for x in actcar_list:
        res_act = res_act + x.get()
    total = len(car_list)
    act = len(res_act)
    print(cargroup)
    print("活跃槽车数量:",act)
    print("总槽车数量：",total)

#%%
a=b=3
#%%
