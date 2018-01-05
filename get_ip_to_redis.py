# coding:utf-8
from redis import StrictRedis
import requests
import time
from copy import deepcopy


redis_client = StrictRedis(
            host='127.0.0.1',
            port=6379
)
i = 0
prev_list = []
while True:
    res = requests.get('http://http-api.taiyangruanjian.com/getip?num=6')
    # 这里写上购买ip的IP提取api  我选择每次提取6个ip，存活下限5分钟，所以5分钟前跟换ip池

    ip_list = res.content.decode().split('\n')
    for ip in ip_list:
        proxy = {'https': ip}
        try:
            rre = requests.get('https://www.baidu.com', proxies=proxy)
        except Exception as e:
            ip_list.remove(ip)

    num = len(ip_list)
    if ip_list == prev_list:  # 还是存在拿到的ip一样
        redis_client.delete('ip')
        i = 0
    for k in range(num):
        if ip_list[k]:
            redis_client.sadd('ip', ip_list[k])
    print(ip_list)
    if i == 0:
        i += 1
    else:
        pre_num = len(prev_list)
        print(prev_list)
        for kk in range(pre_num):
            redis_client.srem('ip', prev_list[kk])
    prev_list = deepcopy(ip_list)

    time.sleep(270)
