# -*- coding: utf-8 -*-
import scrapy
# from scrapy import log
import re
import copy
import json
#from GetEuropePlaceInfo.spiders.esClient import esClient
#from elasticsearch import client
from redis import StrictRedis
import random
from GetEuropePlaceInfo.settings import KEYS
from GetEuropePlaceInfo.spiders.mySqlConnection import mysqlConnection



connection = mysqlConnection()
cursor = connection.cursor()

redis_client = StrictRedis(
    host='127.0.0.1',
    port=6379
)


''''将所有国家的信息拿到,组建信息字典，demo：
place_dic = {country_id:{place_id:{place_info}}}
'''

country_list = [18, 2]
places_dic = {}
for country_id in country_list:
    places_dic[country_id] = {}
    try:
        places_select_sql = 'SELECT id, latitude, longitude, name FROM places' \
                            ' WHERE country_id = {} and is_forbid=0'.format(
                                                                    country_id)
        cursor.execute(places_select_sql)
        places_records = cursor.fetchall()
    except Exception as e:
        # log.logger.error(e)
        pass
    for records in places_records:
        places_dic[country_id][records['id']] = records


def get_f2place_array(country_id):
    """
    Take a POI combination that needs to be grabbed from the redis library
    and delete this set of data from the redis Library
    :return: demo : (country_id,pre_poi_id, next_poi_id)
    """
    try:
        res = redis_client.spop('place_tuple_{}'.format(country_id))
        first_tuple_str = res.decode()
        first_tuple_list = re.findall(r'\d+', first_tuple_str)
        first_tuple_list[0] = int(first_tuple_list[0])
        first_tuple_list[1] = int(first_tuple_list[1])
        first_tuple_list[2] = int(first_tuple_list[2])
        return first_tuple_list
    except Exception as e:
        pass


first_place_tuple = get_f2place_array(country_list[0])
# TODO 记得取完后切换国家
while not first_place_tuple:
    country_list.remove(country_list[0])
    first_place_tuple = get_f2place_array(country_list[0])

first_country_id = first_place_tuple[0]
# 获取两个place的 坐标
first_prev_place_id = int(first_place_tuple[1])
first_next_place_id = int(first_place_tuple[2])

firsst_place_info_dic = places_dic[first_country_id]

first_prev_dic = firsst_place_info_dic[first_prev_place_id]
first_prev_latitude = first_prev_dic['latitude']
first_prev_longitude = first_prev_dic['longitude']
# prev_name = prev_dic['name']

first_next_dic = firsst_place_info_dic[first_next_place_id]
first_next_latitude = first_next_dic['latitude']
first_next_longitede = first_next_dic['longitude']


class Rome2rioSpider(scrapy.Spider):
    name = 'rome2rio'
    allowed_domains = ['rome2rio.com']
    
    start_urls = ["http://free.rome2rio.com/api/1.4/json/Search?key=" + 
                  random.choice(KEYS) + "&currencyCode=usd&"
                                        "languageCode=eng&oPos=" + 
                  str(first_prev_latitude)+","+str(first_prev_longitude) + 
                  "&dPos=" + str(first_next_latitude)+"," 
                  + str(first_next_longitede) + "&noRideshare"]
    print(start_urls[0])
    print("****")

    def __init__(self):
        scrapy.Spider.__init__(self)
        self.place_tuple = first_place_tuple   # TODO  初始 的placetuple
        self.current_key = random.choice(KEYS)

    def parse(self, response):
        print("+++")
        print(self.place_tuple)
        print(response.status)
        if response.status == 401:
            # key失效后 返回的状态为401
            # KEYS.remove(self.current_key)
            # current_country_id = self.place_tuple[0]
            # redis_client.sadd('place_tuple_{}'.format(current_country_id),
            #                   tuple(self.place_tuple))
            pass
        elif response.status == 429:
            # key 请求次数过多
            # current_country_id = self.place_tuple[0]
            # redis_client.sadd('place_tuple_{}'.format(current_country_id),
            #                   tuple(self.place_tuple))
            pass
        else:
            full_json = json.loads(response.text, encoding='utf8')
            # full_json = response.json()
            # print("place_tuple is:{}".format(self.place_tuple))
            prev_place_id = self.place_tuple[1]
            next_place_id = self.place_tuple[2]
            del full_json['elapsedTime']  # 删除不需要的数据字段
            full_json.update({'dep_place': str(prev_place_id)})  # 更新字段的值
            full_json.update({"arr_place": str(next_place_id)})
            full_json = json.loads(
                json.dumps(full_json, default=lambda x: x.__dict__))
            item = full_json
            item['country_id'] = self.place_tuple[0]
            yield item
            # 再提取到相关信息后删除掉redis重的信息，，，但是会有坑，，多台服务器跑的时候，可能会拿到同一个数据，导致重复。建议在去的时候用spop
            current_country_id = self.place_tuple[0]
            redis_client.srem('place_tuple_{}'.format(current_country_id),
                              tuple(self.place_tuple))
        
        # 获取需要查找的两个place tuple
        ture_2place_arrays = self.get_2place_array(country_list[0])
        # TODO 记得取完后切换国家
        while not ture_2place_arrays:
            country_list.remove(country_list[0])
            ture_2place_arrays = self.get_2place_array(country_list[0])
        self.place_tuple = copy.deepcopy(ture_2place_arrays)
        country_id = ture_2place_arrays.pop(0)
        # 获取两个place的 坐标
        prev_place_id = int(ture_2place_arrays[0])
        next_place_id = int(ture_2place_arrays[1])

        place_info_dic = places_dic[country_id]

        prev_dic = place_info_dic[prev_place_id]
        prev_latitude = prev_dic['latitude']
        prev_longitude = prev_dic['longitude']
        # prev_name = prev_dic['name']

        next_dic = place_info_dic[next_place_id]
        next_latitude = next_dic['latitude']
        next_longitede = next_dic['longitude']
        # next_name = next_dic['name']

        next_url = "http://free.rome2rio.com/api/1.4/json/Search?key=" \
                   + random.choice(KEYS) + \
                   "&currencyCode=usd&languageCode=eng&oPos=" + \
                   str(prev_latitude)+","+str(prev_longitude) + \
                   "&dPos="+str(next_latitude)+","+str(next_longitede) +\
                   "&noRideshare"

        yield scrapy.Request(
            url=next_url,
            callback=self.parse,
            dont_filter=False
        )

    def get_2place_array(self, country_id_):
        """
        Take a POI combination that needs to be grabbed from the redis library 
        and delete this set of data from the redis Library
        :return: demo : (country_id,pre_poi_id, next_poi_id)
        """
        try:
            # res = redis_client.spop('place_tuple_{}'.format(country_id_))
            res = redis_client.srandmember('place_tuple_{}'.format(country_id_),
                                           1)
            # first_tuple_str = res.decode()
            first_tuple_str = res[0].decode()
            first_tuple_list = re.findall(r'\d+', first_tuple_str)
            first_tuple_list[0] = int(first_tuple_list[0])
            first_tuple_list[1] = int(first_tuple_list[1])
            first_tuple_list[2] = int(first_tuple_list[2])
            return first_tuple_list
        except Exception as e:
            pass

