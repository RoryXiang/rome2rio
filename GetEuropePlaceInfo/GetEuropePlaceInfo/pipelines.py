# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import json
import hashlib
import time
import re
from pymongo import MongoClient

client = MongoClient(host='127.0.0.1', port=27017)
db = client['local']



class GeteuropeplaceinfoPipeline(object):
    
    def process_item(self, item, spider):
        # print("????????????????????????????????")
        # print(item)
        try:
            item_md5 = hashlib.md5(
                str(item).encode("utf")).hexdigest()

            prev_place_id = item.get('dep_place')
            if prev_place_id:
                dep_place = int(prev_place_id)
            next_place_id = item.get('arr_place')
            if next_place_id:
                next_place_id = int(next_place_id)
            ture_2place_array = [dep_place, next_place_id]
            
            item['by'] = 'xsp'
            db['place_full_transfer_test'].insert(item)
            place_list = item['places']
            airlines_list = item['airlines']
            for way in item['routes']:
                with open('way_name.txt', 'a') as f:
                    f.write('way_name:'+way['name']+str(way)+'\n')
                way_arrPlace_name = (
                    place_list[way['segments'][0]['arrPlace']]).get('shortName')
                if str(way_arrPlace_name) == 'Destination':
                    # continue
                    pass
                # if len(way['segments']) != 1:
                #     continue
                way_dic = {}
                way_name = way['name']
                line_type = way['segments'][0]['segmentKind']
                name_str = re.search('Drive', way['name'])
                if name_str:
                    with open('diffirence.txt', 'a') as f:
                        f.write(way['name']+":->"+str(way)+'\n')
                if way['name'] == 'Drive':
                    print("++++01")
                    # continue
                    line_type = "drive"
                    distance = way['distance']
                    duration = way['totalDuration']
                    price = way['indicativePrices'][0]['price']
                    way_id_md5 = (hashlib.md5((str(ture_2place_array) + str(
                        line_type) + str(way_name)).encode("utf")).hexdigest())
                    print(way_id_md5)
                    now_time = time.strftime("%Y-%m-%d %H:%M:%S",
                                             time.localtime())

                    way_dic = {'dep_place': prev_place_id,
                               'arr_place': next_place_id,
                               'way_name': way_name,
                               'way_type': line_type,
                               'distance': distance,
                               'duration': duration,
                               'price': price,
                               'created_at': str(now_time),
                               'by': 'xsp'}
                    with open('drive_info.txt', 'a') as f:
                        f.write('New'+str(way_dic)+'\n')
                    # print(way_dic)
                    # try:

                    # db['place_line_info'].insert(way_dic)
                        # way_collection.insert(way_dic)
                        # except:
                        #     pass

                elif line_type == "air":
                    way_depPlace_name = \
                        (place_list[way['segments'][0]['depPlace']])[
                            'shortName']
                    way_depPlace_type = \
                        (place_list[way['segments'][0]['depPlace']])['kind']
                    way_depPlace_timezone = \
                        (place_list[way['segments'][0]['depPlace']])['timeZone']
                    way_depPlace_lat = \
                    (place_list[way['segments'][0]['depPlace']])[
                        'lat']
                    way_depPlace_lng = \
                    (place_list[way['segments'][0]['depPlace']])[
                        'lng']
                    way_depPlace_code = \
                        (place_list[way['segments'][0]['depPlace']])['code']
                    way_depPlace_countrycode = \
                        (place_list[way['segments'][0]['depPlace']])[
                            'countryCode']

                    way_arrPlace_name = \
                        (place_list[way['segments'][0]['arrPlace']])[
                            'shortName']
                    way_arrPlace_type = \
                        (place_list[way['segments'][0]['arrPlace']])['kind']
                    way_arrPlace_timezone = \
                        (place_list[way['segments'][0]['arrPlace']])['timeZone']
                    way_arrPlace_lat = \
                    (place_list[way['segments'][0]['arrPlace']])[
                        'lat']
                    way_arrPlace_lng = \
                    (place_list[way['segments'][0]['arrPlace']])[
                        'lng']
                    way_arrPlace_code = \
                        (place_list[way['segments'][0]['arrPlace']])['code']
                    way_arrPlace_countrycode = \
                        (place_list[way['segments'][0]['arrPlace']])[
                            'countryCode']
                    distance = way['distance']
                    duration = way['totalDuration']
                    price = way['indicativePrices'][0]['price']
                    line_list = []
                    id_list = []
                    # codeshares_list = []
                    for one_line in way['segments'][0]['outbound']:
                        continue
                        # if len(one_line['hops']) != 1:  # 判断是否有转机
                        #     continue
                        # else:
                        flight = one_line['hops'][0]['flight']
                        airline = airlines_list[one_line['hops'][0]['airline']][
                            'name']
                        depTerminal = one_line['hops'][0].get('depTerminal')
                        depTime = one_line['hops'][0]['depTime']
                        arrTime = one_line['hops'][0]['arrTime']
                        operatingDays = one_line['operatingDays']
                        one_price = one_line['indicativePrices'][0]['price']
                        priceLow = one_line['indicativePrices'][0]['priceLow']
                        priceHigh = one_line['indicativePrices'][0]['priceHigh']
                        currency = one_line['indicativePrices'][0]['currency']
                        # if 'codeshares' in (one_line['hops'][0]): 班号共用,我试图搞清楚这是什么航空公司的小花样,但是没成功,加上这个字段会超过mongo写入上限就先注释了
                        #     for x in one_line['hops'][0]['codeshares']:
                        #         sharesAirlin = airlines_list[x['airline']]['name']
                        #         sharesFlight = x['flight']
                        #          codeshares_list.append({'airline': sharesAirlin,
                        #                                  'flight': sharesFlight})
                        # else:
                        #     codeshares_list = []
                        id_list.append(
                            [flight, depTerminal, airline, operatingDays])
                        line_list.append({'flight': flight,
                                          'airline': airline,
                                          'depTerminal': depTerminal,
                                          'depTime': depTime,
                                          'arrTime': arrTime,
                                          'operatingDays': operatingDays,
                                          'price': one_price,
                                          'priceLow': priceLow,
                                          'priceHigh': priceHigh,
                                          'currency': currency})  # 'codeshares': codeshares_list})
                        way_id_md5 = (hashlib.md5((str(line_type) + str(
                            id_list) + str(ture_2place_array)).encode(
                            "utf")).hexdigest())
                        dep_id_md5 = hashlib.md5(
                            (str(way_depPlace_lat) + str(
                                way_depPlace_lng)).encode(
                                "utf")).hexdigest()
                        arr_id_md5 = hashlib.md5(
                            (str(way_arrPlace_lat) + str(
                                way_arrPlace_lng)).encode(
                                "utf")).hexdigest()
                        print(way_id_md5)
                        now_time = time.strftime("%Y-%m-%d %H:%M:%S",
                                                 time.localtime())
                        way_dic = {'dep_place': prev_place_id,
                                   'arr_place': next_place_id,
                                   'way_name': way_name,
                                   'way_type': line_type,
                                   'distance': distance,
                                   'duration': duration,
                                   'price': price,
                                   'dep_station_name': way_depPlace_name,
                                   'arr_station_name': way_arrPlace_name,
                                   'dep_station_ES_id': str(dep_id_md5),
                                   'arr_station_ES_id': str(arr_id_md5),
                                   'place_line_info': line_list,
                                   'created_at': str(now_time),
                                   'by': 'xsp'}

                        dep_station_dic = {'name': way_depPlace_name,
                                           'type': way_depPlace_type,
                                           'timezone': way_depPlace_timezone,
                                           'latitude': way_depPlace_lat,
                                           'longitude': way_depPlace_lng,
                                           'station_code': way_depPlace_code,
                                           'countrycode': way_depPlace_countrycode,
                                           'by': 'xsp'}

                        arr_station_dic = {'name': way_arrPlace_name,
                                           'type': way_arrPlace_type,
                                           'timezone': way_arrPlace_timezone,
                                           'latitude': way_arrPlace_lat,
                                           'longitude': way_arrPlace_lng,
                                           'station_code': way_arrPlace_code,
                                           'countrycode': way_arrPlace_countrycode,
                                           'by': 'xsp'}
                        # print(way_dic)
                        try:
                            pass
                            # es.index(index='place_line_info', body=way_dic,
                            #          id=(way_id_md5), doc_type='path')
                            db['place_line_info'].insert(way_dic)
                                # way_collection.insert(way_dic)
                        except:
                            pass
                        try:
                            db['tidy_station_info'].insert(dep_station_dic)
                            pass
                            #
                                # station_collection.insert(dep_station_dic)
                        except:
                            pass
                        try:
                            db['tidy_station_info'].insert(arr_station_dic)
                            pass
                            #
                            # station_collection.insert(arr_station_dic)
                        except:
                            pass

                elif 'agencies' in way['segments'][0]:
                    continue
                    id_list = []
                    line_list = []
                    distance = way['distance']
                    duration = way['totalDuration']
                    frequency = way['segments'][0]['agencies'][0].get(
                        'frequency',
                        '')
                    operatingDays = way['segments'][0]['agencies'][0].get(
                        'operatingDays', '')
                    lineNames = way['segments'][0]['agencies'][0].get(
                        'lineNames',
                        '')
                    lineCodes = way['segments'][0]['agencies'][0].get(
                        'lineCodes',
                        '')
                    priceLow = way['segments'][0]['indicativePrices'][0].get(
                                             'priceLow', '')
                    priceHigh = way['indicativePrices'][0].get('priceHigh', '')
                    currency = way['indicativePrices'][0].get('currency', '')
                    one_price = way['indicativePrices'][0].get('price', '')

                    way_depPlace_name = (
                        place_list[way['segments'][0]['depPlace']]).get(
                        'shortName')
                    way_depPlace_type = (
                        place_list[way['segments'][0]['depPlace']]).get('kind')
                    way_depPlace_timezone = (
                        place_list[way['segments'][0]['depPlace']]).get(
                        'timeZone')
                    way_depPlace_lat = (
                        place_list[way['segments'][0]['depPlace']]).get('lat')
                    way_depPlace_lng = (
                        place_list[way['segments'][0]['depPlace']]).get('lng')
                    way_depPlace_code = (
                        place_list[way['segments'][0]['depPlace']]).get('code',
                                                                        '')
                    way_depPlace_countrycode = (
                        place_list[way['segments'][0]['depPlace']]).get(
                        'countryCode')

                    way_arrPlace_name = (
                        place_list[way['segments'][0]['arrPlace']]).get(
                        'shortName')
                    way_arrPlace_type = (
                        place_list[way['segments'][0]['arrPlace']]).get('kind')
                    way_arrPlace_timezone = (
                        place_list[way['segments'][0]['arrPlace']]).get(
                        'timeZone')
                    way_arrPlace_lat = (
                        place_list[way['segments'][0]['arrPlace']]).get('lat')
                    way_arrPlace_lng = (
                        place_list[way['segments'][0]['arrPlace']]).get('lng')
                    way_arrPlace_code = (
                        place_list[way['segments'][0]['arrPlace']]).get('code',
                                                                        '')
                    way_arrPlace_countrycode = (
                        place_list[way['segments'][0]['arrPlace']]).get(
                        'countryCode')

                    line_dic = {}
                    line_dic['linenames'] = lineNames
                    line_dic['linecodes'] = lineCodes
                    line_dic['frequency'] = frequency
                    line_dic['operatingDays'] = operatingDays
                    line_dic['price'] = one_price
                    line_dic['priceLow'] = priceLow
                    line_dic['priceHigh'] = priceHigh
                    line_dic['currency'] = currency
                    line_list.append(line_dic)
                    id_list.append(
                        [lineNames, lineCodes, operatingDays, frequency])
                    way_id_md5 = hashlib.md5(
                        (str(line_type) + str(id_list) + str(
                            ture_2place_array)).encode("utf")).hexdigest()
                    dep_id_md5 = hashlib.md5(
                        (str(way_depPlace_lat) + str(way_depPlace_lng)).encode(
                            "utf")).hexdigest()
                    arr_id_md5 = hashlib.md5(
                        (str(way_arrPlace_lat) + str(way_arrPlace_lng)).encode(
                            "utf")).hexdigest()
                    print(way_id_md5)
                    now_time = time.strftime("%Y-%m-%d %H:%M:%S",
                                             time.localtime())
                    way_dic = {'dep_place': prev_place_id,
                               'arr_place': next_place_id,
                               'way_name': way_name,
                               'way_type': line_type,
                               'distance': distance,
                               'duration': duration,
                               'price': one_price,
                               'dep_station_name': way_depPlace_name,
                               'arr_station_name': way_arrPlace_name,
                               'dep_station_ES_id': str(dep_id_md5),
                               'arr_station_ES_id': str(arr_id_md5),
                               'place_line_info': line_list,
                               'created_at': str(now_time),
                               'by': 'xsp'}

                    dep_station_dic = {'name': way_depPlace_name,
                                       'type': way_depPlace_type,
                                       'timezone': way_depPlace_timezone,
                                       'latitude': way_depPlace_lat,
                                       'longitude': way_depPlace_lng,
                                       'station_code': way_depPlace_code,
                                       'countrycode': way_depPlace_countrycode,
                                       'by': 'xsp'}

                    arr_station_dic = {'name': way_arrPlace_name,
                                       'type': way_arrPlace_type,
                                       'timezone': way_arrPlace_timezone,
                                       'latitude': way_arrPlace_lat,
                                       'longitude': way_arrPlace_lng,
                                       'station_code': way_arrPlace_code,
                                       'countrycode': way_arrPlace_countrycode,
                                       'by': 'xsp'}
                    # print(way_dic)
                    try:
                        db['place_line_info'].insert(way_dic)

                        pass
                        #
                    except:
                        pass
                    try:
                        db['tidy_station_info'].insert(dep_station_dic)
                        pass
                        #
                    except:
                        pass
                    try:
                        db['tidy_station_info'].insert(arr_station_dic)
                        pass

                    except:
                        pass

                else:
                    
                    continue
                    distance = way['distance']
                    duration = way['totalDuration']
                    price = way.get('indicativePrices', [{1: 1}])[0].get(
                        'price',
                        '')
                    way_id_md5 = hashlib.md5(
                        (str(line_type) + str(way_name) + str(
                            ture_2place_array)).encode("utf")).hexdigest()
                    print(way_id_md5)
                    now_time = time.strftime("%Y-%m-%d %H:%M:%S",
                                             time.localtime())
                    way_dic = {'dep_place': prev_place_id,
                               'arr_place': next_place_id,
                               'way_name': way_name,
                               'way_type': line_type,
                               'distance': distance,
                               'duration': duration,
                               'price': price,
                               'created_at': str(now_time),
                               'by': 'xsp'}
                    # print(way_dic)
                    try:
                        pass
                        # db['place_line_info'].insert(way_dic)
                        # way_collection.insert(way_dic)
                    except:
                        pass
        except Exception as e:
            pass



