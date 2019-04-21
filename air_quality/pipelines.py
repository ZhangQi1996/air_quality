# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import re
from .items import CurDataItem
from .settings import CUR_DATA_LIST, AQI_INFO_LIST


class DataFormatAddrPipeline(object):
    def process_item(self, item, spider):
        pattern = r"\S+"
        time_p = r"\d+"
        if isinstance(item, CurDataItem):
            # 处理time的数据格式
            yyyy_mm_dd_hh = re.findall(time_p, item.get('time'))
            for k in ['pm2_5', 'pm10', 'so2', 'no2', 'o3', 'aqi']:
                if item[k] == '—':  # 注意这非减号
                    item[k] = 1
                else:
                    pass
            if item['co'] == '—':
                item['co'] = 0
            item['time'] = '-'.join(yyyy_mm_dd_hh[:-1]) + ' ' + yyyy_mm_dd_hh[-1] + ':00:00'
        else:
            # 处理date的数据格式
            time_p = r"\d+"
            yyyy_mm_dd = re.findall(time_p, item.get('date'))
            item['date'] = '-'.join(yyyy_mm_dd)
        item['pri_pollutant'] = re.findall(pattern, item.get('pri_pollutant'))[0]
        if len(item['pri_pollutant']) > 25:
            item['pri_pollutant'] = 'PM10,PM2.5'
        return item


class DBPipeline(object):
    def process_item(self, item, spider):
        """将一个个item分类注入到对应的list中"""
        if isinstance(item, CurDataItem):
            CUR_DATA_LIST.append(item)
        else:
            AQI_INFO_LIST.append(item)
        return None