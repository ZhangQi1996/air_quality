# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class AqiInfoItem(scrapy.Item):
    # 城市名
    city_name = scrapy.Field()
    # 城市码
    city_code = scrapy.Field()
    # 日期
    date = scrapy.Field()
    # AQI
    aqi = scrapy.Field()
    # 首要污染物
    pri_pollutant = scrapy.Field()


class CurDataItem(scrapy.Item):
    # 城市名
    city_name = scrapy.Field()
    # 城市码
    city_code = scrapy.Field()
    # 时间 yyyy-mm-dd hh:mm:ss
    time = scrapy.Field()
    # AQI
    aqi = scrapy.Field()
    # PM2.5
    pm2_5 = scrapy.Field()
    # PM10
    pm10 = scrapy.Field()
    # SO2
    so2 = scrapy.Field()
    no2 = scrapy.Field()
    co = scrapy.Field()
    o3 = scrapy.Field()
    pri_pollutant = scrapy.Field()
