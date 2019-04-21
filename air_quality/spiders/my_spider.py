import scrapy
from air_quality.items import AqiInfoItem, CurDataItem
from air_quality.settings import DATA_SOURCE, DB_CONFIG, CUR_DATA_LIST, AQI_INFO_LIST, EMAIL_CONF
from air_quality.connpool import DataSource
from scrapy.mail import MailSender
import time

class AirQualitySpider(scrapy.Spider):
    name = 'my_spider'
    start_urls = [
        "http://datacenter.mee.gov.cn/aqiweb2/",
    ]
    # 执行一次parse就加一加到24后就重置为0
    global_tag = 0

    def start_requests(self):
        # 用于在启动时产生所需的东西
        DATA_SOURCE['data_source'] = DataSource(conf=DB_CONFIG, max_conn_counts=1)
        return [scrapy.FormRequest(url, callback=self.parse) for url in self.start_urls]

    @staticmethod
    def close(spider, reason):
        """在关闭之前释放连接池"""
        DATA_SOURCE['data_source'].exit()
        # 下面是通用写法
        scrapy.Spider.close(spider, reason)

    @staticmethod
    def send_bug_email(err: BaseException):
        mailer = MailSender(
            smtphost="smtp.163.com",  # 发送邮件的服务器
            mailfrom="18239961260@163.com",  # 邮件发送者
            smtpuser="18239961260@163.com",  # 用户名
            smtppass="zq15067522063",  # 发送邮箱的密码不是你注册时的密码，而是授权码！！！切记！
            smtpport=25  # 端口号
        )
        to = ["358929931@qq.com", "1424851327@qq.com"]
        subject = u"啊欧~~，你的程序GG了..."
        body = """<html>
                    <body>
                        <h3><i style='color:#349CFF;'>【Infinity Group: BUG侦测系统】</i></h3>
                        <p>
                            <strong>助手小i提醒您</strong>  位于
                            <font color='green'>
                                <a href='https://www.aliyun.com/'>阿里云服务器</a>
                            </font>上基于scrapy的爬虫程序已经GG了，
                            <font color='red'>请赶快前往抢修BUG！！！</font>
                        </p>
                        <h4><font color='red'>TRACEBACK:</font></h4>
                        <p><font color='red'>%s</font></p>
                        <p><font color='red'>%s</font></p>
                    </body>
                  </html>
                """ % (err.__str__(), '出错类型：' + str(err.__class__).lstrip('<').rstrip('>'))
        cc = None
        mailer.send(to=to, subject=subject, body=body, cc=cc, mimetype='text/HTML')  # 抄送类似于分发

    @staticmethod
    def batch_insert_and_update():
        """
        为解决单条数据插入或者更新数据库的问题，引入批量处理
        用到的参数：全局的CUR_DATA_LIST, AQI_INFO_LIST
        :return:
        """
        conn = DATA_SOURCE['data_source'].getConnect()  # 从连接池中获取一个连接
        # '410100' : 郑州,
        # '410200' : 开封,
        # '410300' : 洛阳,
        # '410700' : 新乡,
        # '411000' : 许昌

        # 删除所有非以上城市的实时数据
        conn.delete("DELETE FROM cur_data WHERE city_code NOT IN ('410100', '410200', '410300', '410700', '411000')")
        args = []
        while len(CUR_DATA_LIST) > 0:
            item = CUR_DATA_LIST.pop()
            args.append((
                    item.get('city_code'),
                    item.get('time'),
                    item.get('aqi'),
                    item.get('pm2_5'),
                    item.get('pm10'),
                    item.get('so2'),
                    item.get('no2'),
                    item.get('co'),
                    item.get('o3'),
                    item.get('pri_pollutant'),
                ))
        conn.insert_many("INSERT INTO cur_data(city_code, time, aqi, pm2_5, pm10, so2, no2, co, o3, pri_pollutant) "
                         "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);", args=args)
        # 非实时数据
        if len(AQI_INFO_LIST) > 0:
            args = []
            while len(AQI_INFO_LIST) > 0:
                item = AQI_INFO_LIST.pop()
                args.append((item.get('city_code'), item.get('date'), item.get('aqi'), item.get('pri_pollutant')))
            conn.insert_many("INSERT INTO aqi_info(city_code, date, aqi, pri_pollutant) VALUES(%s, %s, %s, %s);",
                             args=args)
        conn.close()
        print("the current data has been updated on:%s" % time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())))

    def parse(self, response):
        try:
            rsp = response
            cur_cities_tr = rsp.selector.css('#demo1 #legend_01_table tr')
            for cur_city_tr in cur_cities_tr:
                cur_data = CurDataItem()
                tr_head_str = cur_city_tr.css('::attr(onclick)')[0].extract()
                s = tr_head_str.lstrip('openWin2(').rstrip(')').split(',')
                cur_data['time'], cur_data['city_code'] = s[-2][1:-1], s[-1][1:-1]  # 需要经过pipeline处理
                city = cur_city_tr.css('td::text')
                cur_data['city_name'] = city[0].extract()
                cur_data['aqi'] = city[1].extract()
                cur_data['pm2_5'] = city[2].extract()
                cur_data['pm10'] = city[3].extract()
                cur_data['so2'] = city[4].extract()
                cur_data['no2'] = city[5].extract()
                cur_data['co'] = city[6].extract()
                cur_data['o3'] = city[7].extract()
                cur_data['pri_pollutant'] = city[8].extract()   # 需要经过pipeline处理
                yield cur_data
            if self.global_tag == 0:   # 每24小时后更新aqi_info
                aqi_cities_tr = rsp.selector.css('#colee_02 #legend_02_table tr')
                for aqi_city_tr in aqi_cities_tr:
                    aqi_info = AqiInfoItem()
                    tr_head_str = aqi_city_tr.css('::attr(onclick)')[0].extract()
                    s = tr_head_str.lstrip('openWin2(').rstrip(')').split(',')
                    aqi_info['date'], aqi_info['city_code'] = s[-2][1:-1], s[-1][1:-1]  # 需要经过pipeline处理
                    city = aqi_city_tr.css('td::text')
                    aqi_info['city_name'] = city[0].extract()
                    aqi_info['aqi'] = city[1].extract()
                    aqi_info['pri_pollutant'] = city[3].extract()   # 需要经过pipeline处理
                    yield aqi_info
                # 每天更新完后清除cur_data表中超过十四天的旧数据
                # TODO
            self.global_tag += 1
            if self.global_tag == 24:
                self.global_tag = 0
            self.batch_insert_and_update()
            time.sleep(3600)   # 每隔一个小时访问一次
            yield scrapy.Request(self.start_urls[0], self.parse)    # 开启下一轮
        except Exception as e:
            self.crawler.engine.close_spider(self, 'response msg error %s, job done!' % 'spider出现BUG自动关闭')
            AirQualitySpider.send_bug_email(e)
