import pymysql.cursors
import configparser
import warnings
# 连接配置信息示例
# config = {
#           'host':'localhost',
#           'port':3306,
#           'user':'root',
#           'password':'123',
#           'db':'nsbd',
#           'charset':'utf8mb4',
#           'cursorclass':pymysql.cursors.DictCursor,
#           }


class Sql:
    """Conn模块中的Sql类是搭配DataSource类使用的,此类做连接实例对象"""
    def __init__(self, conf=None, conn_pool=None):
        """conf是接收从DataSource传递过来的配置参数,conn_pool是接收传递过来的连接池"""
        if not isinstance(conn_pool, DataSource):
            raise Exception('请配置连接池给连接实例')
        else:
            self.conn_list = conn_pool.conn_list
            self.config = conf
            self.connection = pymysql.connect(**self.config)

    def close(self):
        """打开一个连接后的关闭连接操作"""
        self.conn_list.append(self)#把操作完成的连接实例在追加到连接池的末尾

    def __createPybean(self, class_path):
        path_list = class_path.split('.')
        module_name = path_list[-2]  # 模块名
        class_name = path_list[-1]  # 类名
        p_m_name = '.'.join(path_list[:-1])  # 包名并模块名
        module = __import__(p_m_name, fromlist=module_name)
        return getattr(module, class_name)()

    def __reflectToBean(self, data_dict, class_path, is_private_attr):
        """data_dict是数据字典，class_path是要映射到类名的路径,is_private_attr是映射的对象属性是否为隐私类型"""
        bean = self.__createPybean(class_path)#动态生成该类
        bean_attr = bean.__dict__#该类的bean属性
        if is_private_attr is True:
            for key in list(data_dict.keys()):
                if '_' + class_path.split('.')[-1] + '__' + key in bean_attr.keys():
                    try:
                        getattr(bean, 'set' + key.capitalize())(data_dict[key])#获取bean的set方法
                    except AttributeError:
                        raise AttributeError('请正确配置pybean的setter,getter方法')
                else:
                    raise Exception('属性映射失败,请确保取得的数据库表中列名与pybean的属性名一致')
        else:
            for key in list(data_dict.keys()):
                if key in bean_attr.keys():
                    setattr(bean, key, data_dict[key])#映射bean中的属性
                else:
                    raise Exception('属性映射失败,请确保取得的数据库表中列名与pybean的属性名一致')
        return bean

    def query(self, sql, args=None, class_path=None, is_private_attr=True):
        """查询"""
        result = None
        with self.connection.cursor() as cursor:
            if args is None:
                cursor.execute(sql)
            else:
                cursor.execute(sql, args)
            result = cursor.fetchall()
            if class_path is not None:
                list = result
                result = []
                for item in list:
                    result.append(self.__reflectToBean(item, class_path, is_private_attr))#进行对象映射
        self.connection.commit()
        return result

    def insert(self, sql, args=None, ignore_integrity_error=True):
        try:
            with self.connection.cursor() as cursor:
                if args is None or len(args) == 0:
                    cursor.execute(sql)
                else:
                    cursor.execute(sql, args)
            self.connection.commit()
        except pymysql.err.IntegrityError as e:     # 完整性错误，如插入重复键
            print("Error: %s %s" % (e, type(e)))
            if ignore_integrity_error is False:
                self.connection.rollback()
            else:
                print("\tand the Error has been ignored(there was not a rollback in the transaction)")
                print("\tyou should set False to param:ignore_integrity_error, if you donnot want ignore that error..")
        except Exception as e:
            print("Error: %s %s" % (e, type(e)))
            self.connection.rollback()
            raise e

    def insert_many(self, sql, args=None, ignore_integrity_error=True):
        try:
            with self.connection.cursor() as cursor:
                if args is not None and len(args) != 0:
                    cursor.executemany(sql, args)
            self.connection.commit()
        except pymysql.err.IntegrityError as e:     # 完整性错误，如插入重复键
            print("Error: %s %s" % (e, type(e)))
            if ignore_integrity_error is False:
                self.connection.rollback()
            else:
                print("\tand the Error has been ignored(there was not a rollback in the transaction)")
                print("\tyou should set False to param:ignore_integrity_error, if you donnot want ignore that error..")
        except Exception as e:
            print("Error: %s %s" % (e, type(e)))
            self.connection.rollback()
            raise e

    def update(self, sql, args=None):
        self.insert(sql, args)

    def update_many(self, sql, args=None):
        self.insert_many(sql, args)

    def delete(self, sql, args=None):
        self.insert(sql, args)

    def _del(self):
        try:
            self.connection.close()
        except Exception as e:
            print("Error: %s %s" % (e, type(e)))


class DataSource:

    def __init__(self, conf, max_conn_counts=5):
        """max_conn_count是最大连接数,conf是连接数据库的基本配置,它可以是一个字典类型也可以是一个配置文件名，\
        若是配置文件名,则配置文件中的session名必须为‘[db]’详细参考ConfigParser模块的使用"""
        print('the CONNPOOL is initiating...')
        if isinstance(conf, dict):
            self.conf = conf
        elif isinstance(conf, str):
            print('正在读取DB配置文件...')
            self.conf = {}
            self.__readConfFile(conf)
        else:
            raise Exception('conf参数必须为dict或str类型')
        if not isinstance(max_conn_counts, int):
            raise Exception("最大连接数必须为int型")
        elif max_conn_counts not in range(1,11):#连接数介于1-10之间
            raise Exception("最大连接数必须介于1-10之间")
        else:
            print('the init of CONNPOOL is running...')
            # 连接池的存储连接列表
            self.conn_list = []
            for i in range(0, max_conn_counts):
                self.conn_list.append(Sql(conf=self.conf, conn_pool=self))
            print('the init of CONNPOOL is successful...')

    def __readConfFile(self, filePath):
        cp = configparser.ConfigParser()
        cp.read(filePath, encoding='utf-8')#编码设置为utf-8
        self.conf['host'] = cp.get('db', 'host')
        self.conf['port'] = cp.getint('db', 'port')
        self.conf['user'] = cp.get('db', 'user')
        self.conf['password'] = cp.get('db', 'password')
        self.conf['db'] = cp.get('db', 'db')
        self.conf['charset'] = 'utf8mb4' #默认编码UTF-8
        self.conf['cursorclass'] = pymysql.cursors.DictCursor #默认游标

    def getConnect(self):
        if len(self.conn_list) == 0:
            raise Exception('连接池中目前没有可用的连接,请稍后再试')
        else:
            return self.conn_list.pop(0)#从连接池中取出最前面的一个连接实例

    def deleteAll(self):
        """在程序运行时关闭连接池"""
        warnings.warn('it is a deprecated func, please use exit() func..', DeprecationWarning)
        print('the CONNPOOL is log-offing...')
        for conn in self.conn_list:
            conn._del()
        print('the CONNPOOL has log-offed...')

    def exit(self):
        self.deleteAll()

