"""
https://www.akshare.xyz/zh_CN/latest/
"""

import __init__
import time
# import redis
import logging
from common import utils
from datetime import datetime,timedelta 
from baseCrawler import Crawler
from pandas._libs.tslibs.timestamps import Timestamp
from common.database import Database
from common import config
import akshare as ak
import tushare as ts


# 配置logging
logging.basicConfig(
    level=logging.INFO,
    filename="./logs/stock_info_output.log",
    filemode='w',  # 使用写模式覆盖日志
    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
    datefmt='%a, %d %b %Y %H:%M:%S'
)


class StockInfoCrawler(Crawler):

    def __init__(self, database_name, collection_name):
        super(StockInfoCrawler, self).__init__()
        # 得到数据库管理对象
        self.db_obj = Database()
        # 得到MongoDB的集合实例
        self.col_basic_info = self.db_obj.get_collection(database_name, collection_name)
        self.database_name = database_name
        self.collection_name = collection_name
        # redis客户端配置
        # self.redis_client = redis.StrictRedis(host=config.REDIS_IP,port=config.REDIS_PORT,db=config.REDIS_CLIENT_FOR_CACHING_STOCK_INFO_DB_ID,password=config.REDIS_PASSWORD)
        # self.redis_client.set("today_date", datetime.now().strftime("%Y-%m-%d"))
        
    # 获取股票代码信息
    def get_stock_info(self):
        # 获取所有 A 股股票的实时数据
        stock_data = ak.stock_zh_a_spot()

        # 提取股票symbol和name
        stock_symbols = stock_data['symbol'].tolist()
        stock_names=stock_data['name'].tolist()
        
        # 将每只股票的代码和名称保存到 col_basic_info 集合中
        for idx in range(len(stock_symbols)):
            stock_dict={"name": stock_names[idx], "symbol": stock_symbols[idx]}
            self.col_basic_info.insert_one(stock_dict)

    # 获取历史数据
    def get_historical_news(self):
        # 从MongoDB中获取symbol字段所有值
        stock_symbol_list = self.col_basic_info.distinct("symbol")
            
        # 如果没有值就去获取最新的
        if len(stock_symbol_list) == 0:
            self.get_stock_info()
            stock_symbol_list = self.col_basic_info.distinct("symbol")
                
        # 遍历stock_symbol_list获取每个symbol对应的历史价格数据
        for idx in range(len(stock_symbol_list)):
            symbol=stock_symbol_list[idx]
            # latest_date=self.redis_client.get(symbol)
            latest_date = None
            # 如果该symbol无历史数据则设置为默认值
            if latest_date is None:
                latest_date=config.STOCK_PRICE_REQUEST_DEFAULT_DATE
            else:
                latest_date=latest_date.decode()
            # 获取离当天最近的工作日期
            recent_weekday_date=utils.get_recent_weekday()
            # 如果最新更新日期早于最新工作日期就需要调用api为symbol补充历史数据
            if(latest_date>=recent_weekday_date):
                continue
            # 获取latest_date的日期对象
            latest_date_obj = datetime.strptime(latest_date, '%Y-%m-%d')
            # 获取symbol对应的股票的历史价格数据
            stock_daily_df = ak.stock_zh_a_daily(symbol=symbol,adjust="qfq")
            # 更改索引,便于遍历
            stock_daily_df.insert(0, 'date', stock_daily_df.index.tolist())
            stock_daily_df.index = range(len(stock_daily_df))
            # 得到对应的价格集合
            symbol_price = self.db_obj.get_collection(self.database_name, symbol+"_price")
            # 将每条价格数据插入到对应的集合中
            for _idx in range(stock_daily_df.shape[0]):
                tmp_dict = stock_daily_df.iloc[_idx].to_dict()
                tmp_date=tmp_dict["date"]
                tmp_date=tmp_date.strftime('%Y-%m-%d')
                tmp_date_obj=datetime.strptime(tmp_date, '%Y-%m-%d')
                # 如果当前价格日期早于latest_date_obj就跳过这条数据
                if(tmp_date_obj<=latest_date_obj):
                    continue
                tmp_dict.pop("outstanding_share")
                tmp_dict.pop("turnover")
                # 将该条数据插入到对应的集合中去
                symbol_price.insert_one(tmp_dict)
                # 打印信息
                # logging.info("{} success to insert into {} ... ".format(tmp_dict,symbol+"price"))
                # 更新symbol对应的latest_date
                # self.redis_client.set(symbol, str(tmp_dict["date"]).split(" ")[0])
            # 打印信息
            logging.info("{} finished saving from {} to {} ... ".format(symbol, datetime.strftime(latest_date_obj+timedelta(days=1), '%Y-%m-%d'), recent_weekday_date))
            # 防止频繁调用api封禁ip
            time.sleep(config.STOCK_PRICE_API_SLEEP_TIME)

    # 获取实时数据
    def get_realtime_news(self):
        # 对所有股票补充数据到最新
        self.get_historical_news()
        
        # 打印完成获取历史数据信息
        logging.info("finished getting historical news ... ")
        
        # 不断获取新数据
        while True:
            time_now = datetime.now().strftime("%H:%M:%S")
            date_now = datetime.now().strftime("%Y-%m-%d")
            # 如果过了凌晨
            # if date_now != self.redis_client.get("today_date").decode():
            #     self.redis_client.set("today_date", date_now)
            #     # 该参数设置回空值，表示今天未进行数据更新
            #     self.redis_client.set("is_today_updated", "")
            # 如果今天还没有更新
            # if not bool(self.redis_client.get("is_today_updated").decode()):
            if True:
                # 设定更新时间，只有达到更新时间才会去更新
                update_time = config.STOCK_PRICE_UPDATE_TIME
                # 如果可以更新
                if time_now >= update_time:
                    # 当天的日数据行情下载
                    stock_zh_a_spot_df = ak.stock_zh_a_spot() 
                    for idx, symbol in enumerate(stock_zh_a_spot_df["symbol"]):
                        # 从数据库中获取symbol_price集合
                        symbol_price_collection = self.db_obj.get_collection(self.database_name, symbol+"_price")
                        tmp_dict = {}
                        tmp_dict.update({"date": Timestamp("{}".format(date_now))})
                        # 将当前股票的开盘价添加到字典中
                        tmp_dict.update({"open": stock_zh_a_spot_df.iloc[idx]["open"]})
                        # 将当前股票的最高价添加到字典中
                        tmp_dict.update({"high": stock_zh_a_spot_df.iloc[idx]["high"]})
                        # 将当前股票的最低价添加到字典中
                        tmp_dict.update({"low": stock_zh_a_spot_df.iloc[idx]["low"]})
                        # 将当前股票的收盘价
                        tmp_dict.update({"close": stock_zh_a_spot_df.iloc[idx]["trade"]})
                        # 将当前股票的成交量添加到字典中
                        tmp_dict.update({"volume": stock_zh_a_spot_df.iloc[idx]["volume"]})
                        # 将包含股票数据的字典插入到数据库中
                        symbol_price_collection.insert_one(tmp_dict)
                        # self.redis_client.set(symbol, date_now)
                        logging.info("finished updating {} price data of {} ... ".format(symbol, date_now))
                    # self.redis_client.set("is_today_updated", "1")
