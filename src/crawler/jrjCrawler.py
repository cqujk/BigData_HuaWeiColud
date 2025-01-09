"""
金融界：https://stock.jrj.com.cn/
"""

import __init__

from baseCrawler import Crawler
from common import utils
from common import config
from common.database import Database
import re
import time
from datetime import datetime
import json
# import redis
import random
import logging
import threading
from bs4 import BeautifulSoup

# 导入webdriver用来模拟用户操作浏览器
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# from kafka import KafkaProducer


logging.basicConfig(
    level=logging.INFO,
    filename="./logs/jrj_news_output.log",
    filemode='w',  # 使用写模式覆盖日志
    format="%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s",
    datefmt="%a, %d %b %Y %H:%M:%S"
)


class JrjCrawler(Crawler):

    def __init__(self, database_name, collection_name):
        super(JrjCrawler, self).__init__()
        self.db_obj = Database()
        self.col_news = self.db_obj.get_collection(database_name, collection_name)
        self.database_name = database_name
        self.collection_name = collection_name
        self.url=config.JRJ_URL
        # redis客户端配置
        # self.redis_client = redis.StrictRedis(host=config.REDIS_IP,port=config.REDIS_PORT,db=config.REDIS_CLIENT_FOR_CACHING_JRJ_DB_ID,password=config.REDIS_PASSWORD)
        # 代理池
        # self.proxy_pool = utils.get_proxy_pool

    # 获取历史新闻
    def get_historical_news(self):
        # 获取redis中jrj_latest_date属性的值,表示jrj新闻数据库最新更新日期
        # latest_time=self.redis_client.get("jrj_latest_time")
        latest_time = None
        
        # 如果为空就使用默认值,否则解码
        if latest_time is None:
            latest_time=config.JRJ_REQUEST_DEFAULT_TIME
        else:
            latest_time=latest_time.decode()
            
        # 随机选择一个代理
        # proxy = random.choice(self.proxy_pool)
            
        # 配置 ChromeOptions
        options = Options()
        options.add_argument('--headless')
        options.add_argument("--no-sandbox")
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument("--blink-settings=imagesEnabled=false")  # 禁用图片加载
        options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.5249.103 Safari/537.36')
        # options.add_argument(f'--proxy-server={"188.166.30.17:8888"}')  # 设置代理

        # 创建 ChromeDriver 实例
        driver = webdriver.Chrome(executable_path=ChromeDriverManager().install(), options=options)
        driver.get(self.url)
        
        # 记录点击按钮次数
        cnt=0
        
        # 记录当前页面最新新闻的日期
        new_latest_time=""
        
        # 加载新闻
        while True:
            try:
                # 等待"加载更多"按钮可点击
                load_more_btn = WebDriverWait(driver, 100).until(
                    EC.element_to_be_clickable((By.ID, "awwmore"))
                )
                driver.execute_script("arguments[0].click();", load_more_btn)
                # 更新点击次数
                cnt = cnt + 1
                # 打印日志
                logging.info("current clicked count: {}".format(cnt))

            except TimeoutException:
                logging.info("加载更多按钮超时，可能是页面未完全加载或按钮不存在")
                # 在这里可以处理页面加载失败的情况，例如跳过当前操作或重新加载页面
            except NoSuchElementException:
                logging.info("未找到加载更多按钮")
                # 如果没有找到按钮，可以考虑终止抓取或者进行其他操作
            except ElementNotInteractableException:
                logging.info("加载更多按钮无法交互")
            
            # 判断是否到底或者到达最大需求数据量
            no_more_btn = driver.find_element(By.ID, "isend")
            if "已经到底了" in no_more_btn.text or 20 * cnt > config.JRJ_REQUEST_MAX_COUNT:
                logging.info("finish loading all news")
                break
        
        # 使用 BeautifulSoup 解析页面内容
        bs = BeautifulSoup(driver.page_source, "html.parser")
        
        # 创建kafka生产者
        # kafka_producer = KafkaProducer(
        #     bootstrap_servers = config.KAFKA_IP_PORT, # kafka服务器地址
        #     value_serializer=lambda v: json.dumps(v).encode('utf-8')
        # )
        
        # 获取每条新闻
        for news_item in bs.find_all("a", attrs={"class": "news_item"}):
            # 获取url
            url=news_item["href"]
            # 获取新闻时间
            news_time=utils.get_url_time(url)
            # 如果当前新闻时间早于最新更新时间就停止传url
            if (news_time and news_time<=latest_time):
                break
            # 更新当前页面最新新闻的日期
            if(new_latest_time=="" and news_time):
                new_latest_time=news_time
            # 获取新闻内容
            news_article = utils.get_url_content(url)
            # 将 news_time 和 url 放入字典中
            news_dict={
                "news_time": news_time,
                "news_url": url,
                "news_article": news_article
            }
            # 发送到 Kafka
            # try:
            #     future=kafka_producer.send(config.JRJ_URL_TOPIC, value=news_dict)
            #     logging.info("success to send message: {}".format(json.dumps(news_dict)))
            # except KafkaError as e:
            #     logging.error(e)
            # 存放到MongoDB中
            self.col_news.insert_one(news_dict)
        
        # 更新jrj新闻数据库最新更新日期
        # self.redis_client.set("jrj_latest_time",new_latest_time)
        
        driver.quit()
                
    # 获取实时新闻
    def get_realtime_news(self):
        # 获取历史新闻
        self.get_historical_news()
        logging.info("finish getting historical news then sleep {} seconds".format(config.JRJ_UPDATE_INTERVAL))
        
        # 获取实时新闻
        while True:
            time.sleep(config.JRJ_UPDATE_INTERVAL)

            # 获取redis中jrj_latest_date属性的值,表示jrj新闻数据库最新更新日期
            # latest_time=self.redis_client.get("jrj_latest_time").decode()
            latest_time = None
            
            # 如果为空就使用默认值
            if latest_time is None:
                latest_time=config.JRJ_REQUEST_DEFAULT_TIME
                
            # 随机选择一个代理
            # proxy = random.choice(self.proxy_pool)
                
            # 配置 ChromeOptions
            options = Options()
            options.add_argument('--headless')
            options.add_argument("--no-sandbox")
            options.add_argument('--disable-gpu')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument("--blink-settings=imagesEnabled=false")  # 禁用图片加载
            options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.5249.103 Safari/537.36')
            # options.add_argument(f'--proxy-server={proxy}')  # 设置代理
            
            # 创建 ChromeDriver 实例
            driver = webdriver.Chrome(executable_path=ChromeDriverManager().install(), options=options)
            driver.get(self.url)
            
            # 记录点击按钮次数
            cnt=0
            
            # 记录当前页面最新新闻的日期
            new_latest_time=""
            
            # 加载新闻
            while True:
                try:
                    # 等待"加载更多"按钮可点击
                    load_more_btn = WebDriverWait(driver, 100).until(
                        EC.element_to_be_clickable((By.ID, "awwmore"))
                    )
                    driver.execute_script("arguments[0].click();", load_more_btn)
                    # 更新点击次数
                    cnt = cnt + 1
                    # 打印日志
                    logging.info("current clicked count: {}".format(cnt))

                except TimeoutException:
                    logging.info("加载更多按钮超时，可能是页面未完全加载或按钮不存在")
                    # 在这里可以处理页面加载失败的情况，例如跳过当前操作或重新加载页面
                except NoSuchElementException:
                    logging.info("未找到加载更多按钮")
                    # 如果没有找到按钮，可以考虑终止抓取或者进行其他操作
                except ElementNotInteractableException:
                    logging.info("加载更多按钮无法交互")
                    
                # 判断是否到底或者到达最大需求数据量
                no_more_btn = driver.find_element(By.ID, "isend")
                if "已经到底了" in no_more_btn.text or 20 * cnt > config.JRJ_REQUEST_MAX_COUNT:
                    logging.info("finish loading all news")
                    break
            
            # 使用 BeautifulSoup 解析页面内容
            bs = BeautifulSoup(driver.page_source, "html.parser")
            
            # 创建kafka生产者
            # kafka_producer = KafkaProducer(
            #     bootstrap_servers = config.KAFKA_IP_PORT, # kafka服务器地址
            #     value_serializer=lambda v: json.dumps(v).encode('utf-8')
            # )
            
            # 获取每条新闻
            for news_item in bs.find_all("a", attrs={"class": "news_item"}):
                # 获取url
                url=news_item["href"]
                # 获取新闻时间
                news_time=utils.get_url_time(url)
                # 如果当前新闻时间早于最新更新时间就停止传url
                if (news_time and news_time<=latest_time):
                    break
                # 更新当前页面最新新闻的日期
                if(new_latest_time=="" and news_time):
                    new_latest_time=news_time
                # 获取新闻内容
                news_article = utils.get_url_content(url)
                # 将 news_time 和 url 放入字典中
                news_dict={
                    "news_time": news_time,
                    "news_url": url,
                    "news_article": news_article
                }
                # 发送到 Kafka
                # try:
                #     future=kafka_producer.send(config.JRJ_URL_TOPIC, value=news_dict)
                #     logging.info("success to send message: {}".format(json.dumps(news_dict)))
                # except KafkaError as e:
                #     logging.error(e)
                # 存放到MongoDB中
                self.col_news.insert_one(news_dict)
            
            # 更新jrj新闻数据库最新更新日期
            # self.redis_client.set("jrj_latest_time",new_latest_time)
            
            driver.quit()
            
            logging.info("finish getting realtime news then sleep {} seconds".format(config.JRJ_UPDATE_INTERVAL))
        
