"""
金融界：https://stock.jrj.com.cn/
"""

import __init__

from jrjCrawler import JrjCrawler
import config
# from kafka import KafkaProducer
import json
import utils
from bs4 import BeautifulSoup


if __name__ == '__main__':
    jrjCrawler=JrjCrawler("news","all_news")
    jrjCrawler.get_realtime_news()
    
    """ bs = utils.html_parser("https://24h.jrj.com.cn/2024/11/18200745411985.shtml")
    # 获取新闻时间
    for span in bs.find_all("span"):
        if span.contents and span.contents[1] == "jrj_final_date_start":
            print(str(span.contents[2]).replace("\r", "").replace("\n", ""))
            # time = span.centents[2].replace("\r", "").replace("\n", "")
            break """
    
    """ kafka_producer = KafkaProducer(
        bootstrap_servers = config.KAFKA_IP_PORT, # kafka服务器地址
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    try:
        news_dict={
            "news_time": "2024-11-18 15:30",
            "news_url": "https://stock.jrj.com.cn/2024/11/18153045397854.shtml"
        }
        future = kafka_producer.send(config.JRJ_URL_TOPIC, value=news_dict)
        record_metadata = future.get(timeout=10)  # 等待消息发送完成，设置超时时间
        print(record_metadata.topic)
        print(record_metadata.partition)
        print(record_metadata.offset)
    except KafkaError as e:
        print(e) """