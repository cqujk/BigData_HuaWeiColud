'''
from pyspark.sql import SparkSession
import json
import pymongo
import logging

logging.basicConfig(
    level=logging.INFO,
    filename="./logs/sparkstreaming.log",
    filemode='w',  # 使用写模式覆盖日志
    format="%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s",
    datefmt="%a, %d %b %Y %H:%M:%S"
)

# 创建 Spark 会话
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .getOrCreate()
# Kafka 配置
kafka_broker = "localhost:9092"  # Kafka 服务器地址
topic = "jrj_news_topic"  # 要订阅的 Kafka 主题
# 从 Kafka 读取数据
kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", topic) \
    .option("checkpointLocation", "/logs/kafka-logs/checkpoint/") \
    .load()
# 将 Kafka 消息的值转换为字符串
kafka_value_df = kafka_stream_df.selectExpr("CAST(value AS STRING)")
# 处理接收到的数据并插入到 MongoDB 的函数
def process_batch(df):
    # 在此处创建 MongoDB 连接
    mongo_host = "1.92.123.158"
    mongo_port = 27777
    mongo_user = "user"
    mongo_password = "cywxbszdjk123"
    mongo_database = "news"
    mongo_collection = "all_news"
    mongo_uri = f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}:{mongo_port}/admin"
    mongo_client = pymongo.MongoClient(mongo_uri)  # MongoDB 地址和端口
    db = mongo_client['news']
    collection = db['all_news']
    try:
      data = json.loads(df.value)  # 假设每条消息是一个列表，解析为 Python 列表
      if isinstance(data, dict) and 'news_time' in data and 'news_article' in data and "news_title" in data:
        news_article = data['news_article'].replace('\n', '').replace('\r', '')
        document = {
          "news_time": data['news_time'],
          "news_title": data['news_title'],
          "news_article": news_article
        }
        collection.insert_one(document)  # 插入到 MongoDB
        logging.info(f"success to insert data: {document}")
      else:
        logging.info(f"Unexpected data format: {row.value}")
    except Exception as e:
      logging.info(f"Error processing data: {e}, Data: {row.value}")
    mongo_client.close()
# 使用写流将数据插入 MongoDB
query = kafka_value_df.writeStream \
    .foreach(process_batch) \
    .outputMode("append") \
    .start()
# 等待终止
query.awaitTermination()
'''

from pyspark.sql import SparkSession
import json
import pymongo
import logging
from pyspark.sql.functions import expr
from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    filename="./logs/sparkstreaming.log",
    filemode='w',  # 使用写模式覆盖日志
    format="%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s",
    datefmt="%a, %d %b %Y %H:%M:%S"
)

# 创建 Spark 会话
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .getOrCreate()

# Kafka 配置
kafka_broker = "localhost:9092"  # Kafka 服务器地址
topic = "jrj_news_topic"  # 要订阅的 Kafka 主题

# 从 Kafka 读取数据
kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", topic) \
    .option("checkpointLocation", "/logs/kafka-logs/checkpoint/") \
    .load()

# 将 Kafka 消息的值转换为字符串
kafka_value_df = kafka_stream_df.selectExpr("CAST(value AS STRING)", "offset")

# 处理接收到的数据并插入到 MongoDB 的函数
def process_batch(df, epoch_id):
    # 在此处创建 MongoDB 连接
    mongo_host = "1.92.123.158"
    mongo_port = 27777
    mongo_user = "user"
    mongo_password = "cywxbszdjk123"
    mongo_database = "news"
    mongo_collection = "all_news"
    mongo_uri = f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}:{mongo_port}/admin"
    mongo_client = pymongo.MongoClient(mongo_uri)  # MongoDB 地址和端口
    db = mongo_client['news']
    collection = db['all_news']
    
    # 获取当前批次的 Kafka 偏移量
    offsets = df.selectExpr("offset").collect()
    
    try:
        for row in df.collect():
            data = json.loads(row['value'])  # 假设每条消息是一个字典，解析为 Python 字典
            if isinstance(data, dict) and 'news_time' in data and 'news_article' in data and "news_title" in data:
                news_article = data['news_article'].replace('\n', '').replace('\r', '')
                document = {
                    "news_time": data['news_time'],
                    "news_title": data['news_title'],
                    "news_article": news_article
                }
                collection.insert_one(document)  # 插入到 MongoDB
                logging.info(f"Success to insert data: {document}")
            else:
                logging.info(f"Unexpected data format: {row['value']}")
        
        # 手动提交偏移量
        for offset in offsets:
            logging.info(f"Committing offset: {offset}")
            # 此处可以使用 Kafka 的 API 来手动提交偏移量（此部分依赖于实际应用环境）
            # 手动提交偏移量（KafkaConsumer）：
            consumer = KafkaConsumer(topic, group_id='spark-streaming', bootstrap_servers=kafka_broker)
            consumer.commit(offset=offset)  # 提交偏移量
            
    except Exception as e:
        logging.error(f"Error processing data: {e}")
    
    mongo_client.close()

# 使用写流将数据插入 MongoDB，并手动提交 Kafka 偏移量
query = kafka_value_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .start()

# 等待终止
query.awaitTermination()
