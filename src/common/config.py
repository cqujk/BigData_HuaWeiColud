# MongoDB
MONGO_CLIENT = "mongodb://user:cywxbszdjk123@1.92.123.158:27777/admin"

# redis
REDIS_IP = "121.36.226.164"
REDIS_PORT = 8848
REDIS_CLIENT_FOR_CACHING_STOCK_INFO_DB_ID = 1
REDIS_CLIENT_FOR_CACHING_JRJ_DB_ID = 1
REDIS_PASSWORD = "szd123456"

# kafka
KAFKA_IP_PORT = "localhost:9092"

# thread
THREAD_NUMS_FOR_SPYDER = 4

# jrjCrawler
JRJ_COLLECTION_NAME = "jrj_news"
JRJ_URL = "https://stock.jrj.com.cn/"
JRJ_MAX_REJECTED_AMOUNTS = 5
JRJ_REQUEST_DEFAULT_TIME = "2024-10-31 23:59"
JRJ_UPDATE_INTERVAL = 3600
JRJ_URL_TOPIC = "jrj_url_topic"
JRJ_REQUEST_MAX_COUNT = 10000

# nbdCrawler
NBD_COLLECTION_NAME = "nbd_news"
NBD_URL = "https://www.nbd.com.cn/columns/3/"
NBD_REQUEST_DEFAULT_TIME = "2024-10-31 23:59"
NBD_TOTAL_PAGES_NUM = 684
NBD_MAX_REJECTED_AMOUNTS = 10
CACHE_SAVED_NEWS_NBD_TODAY_VAR_NAME = "cache_news_queue_nbd"

# news_api
NEWS_API_KEY = "fb54346985124b179fdb5b349db72268"

TUSHARE_TOKEN = "381eccc1bb8e5d7dc5d63eebb7fa2d7ba9de5a9e38d3a2c877da8885"

# stockInfoCrawler
STOCK_BASIC_INFO_COLLECTION_NAME = "basic_info"
STOCK_PRICE_REQUEST_DEFAULT_DATE = "2019-12-31"
STOCK_PRICE_UPDATE_TIME= "15:30:00"
STOCK_PRICE_API_SLEEP_TIME= 2

ALL_NEWS_OF_SPECIFIC_STOCK_DATABASE = "stocknews"

TOPIC_NUMBER = 200
SVM_TUNED_PARAMTERS = {"kernel": ["rbf"], "gamma": [10, 20, 50, 100, 150, 200], "C": [10, 15, 20, 30, 50, 100]}
RDFOREST_TUNED_PARAMTERS = {"n_estimators": [1, 2, 3, 4, 5, 10],
                            "criterion": ["gini", "entropy"],
                            "max_features": ["auto", "sqrt"]}
CLASSIFIER_SCORE_LIST = ["f1_weighted"]
USER_DEFINED_DICT_PATH = "D:/workfiles/gpu-cloud-backup/Listed-company-news-crawl-and-text-analysis/src/Leorio/financedict.txt"
CHN_STOP_WORDS_PATH = "D:/workfiles/gpu-cloud-backup/Listed-company-news-crawl-and-text-analysis/src/Leorio/chnstopwords.txt"

CACHE_NEWS_REDIS_DB_ID = 0
CACHE_NEWS_LIST_NAME = "cache_news_waiting_for_classification"

CACHE_RECORED_OPENED_PYTHON_PROGRAM_DB_ID = 0
CACHE_RECORED_OPENED_PYTHON_PROGRAM_VAR = "opened_python_scripts"

MINIMUM_STOCK_NEWS_NUM_FOR_ML = 1000