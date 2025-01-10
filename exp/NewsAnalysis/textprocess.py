import jieba
from gensim import corpora, models
import pandas as pd
import numpy as np

class TextProcessing:
    def __init__(self, stopwords_path, finance_dict_path):
        self.stopwords_path = stopwords_path
        self.finance_dict_path = finance_dict_path

    def load_stopwords(self):
        with open(self.stopwords_path, 'r', encoding='gbk') as f:
            return [line.strip() for line in f.readlines()]

    def load_finance_dict(self):
        with open(self.finance_dict_path, 'r', encoding='utf-8') as f:
            for word in f:
                jieba.add_word(word.strip())

    def tokenize_documents(self, documents):
        stopwords = self.load_stopwords()
        return [[word for word in jieba.cut(doc) if word not in stopwords and word.strip()] for doc in documents]

    def generate_dictionary_and_vectors(self, documents):
        tokenized_documents = self.tokenize_documents(documents)
        dictionary = corpora.Dictionary(tokenized_documents)
        bow_vectors = [dictionary.doc2bow(doc) for doc in tokenized_documents]
        return dictionary, bow_vectors
    
    def create_lda_vectors(self, dictionary, bow_vectors, num_topics=10):  
        # 创建 LDA 模型  
        lda_model = models.LdaModel(bow_vectors, id2word=dictionary, num_topics=num_topics)  

        lda_vectors = []  
        for doc in bow_vectors:  
            # 获取文档的主题分布，转换为字典形式  
            topic_distribution = lda_model[doc]  
            # 创建一个长度为 num_topics 的零向量  
            lda_vector = [float(0)] * num_topics  

            # 填充主题分布  
            for topic_id, prob in topic_distribution:  
                lda_vector[topic_id] = prob  # 将对应主题的概率填入  

            lda_vectors.append(lda_vector)  

        return lda_vectors

    def judge_news_sentiment(self, news_dates, stock_data_path, judge_term=15):
        # 读取股票价格数据
        stock_data = pd.read_csv(stock_data_path, parse_dates=["date"])
        stock_data["date"] = stock_data["date"].dt.tz_localize(None)  # 将 date 列转换为 tz-naive
        stock_data = stock_data.sort_values(by="date")

        # 计算对数收益率
        stock_data["log_return"] = np.log(stock_data["close"].pct_change() + 1)

        labels = []
        for news_date in news_dates:
            news_date = pd.to_datetime(news_date)  # news_date 保持 tz-naive

            # 找到新闻日期之前的历史收益和波动性
            historical_data = stock_data[stock_data["date"] < news_date]
            if len(historical_data) < 2:
                labels.append("中立")
                continue

            historical_returns = historical_data["log_return"].dropna()
            mean_return = historical_returns.mean()
            std_return = historical_returns.std()

            # 找到新闻日期之后的judge_term天收益
            future_data = stock_data[(stock_data["date"] >= news_date) & (stock_data["date"] < news_date + pd.Timedelta(days=judge_term))]
            if len(future_data) < 2:
                labels.append(0)#中立
                continue

            future_return = np.log(future_data["close"].iloc[-1] / future_data["close"].iloc[0])

            # 根据收益判断情感
            if future_return > mean_return + std_return:
                labels.append(1)#利好
            else:
                labels.append(0)#"中立"

        return labels

if __name__ == "__main__":
    # 加载新闻数据和股票数据
    news_data_path = "stock_news.csv" 
    stock_data_path = "stock_prices.csv" 

    news_df = pd.read_csv(news_data_path)
    news_dates = news_df["news_time"].tolist()
    news_contents = news_df["news_article"].tolist()

    # 新闻数据处理
    processor = TextProcessing(stopwords_path="Chinese_Stop_Words.txt", finance_dict_path="finance_dict.txt")
    processor.load_finance_dict()

    dictionary, bow_vectors = processor.generate_dictionary_and_vectors(news_contents)
    lda_vectors = processor.create_lda_vectors(dictionary, bow_vectors, num_topics=10)

    # 给新闻打标签
    labels = processor.judge_news_sentiment(news_dates, stock_data_path, judge_term=15)
    print(labels)