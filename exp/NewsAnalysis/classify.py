import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.ml.classification import LinearSVC
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from textprocess import TextProcessing

class NewsClassifier:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("NewsClassifier") \
            .master("local[*]") \
            .getOrCreate()

    def preprocess_data(self, features, labels):
        data = self.spark.createDataFrame([(float(label), list(feature)) for label, feature in zip(labels, features)], schema=["label", "features"])
        assembler = VectorAssembler(inputCols=["features"], outputCol="assembledFeatures")
        assembled_data = assembler.transform(data)

        scaler = StandardScaler(inputCol="assembledFeatures", outputCol="scaledFeatures")
        scaled_data = scaler.fit(assembled_data).transform(assembled_data)

        return scaled_data.select("scaledFeatures", "label")

    def train_svm(self, data):
        train_data, test_data = data.randomSplit([0.8, 0.2], seed=1234)
        
        svm = LinearSVC(featuresCol="scaledFeatures", labelCol="label", maxIter=100)
        model = svm.fit(train_data)

        predictions = model.transform(test_data)
        evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
        accuracy = evaluator.evaluate(predictions)
        print(f"Test Accuracy: {accuracy}")

        return model

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

    # 分类
    classifier = NewsClassifier()
    prepared_data = classifier.preprocess_data(lda_vectors, labels)
    classifier.train_svm(prepared_data)
