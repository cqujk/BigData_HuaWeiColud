import pandas as pd  
import numpy as np  
from pyspark.sql import SparkSession  
from pyspark.ml.classification import OneVsRest, LinearSVC  
from pyspark.ml.feature import StandardScaler  
from pyspark.ml.evaluation import MulticlassClassificationEvaluator  
from pyspark.sql.types import StructType, StructField, FloatType  
from pyspark.ml.linalg import Vectors, VectorUDT  

class NewsClassifier:  
   def __init__(self):  
      self.spark = SparkSession.builder \
         .appName("Classifier") \
         .master("local[*]") \
         .getOrCreate()  

   def preprocess_data(self, features, labels):
      # 确保 features 是一个二维数组
      features = np.array(features, dtype=object)
      # 确保 features 中的每个元素都是 Python 的 float 类型
      features = [[float(value) for value in feature] for feature in features]
      # 确保 labels 中的每个元素都是 Python 的 float 类型
      labels = [float(label) for label in labels]
      # 创建 Spark DataFrame，构造特征列
      data = self.spark.createDataFrame(
         [(float(label), Vectors.dense(feature)) for label, feature in zip(labels, features)],
         schema=StructType([
            StructField("label", FloatType(), True),         # 标签列是 Float 类型
            StructField("features", VectorUDT(), True)      # 特征列是 VectorUDT 类型
         ])
      )
      # 使用标准化对 features 进行处理
      scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures", withMean=True, withStd=True)
      scaler_model = scaler.fit(data)                # 训练标准化模型
      scaled_data = scaler_model.transform(data)     # 转换数据
      # 选择所需的列，确保返回 scaledFeatures 和 label
      return scaled_data.select("scaledFeatures", "label")  # 确保选中缩放后的特征和标签 

   def train_svm(self, data):  
      train_data, test_data = data.randomSplit([0.8, 0.2], seed=1234)  

      # 创建 LinearSVC 模型  
      svm = LinearSVC(featuresCol="scaledFeatures", labelCol="label", maxIter=100)  
      
      # 训练模型  
      model = svm.fit(train_data) 

      # 进行预测  
      predictions = model.transform(test_data)  

      # 评估模型  
      evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",  
         metricName="accuracy")  
      accuracy = evaluator.evaluate(predictions)  
      print(f"Test Accuracy: {accuracy:.2f}")  # 打印测试准确度  

      # 输出预测结果  
      predictions.select("label", "prediction").show(10)  # 显示前10条预测结果  

      return model 

if __name__ == "__main__":  
   from textprocess import TextProcessing
   # 加载新闻数据和股票数据  
   #实际替换为你的数据路径
   news_data_path = "/home/developer/new2/BigData_HuaWeiColud/exp/NewsAnalysis/stock_news.csv"  
   stock_data_path = "/home/developer/new2/BigData_HuaWeiColud/exp/NewsAnalysis/stock_prices.csv"  

   news_df = pd.read_csv(news_data_path)
   news_dates = news_df["news_time"].tolist()  
   news_contents = news_df["news_article"].tolist()  

   # 新闻数据处理  
   processor = TextProcessing(stopwords_path="/home/developer/new2/BigData_HuaWeiColud/exp/NewsAnalysis/Chinese_Stop_Words.txt", finance_dict_path="/home/developer/new2/BigData_HuaWeiColud/exp/NewsAnalysis/finance_dict.txt")  
   processor.load_finance_dict()  

   dictionary, bow_vectors = processor.generate_dictionary_and_vectors(news_contents)  
   lda_vectors = processor.create_lda_vectors(dictionary, bow_vectors, num_topics=10)  

   # 给新闻打标签  
   labels = processor.judge_news_sentiment(news_dates, stock_data_path, judge_term=15)  

   # 分类  
   classifier = NewsClassifier()  
   prepared_data = classifier.preprocess_data(lda_vectors, labels)  
   classifier.train_svm(prepared_data)