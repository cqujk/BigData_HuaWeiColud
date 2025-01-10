from pyspark.sql import SparkSession
from pyspark.ml.classification import LinearSVC
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pymongo import MongoClient

class ClassifySpark(object):
    def __init__(self, stockCode, hdfs_host="your_hdfs_host"):
        self.hdfs_host = hdfs_host
        self.spark = SparkSession.builder \
            .appName("StockNewsSVM") \
            .config("spark.master", "yarn") \
            .getOrCreate()

        self.stockCode = stockCode
        self.dict_vec_path = f"{hdfs_host}/allstocks_file/{stockCode}"

        # MongoDB client 
        self.mongo_client = MongoClient("your_mongo_url")
        self.db = self.mongo_client["stock"]
        
    '''由于云主机资源不足，无法直接从MongoDB中获取标签，可将标签保存至文件，读取本地文件作为输入'''
    def load_labels_from_mongodb(self, stock_news_collection):
        """从MOngoDB中加载标签"""
        collection = self.db[stock_news_collection]
        labels = []
        cursor = collection.find({}, {"Character": 1})
        for doc in cursor:
            if doc["Character"] == "利好":
                labels.append(1)
            elif doc["Character"] == "利空":
                labels.append(-1)
            else:
                labels.append(0)
        return labels

    def call_transformation_model(self, tp_instance, dictionary, bowvec):
        """获取LDA模型"""
        tfidf_vec, lda_vec = tp_instance.CallTransformationModel(
            Dict=dictionary,
            Bowvec=bowvec,
            modelType="lda",
            tfDim=100,
            renewModel=False,
            modelPath=self.dict_vec_path
        )
        return lda_vec

    def preprocess_data(self, data):
        """Prepare data for Spark ML pipeline."""
        # 将多个特征列组合成一个单一的向量列
        assembler = VectorAssembler(inputCols=[col for col in data.columns if col != "label"], outputCol="features")
        assembled_data = assembler.transform(data)

        # 缩放特征
        scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures", withMean=False, withStd=True)
        scaled_data = scaler.fit(assembled_data).transform(assembled_data)

        return scaled_data.select("scaledFeatures", "label")

    def train_svm(self, data):
        # 分割数据为训练集和测试集
        train_data, test_data = data.randomSplit([0.8, 0.2], seed=1234)

        svm = LinearSVC(featuresCol="scaledFeatures", labelCol="label", maxIter=100)

        # 定义参数
        param_grid = ParamGridBuilder() \
            .addGrid(svm.regParam, [0.1, 0.01, 0.001]) \
            .build()

        # 定义评估器
        evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")

        # 交叉验证器
        crossval = CrossValidator(estimator=svm,
                                   estimatorParamMaps=param_grid,
                                   evaluator=evaluator,
                                   numFolds=5)

        # 训练模型
        cv_model = crossval.fit(train_data)

        # 在测试集上评估模型
        predictions = cv_model.transform(test_data)
        accuracy = evaluator.evaluate(predictions)
        print(f"Test Accuracy: {accuracy}")

        # 保存模型到 HDFS
        model_path = f"{self.dict_vec_path}/svm_model"
        cv_model.bestModel.save(model_path)
        print(f"Model saved to {model_path}")

        return cv_model.bestModel

    def classify(self, tp_instance, stock_news_collection):
        """End-to-end classification pipeline."""
        # 从MongoDB加载标签（该云主机下不可用）
        labels = self.load_labels_from_mongodb(stock_news_collection)
        '''这里由于云主机资源不足，不支持直接从MOngoDB中获取，可把label保存至文件，读取本地文件作为输入'''
        
        # 得到LDA向量
        dictionary = tp_instance._dictionary
        bowvec = tp_instance._BowVecOfEachDoc
        lda_vec = self.call_transformation_model(tp_instance, dictionary, bowvec)

        # 将LDA向量和标签转换为 Spark 的 DataFrame 形式
        data = self.spark.createDataFrame([
            (float(label), list(vec)) for label, vec in zip(labels, lda_vec)
        ], schema=["label", "features"])

        # 对特征进行处理
        prepared_data = self.preprocess_data(data)

        #训练和评估
        best_model = self.train_svm(prepared_data)

        return best_model

if __name__ == "__main__":
    from Textprocessing import TextProcessing

    # 示例
    stockCode = "600740"
    classifier = ClassifySpark(stockCode)

    # 初始化
    tp = TextProcessing(
        hdfs_url="your_hdfs_url",
        chnSTWPath="your_chnSTWPath",
        finance_dict_path="your_finance_dict_path"
    )

    # 训练
    stock_news_collection = f"{stockCode}_news"
    classifier.classify(tp, stock_news_collection)
