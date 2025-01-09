import pandas as pd
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LSTMRegressor
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import RegressionEvaluator
import matplotlib.pyplot as plt
from pyspark.sql.functions import col, year, month, dayofmonth

#实验包中已提供一个示例数据集，你可能需要根据实际情况调整路径和格式相关参数
file_path = 'exp/sh600446_price.csv.csv'
# 创建SparkSession
spark = SparkSession.builder.appName("StockPricePrediction").getOrCreate()

# 1. 原始数据的获取
def load_data(file_path):
    """
    从文件路径加载数据
    :param file_path: 数据文件路径
    :return: Spark DataFrame
    """
    data = pd.read_csv(file_path)
    spark_df = spark.createDataFrame(data)
    return spark_df

# 2. 数据预处理
def preprocess_data(spark_df):
    """
    对数据进行预处理
    :param spark_df: 原始Spark DataFrame
    :return: 预处理后的Spark DataFrame
    """
    numeric_cols = ["open", "high", "low", "close", "volume"]
    assembler = VectorAssembler(inputCols=numeric_cols, outputCol="features_vector")
    spark_df = assembler.transform(spark_df)

    spark_df = spark_df.withColumn("year", year(col("date")))
    spark_df = spark_df.withColumn("month", month(col("date")))
    spark_df = spark_df.withColumn("day", dayofmonth(col("date")))

    scaler = StandardScaler(inputCol="features_vector", outputCol="scaled_features", withStd=True, withMean=True)
    scaler_model = scaler.fit(spark_df)
    spark_df = scaler_model.transform(spark_df)

    # 创建其他可能有用的特征（自定义特征工程）
    spark_df = spark_df.withColumn("price_range", col("high") - col("low"))
    spark_df = spark_df.withColumn("price_change_percentage", (col("close") - col("open")) / col("open"))

    # 选择特征列并组装最终用于模型的输入特征向量
    final_feature_cols = ["scaled_features", "price_range", "price_change_percentage", "year", "month", "day"]
    assembler_final = VectorAssembler(inputCols=final_feature_cols, outputCol="final_features")
    spark_df = assembler_final.transform(spark_df)

    train_df, val_df, test_df = spark_df.randomSplit([0.7, 0.15, 0.15], seed=42)
    return train_df, val_df, test_df, final_feature_cols
# 3. 模型训练
def train_model(train_df, val_df, final_feature_cols):
    """
    训练模型
    :param train_df: 训练数据集
    :param val_df: 验证数据集
    :param final_feature_cols: 最终特征列
    :return: 训练好的模型
    """
    input_size = len(final_feature_cols)  # 根据最终特征向量的维度确定输入大小
    hidden_size = 64
    num_layers = 2
    output_size = 1  # 预测股票价格，输出一个值

    lstm = LSTMRegressor(inputSize=input_size, hiddenSize=hidden_size, numLayers=num_layers, outputSize=output_size)

    paramGrid = ParamGridBuilder() \
        .addGrid(lstm.learningRate, [0.01, 0.001]) \
        .addGrid(lstm.maxIter, [100, 200]) \
        .build()

    evaluator = RegressionEvaluator(labelCol="close", predictionCol="prediction", metricName="rmse")

    crossval = CrossValidator(estimator=lstm,
                              estimatorParamMaps=paramGrid,
                              evaluator=evaluator,
                              numFolds=3)
    model = crossval.fit(train_df)
    return model

# 4. 测试结果与可视化
def evaluate_model(model, test_df):
    """
    评估模型并可视化结果
    :param model: 训练好的模型
    :param test_df: 测试数据集
    """
    predictions = model.transform(test_df)
    rmse = evaluator.evaluate(predictions)
    print("测试集均方根误差（RMSE）:", rmse)

    # 获取真实价格和预测价格数据，转换为Pandas DataFrame方便可视化
    true_prices = predictions.select("close").toPandas()
    predicted_prices = predictions.select("prediction").toPandas()

    # 绘制对比图
    plt.plot(true_prices.index, true_prices['close'], label='True Prices')
    plt.plot(predicted_prices.index, predicted_prices['prediction'], label='Predicted Prices', alpha=0.7)
    plt.xlabel('Index')
    plt.ylabel('Stock Price')
    plt.title('Stock Price Prediction Comparison')
    plt.legend()
    plt.show()

# 主函数
def main():
    spark_df = load_data(file_path)
    train_df, val_df, test_df, final_feature_cols = preprocess_data(spark_df)
    model = train_model(train_df, val_df, final_feature_cols)
    evaluate_model(model, test_df)


if __name__ == "__main__":
    main()