import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler
from keras.models import Sequential
from keras.layers import LSTM, Dense
from keras.callbacks import EarlyStopping
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import RegressionEvaluator
import matplotlib.pyplot as plt
from pyspark.sql.functions import col, year, month, dayofmonth

#实验包中已提供一个示例数据集，你可能需要根据实际情况调整路径和格式相关参数
#file_path = 'sh600446_price.csv.csv'
file_path = '/home/developer/new/BigData_HuaWeiColud/exp/sh600446_price.csv'
# 创建SparkSession
spark = SparkSession.builder.appName("StockPricePrediction").getOrCreate()
# 将Spark DataFrame转换为numpy数组格式，便于Keras使用
def to_numpy_array(df,feature_dim):
    features = df.select("final_features").rdd.map(lambda x: x[0].toArray()).collect()
    target = df.select("close").rdd.map(lambda x: x[0]).collect()
    features = np.array(features)
    print("Features shape before reshape:", features.shape)
    # for i in range(min(5, len(features))):
    #     print(f"Element {i} of features: {features[i]}")
    time_steps = 1#features.shape[1]
    num_samples = features.shape[0]
    features = features.reshape(num_samples, time_steps, feature_dim)
    return features, np.array(target).reshape(-1, 1)
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
    # 检查scaled_features维度
    sample_scaled_features = spark_df.select("scaled_features").first()[0]
    print(f"Length of scaled_features: {len(sample_scaled_features)}")
    # 创建其他可能有用的特征（自定义特征工程）
    spark_df = spark_df.withColumn("price_range", col("high") - col("low"))
    spark_df = spark_df.withColumn("price_change_percentage", (col("close") - col("open")) / col("open"))

    # 选择特征列并组装最终用于模型的输入特征向量
    final_feature_cols = ["scaled_features", "price_range", "price_change_percentage"]#, "year", "month", "day"]
    assembler_final = VectorAssembler(inputCols=final_feature_cols, outputCol="final_features")
    spark_df = assembler_final.transform(spark_df)

    train_df, val_df, test_df = spark_df.randomSplit([0.7, 0.15, 0.15], seed=42)
    # 获取特征维度信息，用于后续Keras模型构建
    feature_dim = 7
    return train_df, val_df, test_df, feature_dim
# 3. 模型训练
def train_model(train_df, val_df,test_df, feature_dim):
    """
    使用Keras构建并训练LSTM模型
    :param train_df: 训练数据集
    :param val_df: 验证数据集
    :param test_df: 测试数据集
    :param feature_dim: 特征维度
    :return: 训练好的模型
    """
    X_train, y_train = to_numpy_array(train_df, feature_dim)
    X_val, y_val = to_numpy_array(val_df, feature_dim)
    X_test, y_test = to_numpy_array(test_df, feature_dim)

    model = Sequential()
    model.add(LSTM(64, input_shape=(X_train.shape[1], feature_dim), return_sequences=False))
    model.add(Dense(1))

    model.compile(loss='mean_squared_error', optimizer='adam')

    early_stopping = EarlyStopping(monitor='val_loss', patience=10, restore_best_weights=True)
    history = model.fit(X_train, y_train, epochs=200, batch_size=32, validation_data=(X_val, y_val),
                        callbacks=[early_stopping])
    return model

# 4. 测试结果与可视化
def evaluate_model(model, test_df):
    """
    评估模型并可视化结果
    :param model: 训练好的模型
    :param test_df: 测试数据集
    """
    # 获取测试集数据的numpy数组格式
    X_test, y_test = test_df.select("final_features").rdd.map(lambda x: x[0].toArray()).collect(), \
                     test_df.select("close").rdd.map(lambda x: x[0]).collect()
    X_test = np.array(X_test)
    y_test = np.array(y_test).reshape(-1, 1)
    # 重塑X_test的形状以匹配模型期望的输入形状
    time_steps = 1
    num_samples = X_test.shape[0]
    X_test = X_test.reshape(num_samples, time_steps, -1)
    predictions = model.predict(X_test)
    mse = np.mean((predictions - y_test) ** 2)
    print("测试集均方误差（MSE）:", mse)

    # 绘制对比图
    plt.plot(y_test, label='True Prices')
    plt.plot(predictions, label='Predicted Prices', alpha=0.7)
    plt.xlabel('Index')
    plt.ylabel('Stock Price')
    plt.title('Stock Price Prediction Comparison')
    plt.legend()
    plt.savefig('stock_price_prediction.png')

# 主函数
def main():
    spark_df = load_data(file_path)
    train_df, val_df, test_df, feature_dim = preprocess_data(spark_df)
    model = train_model(train_df, val_df, test_df, feature_dim)
    evaluate_model(model, test_df)


if __name__ == "__main__":
    main()