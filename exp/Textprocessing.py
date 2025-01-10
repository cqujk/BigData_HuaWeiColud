from collections import defaultdict
import numpy as np
import jieba, os,io
from gensim import corpora,models
from pymongo import MongoClient
import pandas as pd
from bson import ObjectId
import pyhdfs
import pickle

judgeTerm = {"3天": 3, "5天": 5,"10天": 10,"15天": 15,"30天": 30,"60天": 60}

class TextProcessing(object):
    '''Text pre-processing functions class.
        chnSTWPath: 中文停用词路径.
        finance_dict: 金融词典路径.
    '''

    def __init__(self,hdfs_url,chnSTWPath,finance_dict_path):
        self.chnSTWPath = chnSTWPath
        self.finance_dict_path = finance_dict_path
        
        self._Conn=MongoClient("your_mongo_url")
        self.hdfs_client = pyhdfs.HdfsClient(hdfs_url)

    def getchnSTW(self):
        '''
        从HDFS中加载停用词文件.
        '''
        # 使用 HDFS 客户端读取停用词文件
        with self.hdfs_client.open(f"{self.chnSTWPath}") as file:
            stopwords = [line.strip() for line in file.readlines()]  # 提取停用词列表
        return stopwords

    def load_finance_dict(self):
        '''
        从HDFS中加载金融词典文件.
        '''
        # 使用 HDFS 客户端读取金融词典文件
        with self.hdfs_client.open(f"{self.finance_dict_path}") as file:
            finance_dict = file.readlines()

        # 将词典逐行添加到 jieba 的用户词典中
        for word in finance_dict:
            jieba.add_word(word.strip())
        print("成功加载自定义词典")

    '''去除停用词和空格等无意义的词'''
    def jieba_tokenize(self, documents):
        '''
        用jieba进行分词和去停用词.
        documents: 新闻列表.
        corpora_documents: tokenized documents.
        '''
        # 加载停用词和金融词典
        chnSTW = self.getchnSTW()
        
        # 定义分词和去停用词的逻辑
        def tokenize_and_filter(doc):
            tokens = list(jieba.cut(doc))
            return [word for word in tokens if word not in chnSTW and word.strip()]

        # 执行分词和停用词过滤
        tokenized_documents = [tokenize_and_filter(doc) for doc in documents]

        return tokenized_documents

    def genDictionary(self, documents, **kwarg):
        '''
        生成词典和BoW向量.
        documents: List of news(articles).
        saveDict: Save dictionary or not (bool).
        saveBowvec: Save BoW vectors or not (bool).
        returnValue: Return tokenized documents, dictionary, and BoW vectors (bool).
        '''
        # 调用已实现的分词逻辑
        tokenized_documents = self.jieba_tokenize(documents)

        # 使用 Gensim 的 Dictionary 创建词典
        self._dictionary = corpora.Dictionary(tokenized_documents)

        # 保存词典到 HDFS
        if kwarg.get('saveDict', False):
            saveDictPath = f"{kwarg['saveDictPath']}"
            if not self.hdfs_client.exists(saveDictPath):
                # 将 gensim 的 Dictionary 序列化为字节流
                dict_content = io.BytesIO()
                self._dictionary.save(dict_content)  # 使用 gensim 的 save 方法保存到字节流中
                dict_content.seek(0)  # 需要将字节流的指针重置到开始位置

                # 将字节流内容上传到 HDFS
                self.hdfs_client.create(saveDictPath, dict_content.read())
        
        # 生成 BoW 向量
        self._BowVecOfEachDoc = [self._dictionary.doc2bow(doc) for doc in tokenized_documents]

        # 保存 BoW 向量到 HDFS
        if kwarg.get('saveBowvec', False):
            saveBowvecPath = f"{kwarg['saveBowvecPath']}"
            if not self.hdfs_client.exists(saveBowvecPath):
                # 使用 pickle 将 BoW 向量序列化为二进制格式
                bow_data = pickle.dumps(self._BowVecOfEachDoc)

                # 使用 create() 方法将内容写入 HDFS 文件
                self.hdfs_client.create(saveBowvecPath, bow_data)

        # 返回值（如果需要）
        if kwarg.get('returnValue', True):
            return tokenized_documents, self._dictionary, self._BowVecOfEachDoc
        
        
    def extractData(self, dbName, colName, tag_list, skip=0,limit=1000,filter_conditions=None):
        '''
        从特定数据库特定表提取数据：
            dbName: 数据库.
            colName: 表.
            tag_list: 需要被提取的属性（列）
            filter_conditions: 过滤条件（可选）
        '''
        # 取出特定名称的数据库连接
        db = self._Conn[dbName]
        
        # 获取数据库中的集合（类似于关系数据库中的表）
        collection = db.get_collection(colName)
        
        # 如果提供了过滤条件，应用该条件
        query = filter_conditions if filter_conditions else {}
        
        # 查询满足条件的数据
        cursor = collection.find(query).skip(skip).limit(limit)
        
        # 初始化存储数据的字典
        tag_data_dict = {tag: [] for tag in tag_list}
        
        # 遍历结果集，将每个标签的数据提取出来
        for doc in cursor:
            for tag in tag_list:
                tag_data_dict[tag].append(doc.get(tag))
        
        # 将每个标签数据转化为 DataFrame
        df_list = [pd.Series(tag_data_dict[tag], name=tag) for tag in tag_list]
        dataFrame = pd.concat(df_list, axis=1)
        
        # 过滤出所有列均不为空的行
        dataFrame = dataFrame.dropna()
        
        return dataFrame

    def extractStockCodeFromArticle(self, dbName, colName):
        """提取每篇新闻涉及的股票代码"""
        db = self._Conn[dbName]
        collection = db.get_collection(colName)
        # 获取股票基本信息，并创建一个字典
        data = self.extractData("stock", "basic_info", ['name', 'symbol'])
        stock_name_to_code = {name: symbol for name, symbol in zip(data['name'], data['symbol'])}
        
        query_batch_size = 3000  # 每次查询1000个ID
        offset = 0
        all_ids = []
        batch_size=500
        # 由于数据太多，采取分页查询获取文章ID
        k=0
        while True:
            ids = self.extractData(dbName, colName, ['_id'], skip=offset, limit=query_batch_size)._id
            if ids.empty:
                print("新闻ID查询完毕")
                break  # 如果没有更多数据，退出循环
            k+=1;
            #all_ids.extend(ids)
            offset += query_batch_size
            
            total_articles = len(ids)
            bulk_operations = []

            for i in range(0, total_articles, batch_size):
                batch_ids = ids[i:i + batch_size]
                articles = []

                # 批量查询文章
                batch_docs = collection.find({'_id': {'$in': batch_ids}}, {'_id': 1, 'news_title': 1, 'news_article': 1})
                for doc in batch_docs:
                    title = doc['news_title']
                    article = doc['news_article']
                    articles.append(title + ' ' + article)

                # 分词并提取股票代码
                token = self.jieba_tokenize(articles)
                for j, tk in enumerate(token):
                    relevantStockCode = []
                    for word in tk:
                        if len(word) >= 3:
                            stock_code = stock_name_to_code.get(word)
                            if stock_code:
                                relevantStockCode.append(stock_code)

                    if relevantStockCode:
                        relevantStockCodeDuplicateRemoval = list(set(relevantStockCode))
                        update_operation = UpdateOne(
                            {"_id": batch_ids.iloc[j]},   # 使用 iloc 来通过整数位置访问 Series 元素
                            {"$set": {"relevantStock": ' '.join(relevantStockCodeDuplicateRemoval)}}
                        )
                        bulk_operations.append(update_operation)

                # 执行批量更新
                if bulk_operations:
                    collection.bulk_write(bulk_operations)
                    bulk_operations.clear()
                del articles
                del token
                print("已经有{}篇新闻被扫描".format(min(i + batch_size, total_articles)))

        print('所有文章处理完成！')


    # 给每条新闻打标签
    def judgeGoodOrBadNews(self, date, judgeTerm, stockCode):
        """
        基于股票历史数据分析新闻对该股票的影响

        # Arguments:
            date: 新闻发布日期
            judgeTerm: 判断周期
        """
        # 连接对应表
        col_name = stockCode + "_price"
        db = self._Conn['stock']
        collection = db.get_collection(col_name)

        # 提取日期列表，日期格式为 ISODate("2024-11-14T00:00:00Z")
        dateLst = self.extractData("stock", col_name, ['date']).date

        # date是字符串格式，例如："2024-10-15"，转换为Timestamp格式，便于比较
        date_obj = pd.to_datetime(date)

        news_close_prices = []
        days = 0
        historical_close_prices = []

        # 获取新闻发布前的数据
        for dt in dateLst:
            if dt < date_obj:  # 获取发布新闻之前的历史收盘价
                stock_data = collection.find_one({'date': dt})
                if stock_data:
                    close = float(stock_data['close'])
                    if not np.isnan(close):  # 使用 np.isnan() 检查是否为 NaN
                        historical_close_prices.append(close)
            else:  # 发布新闻后的收盘价
                stock_data = collection.find_one({'date': dt})
                if stock_data:
                    close = float(stock_data['close'])
                    if not np.isnan(close):
                        news_close_prices.append(close)
                        days += 1
                        if days > judgeTerm:
                            break

        # 使用对数收益率计算历史收益和波动性
        historical_returns = [
            np.log(historical_close_prices[i] / historical_close_prices[i - 1])
            for i in range(1, len(historical_close_prices))
        ]
        
        # 计算历史收益均值和波动性
        historical_mean = np.mean(historical_returns)
        historical_std = np.std(historical_returns)

        # 计算新闻后的总收益率（对数收益）
        if len(news_close_prices) >= 2:
            news_return = np.log(news_close_prices[-1] / news_close_prices[0])
        else:
            return '中立'
        # 判断新闻的影响
        if news_return > historical_mean + historical_std:
            return '利好'
        elif news_return < historical_mean - historical_std:
            return '利空'
        else:
            return '中立'

    def getNewsOfSpecificStock(self, dbColLst, stockCode, **kwarg):
        '''
            从历史数据库获取与特定股票相关的新闻
            dbColLst: 数据和表的list ，如: [(db_1,col_1),(db_2,col_2),...,(db_N,col_N)].
            stockCode: 股票代码
            export: 具体保存的数据库和表 eg: export=['database','collection'].
        '''

        for dbName, colName in dbColLst:
            db = self._Conn[dbName]
            collection = db.get_collection(colName)
            idLst = self.extractData(dbName, colName, ['_id'])._id
            
            newdb = self._Conn[kwarg['export'][0]]
            newcollection = newdb.get_collection(kwarg['export'][1])
            i=0
            for _id in idLst:
                keys = ' '.join([k for k in collection.find_one({'_id': ObjectId(_id)}).keys()])
                if keys.find('relevantStock') != -1:
                    if collection.find_one({'_id': ObjectId(_id)})['relevantStock'].find(stockCode) != -1:
                        #获取新闻相关信息
                        article=collection.find_one({'_id': ObjectId(_id)})
                        news_time=article['news_time']
                        news_title=article['news_title']
                        news_article=article['news_article']
                        
                        character = self.judgeGoodOrBadNews(news_time,judgeTerm["15天"],stockCode)

                        data = {'news_time': news_time,
                                'news_title': news_title,
                                'news_article': news_article,
                                'Character': character}
                        newcollection.insert_one(data)
                        i+=1
                        if i%100==0:
                            print("{}条新闻已经插入{}股票的表中".format(i,stockCode))

            print(' 对于[' + stockCode + ']在 ' + dbName + ' 数据库中的新闻已经成功提取 ')


    def CallTransformationModel(self, Dict, Bowvec, **kwarg):
        '''
        调用 Gensim 模块中的特定转换模型

        Dict: 由所有分词后的新闻（文章/文档）构建的字典。
        Bowvec: 由所有分词后的新闻（文章/文档）创建的词袋向量。
        modelType: 转换模型类型，包括 'lsi'、'lda' 和 'None'，其中 'None' 表示 TF-IDF 模型。
        tfDim: 从每篇新闻（文章/文档）中提取的主题数量。
        renewModel: 是否重新训练转换模型（布尔类型）。
        modelPath: 保存训练好的转换模型的路径。
        '''
        
        # 默认从 HDFS 加载或保存模型路径
        tfidf_model_path = f"{kwarg['modelPath']}tfidf_model.tfidf"
        lsi_model_path = f"{kwarg['modelPath']}lsi_model.lsi"
        lda_model_path = f"{kwarg['modelPath']}lda_model.lda"

        # 如果需要重新训练模型
        if kwarg['renewModel']:
            tfidf = models.TfidfModel(Bowvec)  # Initialize TF-IDF model
            tfidfVec = tfidf[Bowvec]  # Transform corpus using TF-IDF

            # 序列化 tfidf 模型
            serialized_tfidf = pickle.dumps(tfidf)

            # 将序列化后的数据保存到 HDFS
            self.hdfs_client.create(tfidf_model_path, data=serialized_tfidf, overwrite=True)

            # 根据模型类型选择是否训练 LSI 或 LDA 模型
            if kwarg['modelType'] == 'lsi':
                model = models.LsiModel(tfidfVec, id2word=Dict, num_topics=kwarg['tfDim'])
                modelVec = model[tfidfVec]  # Transform corpus using LSI

                serialized_lsi = pickle.dumps(model)

                # 将序列化后的数据保存到 HDFS
                self.hdfs_client.create(lsi_model_path, data=serialized_lsi, overwrite=True)

            elif kwarg['modelType'] == 'lda':
                model = models.LdaModel(tfidfVec, id2word=Dict, num_topics=kwarg['tfDim'])
                modelVec = model[tfidfVec]  # Transform corpus using LDA

                serialized_lda = pickle.dumps(model)

                # 将序列化后的数据保存到 HDFS
                self.hdfs_client.create(lsi_model_path, data=serialized_lda, overwrite=True)

            elif kwarg['modelType'] == 'None':
                model = tfidf
                modelVec = tfidfVec

        else:
            # 如果不需要重新训练，加载已有模型
            # 加载 TF-IDF 模型
            with self.hdfs_client.open(tfidf_model_path) as f:
                tfidf = pickle.load(f)
                
            print(f"Bowvec: {Bowvec}")
            tfidfVec = tfidf[Bowvec]  # Transform corpus using the loaded TF-IDF model
            print(f'tfidfVec是{tfidfVec}')
            # 根据模型类型加载相应的 LSI 或 LDA 模型
            if kwarg['modelType'] == 'lsi':
                with self.hdfs_client.open(lsi_model_path) as f:
                    model = pickle.load(f)
                modelVec = model[tfidfVec]
            elif kwarg['modelType'] == 'lda':
                with self.hdfs_client.open(lda_model_path) as f:
                    model = pickle.load(f)
                modelVec = model[tfidfVec]
            elif kwarg['modelType'] == 'None':
                model = tfidf
                modelVec = tfidfVec

        return tfidfVec, modelVec