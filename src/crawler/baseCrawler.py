class Crawler(object):

    #初始化
    def __init__(self):
        self.is_article_prob = .5

    #返回包含所有tag数据的 data 列表。
    def extract_data(self, tag_list):
        data = list()
        for tag in tag_list:
            #调用MongoDB的查询方法，返回数据库中tag字段的所有值
            exec(tag + " = self.col.distinct('" + tag + "')")
            exec("data.append(" + tag + ")")

        return data

    # 模糊查询，查询所有包含param字符串的记录
    def query_news(self, _key, param):
        return self.col.find({_key: {'$regex': ".*{}.*".format(param)}})

    def get_url_info(self, url):
        pass

    def get_historical_news(self, url):
        pass

    def get_realtime_news(self, url):
        pass