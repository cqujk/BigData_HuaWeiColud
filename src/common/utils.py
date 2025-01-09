import re
from datetime import datetime,timedelta
import requests
import numpy as np
from bs4 import BeautifulSoup
from scipy.sparse import csr_matrix


# 获取新闻时间(针对金融界)
def get_url_time(url):
    time=""
    # 使用正则表达式提取日期和时间
    match = re.search(r'/(\d{4})/(\d{2})/(\d{2})(\d{2})(\d{2})', url)
    if match:
        # 提取年份、月份、日期、小时和分钟
        year, month, day, hour, minute = match.groups()
        
        # 将提取到的信息格式化为所需的时间格式
        time = f"{year}-{month}-{day} {hour}:{minute}"

    return time

# 获取新闻内容
def get_url_content(url):
    bs = html_parser(url)
    content = bs.find("div", class_="article_content")
    if content:
        return content.text
    else:
        return ""

# 获取代理池
def get_proxy_pool():
    # FreeProxy API URL
    api_url = "https://www.proxy-list.download/api/v1/get?type=https"
    
    # 获取免费代理
    response = requests.get(api_url)
    proxy_list = response.text.splitlines()
    
    return proxy_pool


def html_parser(url):
    headers = {
        "User-Agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
    }
    resp = requests.get(url,headers=headers)
    resp.encoding = BeautifulSoup(resp.content, "lxml").original_encoding
    bs = BeautifulSoup(resp.text, "lxml")
    
    return bs


def get_chn_stop_words(path):
    '''Load the stop words txt file.
    '''
    stopwords = [line.strip() for line in open(path, 'r').readlines()]

    return stopwords


def convert_to_csr_matrix(model_vector):
    """
    Convert LDA(LSI) model vector to CSR sparse matrix, that could be accepted by Scipy and Numpy.

    # Arguments:
        modelVec: Transformation model vector, such as LDA model vector, tfidf model vector or lsi model vector.
    """
    data = []
    rows = []
    cols = []
    _line_count = 0
    for line in model_vector:  # line=[(int, float), (int, float), ...]
        for elem in line:  # elem=(int, float)
            rows.append(_line_count)
            cols.append(elem[0])
            data.append(elem[1])
        _line_count += 1
    sparse_matrix = csr_matrix((data, (rows, cols)))
    matrix = sparse_matrix.toarray()  # <class 'numpy.ndarray'>

    return matrix


def generate_training_set(x, y, split=0.8):
    rand = np.random.random(size=x.shape[0])
    train_x = []
    train_y = []
    test_x = []
    test_y = []
    for i in range(x.shape[0]):
        if rand[i] < split:
            train_x.append(x[i, :])
            train_y.append(y[i])
        else:
            test_x.append(x[i, :])
            test_y.append(y[i])
    return train_x, train_y, test_x, test_y


def is_contain_chn(word):
    """
    判断传入字符串是否包含中文
    :param word: 待判断字符串
    :return: True:包含中文  False:不包含中文
    """
    zh_pattern = re.compile(u'[\u4e00-\u9fa5]+')
    if zh_pattern.search(word):
        return True
    else:
        return False


def batch_lpop(client, key, n):
    p = client.pipeline()
    p.lrange(key, 0, n-1)
    p.ltrim(key, n, -1)
    p.execute()
    
# 获得离当天最近的工作日期(当天不算)
def get_recent_weekday():
    # 获取当前日期
    today = datetime.today()  
    # 如果是周一(5)，减去三天,得到上周五；如果是周日 (6)，减去两天，得到这周五；
    # 其他情况都是减去一天
    if today.weekday() == 0:  # 周一
        recent_weekday = today - timedelta(days=3)
    elif today.weekday() == 6:  # 周日
        recent_weekday = today - timedelta(days=2)
    else:  # 周二到周六
        recent_weekday = today - timedelta(days=1)
    
    # 返回日期字符串，格式：YYYY-MM-DD
    return recent_weekday.strftime('%Y-%m-%d')  
