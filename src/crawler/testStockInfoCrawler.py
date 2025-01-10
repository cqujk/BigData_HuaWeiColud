from stockInfoCrawler import StockInfoCrawler
import akshare as ak
import config

if __name__ == "__main__":
        
    stockInfoCrawler=StockInfoCrawler("stock","basic_info")
    stockInfoCrawler.get_realtime_news()
    
    """ stock_daily_df = ak.stock_zh_a_daily(symbol="sh600000",adjust="qfq")
    print(stock_daily_df) """
    
    
    
    
 