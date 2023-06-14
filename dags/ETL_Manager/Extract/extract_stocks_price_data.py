import requests
import json
import os


current_file = __file__  # Gets the current file path
parent_directory = os.path.abspath(os.path.join(current_file, os.pardir))


class Stock_Price_Crawler:
    def __init__(self):
        self.url = 'https://wgateway-iboard.ssi.com.vn/graphql'
        self.query_parameters = {
            "operationName":"stockRealtimes",
            "variables":{
                "exchange":""
            },
            "query":"query stockRealtimes($exchange: String) {\n  stockRealtimes(exchange: $exchange) {\n    stockNo\n    ceiling\n    floor\n    refPrice\n    stockSymbol\n    stockType\n    exchange\n    lastMatchedPrice\n    matchedPrice\n    matchedVolume\n    priceChange\n    priceChangePercent\n    highest\n    avgPrice\n    lowest\n    nmTotalTradedQty\n    best1Bid\n    best1BidVol\n    best2Bid\n    best2BidVol\n    best3Bid\n    best3BidVol\n    best4Bid\n    best4BidVol\n    best5Bid\n    best5BidVol\n    best6Bid\n    best6BidVol\n    best7Bid\n    best7BidVol\n    best8Bid\n    best8BidVol\n    best9Bid\n    best9BidVol\n    best10Bid\n    best10BidVol\n    best1Offer\n    best1OfferVol\n    best2Offer\n    best2OfferVol\n    best3Offer\n    best3OfferVol\n    best4Offer\n    best4OfferVol\n    best5Offer\n    best5OfferVol\n    best6Offer\n    best6OfferVol\n    best7Offer\n    best7OfferVol\n    best8Offer\n    best8OfferVol\n    best9Offer\n    best9OfferVol\n    best10Offer\n    best10OfferVol\n    buyForeignQtty\n    buyForeignValue\n    sellForeignQtty\n    sellForeignValue\n    caStatus\n    tradingStatus\n    remainForeignQtty\n    currentBidQty\n    currentOfferQty\n    session\n    __typename\n  }\n}\n"
        }

    def crawl_stocks_price(self, exchanges=['hose', 'hnx', 'upcom']):
        for exchange in exchanges:
            self.query_parameters['variables']['exchange'] = exchange
            
            try:
                res = requests.post(self.url, json=self.query_parameters)
                json_data = json.loads(res.text)
                stocks = json_data['data']['stockRealtimes']
            except requests.RequestException as e:
                print(e)
            except json.JSONDecodeError as e:
                print(e)
            else:
                stocks_data = {'data':[]}
                for stock in stocks:
                    stock_info = [{
                        'Ma CP': stock['stockSymbol'],
                        'Gia dong cua': stock['lastMatchedPrice'] if stock['lastMatchedPrice'] else stock['matchedPrice'],
                        'Khoi luong': stock['nmTotalTradedQty'],
                        'Thay doi': stock['priceChange'],
                        'Thay doi %': stock['priceChangePercent'],
                        'Thap_nhat': stock['lowest'],
                        'Cao_nhat': stock['highest'],
                        'San giao dich': stock['exchange']
                    }]
                    stocks_data['data'] = stocks_data['data'] + stock_info
                    
                stocks_object = json.dumps(stocks_data, indent=4)
                with open(f'{parent_directory}/Stocks_Price/{exchange}_stocks_data.json', 'w') as json_file:
                    json_file.write(stocks_object)


if __name__ == "__main__":
    stp_crawler = Stock_Price_Crawler()
    stp_crawler.crawl_stocks_price()
    print("End!!!")
    