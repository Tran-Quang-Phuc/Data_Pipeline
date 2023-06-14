import json
import requests
import os


current_file = __file__  # Gets the current file path
parent_directory = os.path.abspath(os.path.join(current_file, os.pardir))


class Energy_Price_Crawler:
    def __init__(self):
        self.energy_types = ['crude-oil', 'brent-crude-oil', 'natural-gas']       
        self.url = 'https://markets.tradingeconomics.com/chart?'
        self.query_parameters = {
            'interval':'1d',
            'span':'1y',
            'securify':'new',
            'ohlc':'0',
        }
        self.s = ['cl1:com', 'co1:com', 'ng1:com']
        self.AUTH = ['aV3U8336wt6sM4brwM+9dBt8VXCtW2n1dLJtawZXGbRJI8AL6md/vR96Y3H1wRKY',
                     'aV3U8336wt6sM4brwM+9dFgF73uKcgo5OXZuow1Hd0WxT+jFQNaUk/HfFGcKgLWu',
                     'aV3U8336wt6sM4brwM+9dNaMP4Hy77/Zw/FEJtssv8iDPfD5h/LbRg2RLPJMBkm8']

    def crawl_energy_price(self):
        price_data = {}
        for i in range(len(self.energy_types)):
            self.query_parameters['s'] = self.s[i]
            self.query_parameters['url'] = f'/commodity/{self.energy_types[i]}'
            self.query_parameters['AUTH'] = self.AUTH[i]
            try:
                res = requests.get(self.url, params=self.query_parameters)
                json_data_ob = json.loads(res.text)
                historical_prices = json_data_ob['series'][0]['data']
            except requests.RequestException as e:
                print("Exception: ", e)
            except json.JSONDecodeError as e:
                print("Exception: ", e)
            except Exception as e:
                print("Exception: ", e)
            else:
                price_data['date'] = historical_prices[-1]['date']
                price_data[f'{self.energy_types[i]}'] = historical_prices[-1]['y']
                price_data_obj = json.dumps(price_data)
                with open(f'{parent_directory}/Dimentional_data/energy_price.json', 'w') as json_file:
                    json_file.write(price_data_obj)


if __name__ == "__main__":
    croc = Energy_Price_Crawler()
    croc.crawl_energy_price()
