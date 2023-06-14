import json
import requests
import os
import pandas as pd
from Common.dw_tables import dw_conn


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
        self.AUTH = ['MJoTYGFvtY9c9U2vqv9RgzRmG+b9gbFA+LomnH682Z/NWCko99RYJubnJAoVUTq0',
                     'MJoTYGFvtY9c9U2vqv9Rg16t3hatoHCinzfu5gqp/he+XBuUQecxj8RHhFARmo0p',
                     'MJoTYGFvtY9c9U2vqv9Rg9eYmZt5dCZoh4j9qqaosXCRQBEnnjYxJR8gMSz6uO1q']

    def crawl_energy_price(self):
        historical_data = {
            'id': [],
            'crude-oil': [],
            'brent-crude-oil': [],
            'natural-gas': []
        }
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
                for price_data in historical_prices:
                    historical_data[self.energy_types[i]].append(price_data['y'])
                    if self.energy_types[i] == 'natural-gas':
                        historical_data['id'].append('nl_' + price_data['date'][:10])

        historical_data['crude-oil'].insert(historical_data['id'].index('nl_2022-07-04'), 96.52)
        hdata_df = pd.DataFrame(historical_data).rename(columns={'crude-oil': 'Gia_dau_WTI', 'brent-crude-oil': 'Gia_dau_brent', 'natural-gas': 'Gia_khi_gas'})
        print(hdata_df.head(10))  
        hdata_df.to_sql('nhien_lieu', dw_conn, if_exists='append', index=False)

    
    def load_dimensional_table(self):
        jfile = open(f'{parent_directory}/Extract/Dimentional_data/energy_price.json')
        date = json.load(jfile)
        df = pd.DataFrame({'id':date['date']})
        df['id'] = df['id'].apply(lambda x: 'ls_' + x[:10])
        print(df)

        df.to_sql('lai_suat', dw_conn, if_exists='append', index=False)
        


if __name__ == "__main__":
    croc = Energy_Price_Crawler()
    # croc.crawl_energy_price()
    croc.load_dimensional_table()
