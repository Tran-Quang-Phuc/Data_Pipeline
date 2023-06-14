import requests
from datetime import datetime
import xml.etree.ElementTree as ET
import os
import json
from Common.dw_tables import dw_conn
from sqlalchemy import text


current_file = __file__  # Gets the current file path
parent_directory = os.path.abspath(os.path.join(current_file, os.pardir))

def crawl_historiccal_data():
    stocks = dw_conn.execute(text("""SELECT "Ma_cp" FROM thong_tin_co_phieu WHERE "Nganh" <> 'Chứng quyền'""")).fetchall()
    url_p = 'https://www.stockbiz.vn/Stocks/'
    url_a = '/HistoricalQuotes.aspx?'
    query_params = {
        'Cart_ctl00_webPartManager_wp1770166562_wp1427611561_callbackData_Callback_Param': ['2022-12-11', '2023-6-11', '']
    }
    pages = ['1', '2', '3', '4', '5', '6', '7']
    data = {}
    for stock in stocks:
        try:
            url = url_p + stock[0] + url_a
            data[stock[0]] = []
            print(url)
        except Exception as e:
            print(e, stock[0])
        
        for page in pages:
            query_params['Cart_ctl00_webPartManager_wp1770166562_wp1427611561_callbackData_Callback_Param'][2] = page
            response = requests.get(url, params=query_params)
            xml_data_string = response.text.replace(" ]]>", "").replace("<![CDATA[", "").replace("&nbsp;", "")
            root = ET.fromstring(xml_data_string)
            daily_data = root.findall('.//tr')
            for day in daily_data:
                daily_data = {}
                daily_data['date'] = day[0].text.replace('\n', '').replace('  ', '')
                daily_data['ma'] = stock[0]
                daily_data['close_price'] = day[5].text.replace('\n', '').replace('  ', ''),
                if day[1].text.replace('\n','').replace(' ', ''):
                    daily_data['change'] = 0.0
                    daily_data['change%'] = 0.0
                else:
                    change = day[1][0].text.split('/')
                    daily_data['change'] = change[0]
                    daily_data['change%'] = change[1]
                daily_data['lowest'] = day[4].text.replace('\n', '').replace(' ', '')
                daily_data['highest'] = day[3].text.replace('\n', '').replace(' ', '')
                daily_data['kl'] = day[8].text.replace('\n','').replace(' ', '')

                data[stock[0]].append(daily_data)
        
    
    data_obj = json.dumps(data, indent=4, ensure_ascii=False)
    with open(f'{parent_directory}/Extract/Stocks_Price/historical_stocks_data.json', 'w', encoding='utf-8') as json_file:
        json_file.write(data_obj)
    


if __name__ == "__main__":
    crawl_historiccal_data()