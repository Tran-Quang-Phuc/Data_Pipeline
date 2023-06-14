import json
import requests
import xml.etree.ElementTree as ET
import os


current_file = __file__  # Gets the current file path
parent_directory = os.path.abspath(os.path.join(current_file, os.pardir))


class Gold_Price_Crawler():
    def __init__(self):
        self.url = 'https://sjc.com.vn/xml/tygiavang.xml'
    
    def crawl_scj_price(self):
        response = requests.get(self.url)
        root = ET.fromstring(response.text)
        SCJ = {'data': []}
        cities = root.findall('./ratelist/city')
        for city in cities:
            try:
                item = city.find('./item')
                data = {
                    'name':city.attrib['name'],
                    'buy': item.attrib['buy'],
                    'sell': item.attrib['sell']
                }
            except Exception as e:
                print(e)
            else:
                SCJ['data'].append(data)
    
        scj_ob = json.dumps(SCJ, indent=4, ensure_ascii=False)
        with open(f'{parent_directory}/Dimentional_data/scj_price.json', 'w', encoding='utf-8') as json_file:
            json_file.write(scj_ob)


if __name__ == "__main__":
    gpc = Gold_Price_Crawler()
    gpc.crawl_scj_price()
    print("End!!!")
