import json
import requests
import xml.etree.ElementTree as ET
import os


current_file = __file__  # Gets the current file path
parent_directory = os.path.abspath(os.path.join(current_file, os.pardir))


class Interest_Rate_Crawler:
    def __init__(self):
        self.url = 'https://portal.vietcombank.com.vn/UserControls/TVPortal.TyGia/pListLaiSuat.aspx?'
        self.query_params = {
            'CusstomType': '2',
            'BacrhID': '1',
            'InrateType': '',
            'isEn': 'False',
            'numAfter': '2'
        }

    def crawl_interest_rate(self):
        response = requests.get(self.url, params=self.query_params)
        root = ET.fromstring('<root>' + response.text + '</root>')
        VCB_interst_rate = {}
        table = root.findall('.//table/tbody/tr/td[@class="code"]...')
        for han_muc in table:
            thoi_gian = han_muc.find('./td[@class="code"]').text.replace('\n', '').replace('\r', '')
            thoi_gian = thoi_gian.strip()
            lai_suat = han_muc.find('./td[2]').text.replace('\n', '').replace(' ', '').replace('%', '')
            VCB_interst_rate[thoi_gian] = float(lai_suat)

        VCB_object = json.dumps(VCB_interst_rate, ensure_ascii=False)
        with open(f'{parent_directory}/Dimentional_data/vcb_interest_rate.json', 'w', encoding='utf-8') as json_file:
            json_file.write(VCB_object)


if __name__ == "__main__":
    irc = Interest_Rate_Crawler()
    irc.crawl_interest_rate()
