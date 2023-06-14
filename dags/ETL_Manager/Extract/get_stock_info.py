import json
from string import ascii_uppercase as auc

import requests
import os


current_file = __file__  # Gets the current file path
parent_directory = os.path.abspath(os.path.join(current_file, os.pardir))


class Company_Info_Crawler:
    def __init__(self):
        self.url = 'https://api-finance-t19.24hmoney.vn/v1/ios/company/az?'
        self.query_parameters = {
            'letter':'',
            'page':'1',
            'per_page':'20'
        }

    def get_company_info(self):
        companies_info = {'data': []}
        for letter in auc:
            self.query_parameters['letter'] = letter           
            try:
                res = requests.get(self.url, params=self.query_parameters)
                json_data_ob = json.loads(res.text)
                n_page = json_data_ob['data']['total_page']    
            except requests.RequestException as e:
                print(e)
            except json.JSONDecodeError as e:
                print(e)
            else:
                for i in range(1, n_page + 1):
                    self.query_parameters['page'] = f'{i}'
                    try:
                        res = requests.get(self.url, params=self.query_parameters)
                        json_data_ob = json.loads(res.text)
                        com_list = json_data_ob['data']['data']    
                    except requests.RequestException as e:
                        print(e)
                    except json.JSONDecodeError as e:
                        print(e)
                    else:
                        for com in com_list:
                            info = {
                                'Ma CP': com['symbol'],
                                'Ten_cty': com['company_name'],
                                'Nganh': com['icb_name_vi'],
                                'San giao dich': com['floor']
                            }
                            companies_info['data'].append(info)

        companies_info_object = json.dumps(companies_info, indent=4, ensure_ascii=False)        
        with open(f'{parent_directory}/Dimentional_data/stocks_information.json', 'w', encoding='utf-8') as json_file:
            json_file.write(companies_info_object)


if __name__ == "__main__":
    cic = Company_Info_Crawler()
    cic.get_company_info()
    print("End!!!")