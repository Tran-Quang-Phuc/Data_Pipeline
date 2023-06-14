from datetime import datetime
import json, os
import sqlalchemy
from ETL_Manager.Common.dw_tables import dw_conn, gold_price

current_file = __file__  # Gets the current file path
parent_directory = os.path.abspath(os.path.join(current_file, os.pardir))


def get_lowest_and_highest_price():
    scj_keys = dw_conn.execute(sqlalchemy.text('SELECT "id" FROM vang_scj')).fetchall()
    sjc_jfile = open(f'{parent_directory}/Extract/Dimentional_data/scj_price.json', encoding='utf-8')
    price_data = json.load(sjc_jfile)['data']
    current_id = 'scj_' + datetime.now().strftime("%Y-%m-%d")
    if (current_id,) in scj_keys:
        return
    
    lowest_buy = 100.0
    highest_buy = 0.0
    lowest_sell = 100.0
    highest_sell = 0.0
    for price in price_data:
        lowest_buy = min(lowest_buy, float(price['buy']))
        highest_buy = max(highest_buy, float(price['buy']))
        lowest_sell = min(lowest_sell, float(price['sell']))
        highest_sell = max(highest_sell, float(price['sell']))
    
    query = sqlalchemy.insert(gold_price).values(
        id='scj_' + datetime.now().strftime("%Y-%m-%d"),
        Gia_mua_thap_nhat=lowest_buy,
        Gia_mua_cao_nhat=highest_buy,
        Gia_ban_thap_nhat=lowest_sell,
        Gia_ban_cao_nhat=highest_sell
    )
    dw_conn.execute(query)

