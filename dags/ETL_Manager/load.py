import json, os
import sqlalchemy
from sqlalchemy import text
from datetime import datetime
from ETL_Manager.Common.dw_tables import dw_conn, time, energy_price, interest_rates, fact_table


current_file = __file__  # Gets the current file path
parent_directory = os.path.abspath(os.path.join(current_file, os.pardir))


def load_time_dimension():
    time_keys = dw_conn.execute(text('SELECT "Thoi_gian" FROM thoi_gian')).fetchall()
    print(time_keys)
    t = datetime.now()
    if (t.date(),) not in time_keys:
        query = sqlalchemy.insert(time).values(
            Thoi_gian = t,
            Ngay = t.day,
            Thang = t.month,
            Quy = (t.month-1)/4 + 1,
            Nam = t.year
        )
        dw_conn.execute(query)


def load_energy_price():
    energy_keys = dw_conn.execute(text('SELECT "id" FROM nhien_lieu')).fetchall()
    print(energy_keys)
    jfile = open(f'{parent_directory}/Extract/Dimentional_data/energy_price.json')
    ep_obj = json.load(jfile)
    current_id = 'nl_' + datetime.now().strftime("%Y-%m-%d")
    if (current_id,) not in energy_keys:   
        query = sqlalchemy.insert(energy_price).values(
            id = current_id,
            Gia_dau_WTI = ep_obj['crude-oil'],
            Gia_dau_brent = ep_obj['brent-crude-oil'],
            Gia_khi_gas = ep_obj['natural-gas']
        )
        dw_conn.execute(query)


def load_interest_rate():
    ir_keys = dw_conn.execute(text('SELECT "id" FROM lai_suat')).fetchall()
    jfile = open(f'{parent_directory}/Extract/Dimentional_data/vcb_interest_rate.json')
    ir_obj = json.load(jfile)
    current_id = 'ls_' + datetime.now().strftime("%Y-%m-%d")
    if (current_id,) not in ir_keys:
        query = sqlalchemy.insert(interest_rates).values(
            id = current_id,
            KH_24thang = float(ir_obj["24 tháng"]),
            KH_36thang = float(ir_obj["36 tháng"]),
            KH_60thang = float(ir_obj["60 tháng"])
        )
        dw_conn.execute(query)


def load_main_transactions():
    stock_keys = dw_conn.execute(text('SELECT "Ma_cp" FROM thong_tin_co_phieu')).fetchall()
    print(stock_keys)

    exchanges = ['hnx', 'hose', 'upcom']

    for exchange in exchanges:
        jfile = open(f'{parent_directory}/Extract/Stocks_Price/{exchange}_stocks_data.json')
        data_obj = json.load(jfile)['data']

        for stock in data_obj:
            if (stock['Ma CP'],) in stock_keys:
                query = sqlalchemy.insert(fact_table).values(
                    Thoi_gian = datetime.now(),
                    scj_id = 'scj_' + datetime.now().strftime("%Y-%m-%d"),
                    nl_id = 'nl_' + datetime.now().strftime("%Y-%m-%d"),
                    ls_id = 'ls_' + datetime.now().strftime("%Y-%m-%d"),
                    Ma_cp = stock['Ma CP'],
                    Gia_dong_cua = float(stock['Gia dong cua']) if stock['Gia dong cua']  else None, 
                    Thay_doi_phan_tram = float(stock['Thay doi %']) if stock['Thay doi %']  else None,
                    Thay_doi = float(stock['Thay doi']) if stock['Thay doi']  else None,
                    Gia_thap_nhat = float(stock['Thap_nhat']) if stock['Thap_nhat']  else None,
                    Gia_cao_nhat = float(stock['Cao_nhat']) if stock['Cao_nhat']  else None,
                    Tong_KLGD = stock['Khoi luong'] if stock['Khoi luong']  else None
                )
                dw_conn.execute(query)


if __name__ == "__main__":
    load_time_dimension()
    load_energy_price()
    # load_interest_rate()
    # load_main_transactions()
