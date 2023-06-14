import os, json
from datetime import datetime
from Common.dw_tables import dw_conn, fact_table
import sqlalchemy
from sqlalchemy import text


current_file = __file__  # Gets the current file path
parent_directory = os.path.abspath(os.path.join(current_file, os.pardir))

def load_historical_stock_data():
    stocks = dw_conn.execute(text("""SELECT "Ma_cp" FROM thong_tin_co_phieu WHERE "Nganh" <> 'Chứng quyền'""")).fetchall()
    jfile = open(f'{parent_directory}/Extract/Stocks_Price/historical_stocks_data.json', encoding='utf-8')
    data_obj = json.load(jfile)
    times = dw_conn.execute(text('SELECT "Thoi_gian" FROM thoi_gian')).fetchall()

    for stock in stocks:
        for day in data_obj[stock[0]]:
            if day['date'] == "Ngày" or day['close_price'][0] == "Đóng cửa":
                continue
            else:
                time = datetime.strptime(day['date'], '%d/%m/%Y')
                if(time.date(),) not in times:
                    continue
                time_str = time.strftime('%Y-%m-%d')
                query = sqlalchemy.insert(fact_table).values(
                    Thoi_gian = time,
                    scj_id = 'scj_' + time_str,
                    nl_id = 'nl_' + time_str,
                    ls_id = 'ls_' + time_str,
                    Ma_cp = stock[0],
                    Gia_dong_cua = float(day['close_price'][0].replace(',', '.')),
                    Thay_doi_phan_tram = day['change%'] if day['change%'] == 0.0 else float(day['change%'].replace('%', '').replace(',', '.')),
                    Thay_doi = day['change'] if day['change'] == 0.0 else float(day['change'].replace(',', '')),
                    Gia_thap_nhat = float(day['lowest'].replace(',', '.')),
                    Gia_cao_nhat = float(day['highest'].replace(',', '.')),
                    Tong_KLGD = float(day['kl'].replace('.', ''))   
                )

                dw_conn.execute(query)


if __name__ == "__main__": 
    load_historical_stock_data()
