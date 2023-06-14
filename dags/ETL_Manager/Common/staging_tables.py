from sqlalchemy import create_engine
from sqlalchemy import MetaData, Table, Column, Date, String, Float, Integer, Identity


metadata = MetaData()
raw_engine = create_engine("postgresql://postgres:Abc1234@localhost:5432/stock_market_raw_data")
raw_engine_conn = raw_engine.connect()


stock_price = Table(
    'stock_price',
    metadata,
    Column('id', Integer, Identity(start=1, cycle=True), primary_key=True),
    Column('Thoi_gian', Date),
    Column('Ma_CK', String),
    Column('Gia_mo_cua', Float),
    Column('Gia_dong_cua', Float),
    Column('Thay_doi', Float),
    Column('Gia_thap_nhat', Float),
    Column('Gia_cao_nhat', Float),
    Column('KLGD_khop_lenh', Integer),
    Column('GTGD_khop_lenh', Integer)
)

metadata.create_all(raw_engine)
