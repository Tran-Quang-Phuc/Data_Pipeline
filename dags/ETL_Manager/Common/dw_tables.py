from sqlalchemy import Identity, MetaData, create_engine, ForeignKey
from sqlalchemy import Table, Column, Integer, Float, String, Date


dw_engine = create_engine("postgresql://postgres:Abc1234@localhost:5432/data_warehouse_for_stock_market")
metadata = MetaData()
dw_conn = dw_engine.connect()


fact_table = Table(
    'du_lieu_giao_dich',
    metadata,
    Column('id', Integer, Identity(start=1, cycle=True), primary_key=True),
    Column('Thoi_gian', Date, ForeignKey('thoi_gian.Thoi_gian'), nullable=False),
    Column('scj_id', String, ForeignKey('vang_scj.id'), nullable=False),
    Column('nl_id', String, ForeignKey('nhien_lieu.id'), nullable=False),
    Column('ls_id', String, ForeignKey('lai_suat.id'), nullable=False),
    Column('Ma_cp', String, ForeignKey('thong_tin_co_phieu.Ma_cp')),
    Column('Gia_dong_cua', Float),
    Column('Thay_doi_phan_tram', Float),
    Column('Thay_doi', Float),
    Column('Gia_thap_nhat', Float),
    Column('Gia_cao_nhat', Float),
    Column('Tong_KLGD', Integer)
)


time = Table(
    'thoi_gian',
    metadata,
    Column('Thoi_gian', Date, primary_key=True),
    Column('Ngay', Integer),
    Column('Thang', Integer),
    Column('Quy', Integer),
    Column('Nam', Integer),
)


gold_price = Table(
    'vang_scj',
    metadata,
    Column('id', String, primary_key=True),
    Column('Gia_mua_thap_nhat', Float),
    Column('Gia_mua_cao_nhat', Float),
    Column('Gia_ban_thap_nhat', Float),
    Column('Gia_ban_cao_nhat', Float)
)


energy_price = Table(
    'nhien_lieu',
    metadata,
    Column('id', String, primary_key=True),
    Column('Gia_dau_WTI', Float),
    Column('Gia_dau_brent', Float),
    Column('Gia_khi_gas', Float),
)


interest_rates = Table(
    'lai_suat',
    metadata,
    Column('id', String, primary_key=True),
    Column('KH_24thang', Float),
    Column('KH_36thang', Float),
    Column('KH_60thang', Float)
)


stock_info = Table(
    'thong_tin_co_phieu',
    metadata,
    Column('Ma_cp', String, primary_key=True),
    Column('Ten_cty', String),
    Column('Nganh', String),
    Column('Nhom_nganh', String),
    Column('Chi_so', String)
)


metadata.create_all(dw_engine)
