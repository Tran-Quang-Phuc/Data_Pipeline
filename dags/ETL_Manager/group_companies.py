import json
import os
import sqlalchemy
from sqlalchemy import text
from ETL_Manager.Common.dw_tables import dw_conn, stock_info


current_file = __file__  # Gets the current file path
parent_directory = os.path.abspath(os.path.join(current_file, os.pardir))


def group_companies_by_sector(sub_sector):
    sectors = {
        'sản xuất và khai thác dầu khí': 'Dầu khí',
        'thiết bị và dịch vụ dầu khí': 'Dầu khí',
        'nhựa, cao su & sợi': 'Hóa chất',
        'sản phẩm hóa dầu, nông dược & hóa chất khác': 'Hóa chất',
        'lâm sản và chế biến gỗ': 'Tài nguyên cơ bản',
        'khai khoáng ': 'Tài nguyên cơ bản',
        'thép và sản phẩm thép': 'Tài nguyên cơ bản',
        'khai thác than': 'Tài nguyên cơ bản',
        'sản xuất giấy': 'Tài nguyên cơ bản',
        'khai thác vàng': 'Tài nguyên cơ bản',
        'kim loại màu': 'Tài nguyên cơ bản',
        'nhôm': 'Tài nguyên cơ bản',
        'xây dựng': 'Xây dựng và Vật liệu',
        'vật liệu xây dựng & nội thất': 'Xây dựng và Vật liệu',
        'tư vấn & hỗ trợ kd': 'Hàng & Dịch vụ Công nghiệp',
        'kho bãi, hậu cần và bảo dưỡng': 'Hàng & Dịch vụ Công nghiệp',
        'chất thải & môi trường': 'Hàng & Dịch vụ Công nghiệp',
        'thiết bị điện': 'Hàng & Dịch vụ Công nghiệp',
        'nhà cung cấp thiết bị': 'Hàng & Dịch vụ Công nghiệp',
        'containers & đóng gói': 'Hàng & Dịch vụ Công nghiệp',
        'máy công nghiệp': 'Hàng & Dịch vụ Công nghiệp',
        'đường sắt': 'Hàng & Dịch vụ Công nghiệp',
        'đào tạo & việc làm': 'Hàng & Dịch vụ Công nghiệp',
        'hàng điện & điện tử': 'Hàng & Dịch vụ Công nghiệp',
        'vận tải thủy': 'Hàng & Dịch vụ Công nghiệp',
        'dịch vụ vận tải': 'Hàng & Dịch vụ Công nghiệp',
        'xe tải & đóng tàu': 'Hàng & Dịch vụ Công nghiệp',
        'chuyển phát nhanh': 'Hàng & Dịch vụ Công nghiệp',
        'công nghiệp phức hợp': 'Hàng & Dịch vụ Công nghiệp',
        'sản xuất ô tô': 'Ô tô và phụ tùng',
        'lốp xe': 'Ô tô và phụ tùng',
        'phụ tùng ô tô': 'Ô tô và phụ tùng',
        'nuôi trồng nông & hải sản': 'Thực phẩm và đồ uống',
        'thực phẩm': 'Thực phẩm và đồ uống',
        'sản xuất bia ': 'Thực phẩm và đồ uống',
        'đồ uống & giải khát': 'Thực phẩm và đồ uống',
        'vang & rượu mạnh': 'Thực phẩm và đồ uống',
        'hàng may mặc': 'Hàng cá nhân & gia dụng',
        'giầy dép': 'Hàng cá nhân & gia dụng',
        'điện tử tiêu dùng': 'Hàng cá nhân & gia dụng',
        'hàng cá nhân': 'Hàng cá nhân & gia dụng',
        'thuốc lá': 'Hàng cá nhân & gia dụng',
        'thiết bị gia dụng': 'Hàng cá nhân & gia dụng',
        'đồ gia dụng lâu bền': 'Hàng cá nhân & gia dụng',
        'đồ chơi': 'Hàng cá nhân & gia dụng',
        'đồ gia dụng một lần': 'Hàng cá nhân & gia dụng',
        'dược phẩm': 'Y tế',
        'thiết bị y tế': 'Y tế',
        'công nghệ sinh học': 'Y tế',
        'chăm sóc y tế': 'Y tế',
        'dụng cụ y tế': 'Y tế',
        'phân phối hàng chuyên dụng': 'Bán lẻ',
        'phân phối thực phẩm': 'Bán lẻ',
        'bán lẻ phức hợp': 'Bán lẻ',
        'dịch vụ tiêu dùng chuyên ngành': 'Bán lẻ',
        'phân phối dược phẩm': 'Bán lẻ',
        'bán lẻ hàng may mặc': 'Bán lẻ',
        'sách, ấn bản & sản phẩm văn hóa': 'Truyền thông',
        'dịch vụ truyền thông': 'Truyền thông',
        'giải trí & truyền thông': 'Truyền thông',
        'nhà hàng và quán bar': 'Du lịch và giải trí',
        'khách sạn': 'Du lịch và giải trí',
        'vận tải hành khách & du lịch': 'Du lịch và giải trí',
        'dịch vụ giải trí': 'Du lịch và giải trí',
        'hàng không': 'Du lịch và giải trí',
        'viễn thông di động': 'Viễn thông',
        'viễn thông cố định': 'Viễn thông',
        'nước': 'Điện, nước & xăng dầu khí đốt',
        'phân phối xăng dầu & khí đốt': 'Điện, nước & xăng dầu khí đốt',
        'sản xuất & phân phối điện': 'Điện, nước & xăng dầu khí đốt',
        'tiện ích khác': 'Điện, nước & xăng dầu khí đốt',
        'ngân hàng': 'Ngân hàng',
        'bảo hiểm phi nhân thọ': 'Bảo hiểm',
        'bảo hiểm nhân thọ': 'Bảo hiểm',
        'tái bảo hiểm': 'Baỏ hiểm',
        'bất động sản': 'Bất động sản',
        'tư vấn, định giá, môi giới bất động sản': 'Bất động sản',
        'môi giới chứng khoán': 'Dịch vụ tài chính',
        'tài chính đặc biệt': 'Dịch vụ tài chính',
        'quỹ đầu tư': 'Dịch vụ tài chính',
        'tài chính cá nhân': 'Dịch vụ tài chính',
        'quản lý tài sản': 'Dịch vụ tài chính',
        'thiết bị viễn thông': 'Công nghệ thông tin',
        'dịch vụ máy tính': 'Công nghệ thông tin',
        'phần mềm': 'Công nghệ thông tin',
        'phần cứng': 'Công nghệ thông tin',
        'internet': 'Công nghệ thông tin',
        'thiết bị văn phòng': 'Công nghệ thông tin',
        'chứng quyền': 'Chứng quyền'
    }

    sub_sector = 'chứng quyền' if sub_sector == None else sub_sector
    return sectors[sub_sector.lower()]


def group_companies_by_index():
    pass


def group_companies():
    stock_keys = dw_conn.execute(text('SELECT "Ma_cp" FROM thong_tin_co_phieu')).fetchall()
    jfile = open(f'{parent_directory}/Extract/Dimentional_data/stocks_information.json', encoding='utf-8')
    stocks = json.load(jfile)['data']
    
    for stock in stocks:
        if (stock['Ma CP'],) not in stock_keys:
            query = sqlalchemy.insert(stock_info).values(
                Ma_cp = stock['Ma CP'], 
                Ten_cty = stock['Ten_cty'],
                Nganh = stock['Nganh'] if stock['Nganh'] else 'Chứng quyền',
                Nhom_nganh = group_companies_by_sector(stock['Nganh']),
                Chi_so = stock['San giao dich']
            ) 
            dw_conn.execute(query) 

    
if __name__ == "__main__":
    group_companies()
    