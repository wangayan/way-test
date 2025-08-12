import pymysql
from datetime import datetime, time, timedelta
import random
import string
from faker import Faker

# 初始化Faker
fake = Faker('zh_CN')

# 数据库配置
db_config = {
    'host': 'cdh01',
    'user': 'root',
    'password': '123456',
    'database': 'gmall_work_02',
    'charset': 'utf8mb4'
}


# 生成随机字符串
def random_string(length):
    return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(length))


# 生成随机商品ID
def generate_product_id():
    return f"P{random.randint(10000, 99999)}"


# 生成随机SKU ID
def generate_sku_id():
    return f"SKU{random.randint(1000, 9999)}"


# 生成随机访客ID
def generate_visitor_id():
    return f"V{random.randint(100000, 999999)}"


# 生成随机订单ID
def generate_order_id():
    return f"O{random.randint(1000000, 9999999)}"


# 生成随机买家ID
def generate_buyer_id():
    return f"B{random.randint(10000, 99999)}"


# 流量来源选项
traffic_sources = [
    '效果广告', '站外广告', '内容广告', '手淘搜索', '购物车',
    '我的淘宝', '手淘其他店铺', '手淘推荐', '品牌广告', '淘内待分类'
]

# 商品分类数据
categories = [
    {'category_id': 'C1001', 'category_name': '电子产品', 'parent_id': None, 'level': 1},
    {'category_id': 'C1002', 'category_name': '服装', 'parent_id': None, 'level': 1},
    {'category_id': 'C1003', 'category_name': '家居', 'parent_id': None, 'level': 1},
    {'category_id': 'C2001', 'category_name': '手机', 'parent_id': 'C1001', 'level': 2},
    {'category_id': 'C2002', 'category_name': '笔记本电脑', 'parent_id': 'C1001', 'level': 2},
    {'category_id': 'C2003', 'category_name': '男装', 'parent_id': 'C1002', 'level': 2},
    {'category_id': 'C2004', 'category_name': '女装', 'parent_id': 'C1002', 'level': 2},
    {'category_id': 'C3001', 'category_name': '智能手机', 'parent_id': 'C2001', 'level': 3},
    {'category_id': 'C3002', 'category_name': '游戏本', 'parent_id': 'C2002', 'level': 3},
]

# 品牌数据
brands = [
    {'brand_id': 'B001', 'brand_name': '华为'},
    {'brand_id': 'B002', 'brand_name': '小米'},
    {'brand_id': 'B003', 'brand_name': '苹果'},
    {'brand_id': 'B004', 'brand_name': '耐克'},
    {'brand_id': 'B005', 'brand_name': '阿迪达斯'},
    {'brand_id': 'B006', 'brand_name': '优衣库'},
]


# 生成随机SKU信息
def generate_sku_info():
    colors = ['红色', '蓝色', '绿色', '黑色', '白色', '银色', '金色']
    sizes = ['S', 'M', 'L', 'XL', 'XXL', '均码']
    types = ['标准版', '豪华版', '尊享版', '青春版']

    if random.random() > 0.5:
        return f"{random.choice(colors)}-{random.choice(sizes)}"
    else:
        return f"{random.choice(types)}-{random.choice(colors)}"


# 生成商品数据
def generate_products(num_products, target_date):
    products = []
    for _ in range(num_products):
        category = random.choice(categories)
        brand = random.choice(brands)
        create_time = fake.date_time_between(
            start_date=datetime.combine(target_date, time(0, 0, 0)),
            end_date=datetime.combine(target_date, time(23, 59, 59))
        )

        product = {
            'product_id': generate_product_id(),
            'product_name': fake.word().capitalize() + ' ' + fake.word(),
            'category_id': category['category_id'],
            'brand_id': brand['brand_id'],
            'brand_name': brand['brand_name'],
            'shelf_time': create_time,
            'price': round(random.uniform(50, 2000), 2),
            'status': random.choices([0, 1], weights=[0.1, 0.9])[0],
            'create_time': create_time,
            'update_time': create_time
        }
        products.append(product)
    return products


# 生成访客数据
def generate_visitors(products, num_visits_per_product, target_date):
    visitors = []
    for product in products:
        for _ in range(random.randint(1, num_visits_per_product)):
            visit_time = fake.date_time_between(
                start_date=datetime.combine(target_date, time(0, 0, 0)),
                end_date=datetime.combine(target_date, time(23, 59, 59))
            )
            visitors.append({
                'visitor_id': generate_visitor_id(),
                'product_id': product['product_id'],
                'visit_time': visit_time,
                'traffic_source': random.choice(traffic_sources),
                'stay_duration': random.randint(5, 600),
                'page_views': random.randint(1, 10),
                'create_time': visit_time
            })
    return visitors


# 生成交易数据
def generate_transactions(products, num_transactions_per_product, target_date):
    transactions = []
    for product in products:
        for _ in range(random.randint(0, num_transactions_per_product)):
            payment_time = fake.date_time_between(
                start_date=datetime.combine(target_date, time(0, 0, 0)),
                end_date=datetime.combine(target_date, time(23, 59, 59))
            )
            transactions.append({
                'order_id': generate_order_id(),
                'product_id': product['product_id'],
                'sku_id': generate_sku_id(),
                'sku_info': generate_sku_info(),
                'buyer_id': generate_buyer_id(),
                'payment_amount': round(product['price'] * random.uniform(0.8, 1.2) * random.randint(1, 3), 2),
                'payment_time': payment_time,
                'quantity': random.randint(1, 3),
                'create_time': payment_time
            })
    return transactions


# 生成库存数据
def generate_inventory(products, target_date):
    inventory = []
    for product in products:
        for _ in range(random.randint(1, 3)):
            create_time = fake.date_time_between(
                start_date=datetime.combine(target_date, time(0, 0, 0)),
                end_date=datetime.combine(target_date, time(23, 59, 59))
            )
            inventory.append({
                'product_id': product['product_id'],
                'sku_id': generate_sku_id(),
                'sku_info': generate_sku_info(),
                'current_stock': random.randint(10, 500),
                'warning_stock': random.randint(5, 50),
                'create_time': create_time,
                'update_time': create_time
            })
    return inventory


# 生成搜索词数据
def generate_search_terms(products, num_terms_per_product, target_date):
    search_terms = []
    terms = ['新款', '促销', '特价', '正品', '旗舰', '官方', '2025', '限量', '爆款', '热销']

    for product in products:
        for _ in range(random.randint(1, num_terms_per_product)):
            create_time = fake.date_time_between(
                start_date=datetime.combine(target_date, time(0, 0, 0)),
                end_date=datetime.combine(target_date, time(23, 59, 59))
            )

            search_terms.append({
                'search_term': f"{random.choice(terms)} {product['product_name'].split()[0]}",
                'product_id': product['product_id'],
                'visitor_count': random.randint(10, 500),
                'click_rate': round(random.uniform(0.1, 5.0), 2),
                'stat_date': target_date,
                'create_time': create_time
            })
    return search_terms


# 生成价格力评估数据
def generate_price_power(products, target_date):
    price_powers = []
    for product in products:
        market_price = product['price'] * random.uniform(0.8, 1.5)
        create_time = fake.date_time_between(
            start_date=datetime.combine(target_date, time(0, 0, 0)),
            end_date=datetime.combine(target_date, time(23, 59, 59))
        )

        price_powers.append({
            'product_id': product['product_id'],
            'star_level': round(random.uniform(1.0, 5.0), 1),
            'after_coupon_price': round(product['price'] * random.uniform(0.7, 0.95), 2),
            'market_avg_price': round(market_price, 2),
            'assessment_date': target_date,
            'create_time': create_time
        })
    return price_powers


# 生成预警数据
def generate_warnings(products, target_date):
    warnings = []
    for product in products:
        if random.random() < 0.1:
            create_time = fake.date_time_between(
                start_date=datetime.combine(target_date, time(0, 0, 0)),
                end_date=datetime.combine(target_date, time(23, 59, 59))
            )
            warning_type = random.choice([1, 2])

            if warning_type == 1:
                reason = random.choice(['价格高于市场平均价', '价格波动过大', '促销力度不足'])
                suggestion = random.choice(['调整价格', '增加优惠券', '参加平台活动'])
            else:
                reason = random.choice(['转化率低于平均水平', '访客数下降', '加购率低'])
                suggestion = random.choice(['优化详情页', '增加促销活动', '优化标题和图片'])

            warnings.append({
                'product_id': product['product_id'],
                'warning_type': warning_type,
                'warning_reason': reason,
                'suggestion': suggestion,
                'warning_status': random.choices([0, 1], weights=[0.7, 0.3])[0],
                'create_time': create_time,
                'update_time': create_time
            })
    return warnings


# 插入数据到数据库
def insert_data(conn, table_name, data):
    with conn.cursor() as cursor:
        for record in data:
            columns = ', '.join(record.keys())
            placeholders = ', '.join(['%s'] * len(record))
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            cursor.execute(sql, list(record.values()))
    conn.commit()


# 检查表是否为空
def is_table_empty(conn, table_name):
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        return count == 0


def insert_categories(conn, target_date):
    print("处理分类数据...")

    # 为分类数据使用随机时间
    for category in categories:
        random_time = fake.date_time_between(
            start_date=datetime.combine(target_date, time(0, 0, 0)),
            end_date=datetime.combine(target_date, time(23, 59, 59))
        )
        category['create_time'] = random_time
        category['update_time'] = random_time

    # 获取已存在的分类ID
    existing_ids = set()
    with conn.cursor() as cursor:
        cursor.execute("SELECT category_id FROM product_category")
        existing_ids = {row[0] for row in cursor.fetchall()}

    # 只插入不存在的分类
    new_categories = [c for c in categories if c['category_id'] not in existing_ids]

    if new_categories:
        print(f"新增{len(new_categories)}条分类数据...")
        insert_data(conn, 'product_category', new_categories)
    else:
        print("没有新的分类数据需要添加")

# 主函数
def main():
    # 用户指定要生成数据的日期
    date_str = input("请输入要生成数据的日期(YYYY-MM-DD): ")
    target_date = datetime.strptime(date_str, '%Y-%m-%d').date()

    # 连接数据库
    conn = pymysql.connect(**db_config)

    try:
        # 生成数据
        num_products = random.randint(20, 50)  # 生成20-50个商品

        print("生成商品数据...")
        products = generate_products(num_products, target_date)
        insert_data(conn, 'product_info', products)

        # 插入分类数据
        insert_categories(conn, target_date)

        print("生成访客数据...")
        visitors = generate_visitors(products, random.randint(5, 20), target_date)
        insert_data(conn, 'product_visitor_detail', visitors)

        print("生成交易数据...")
        transactions = generate_transactions(products, random.randint(0, 10), target_date)
        insert_data(conn, 'product_transaction_detail', transactions)

        print("生成库存数据...")
        inventory = generate_inventory(products, target_date)
        insert_data(conn, 'product_inventory', inventory)

        print("生成搜索词数据...")
        search_terms = generate_search_terms(products, random.randint(1, 3), target_date)
        insert_data(conn, 'product_search_term', search_terms)

        print("生成价格力评估数据...")
        price_powers = generate_price_power(products, target_date)
        insert_data(conn, 'product_price_power', price_powers)

        print("生成预警数据...")
        warnings = generate_warnings(products, target_date)
        if warnings:
            insert_data(conn, 'product_warning', warnings)

        print(f"数据生成完成! 共生成:")
        print(f"- {len(products)} 个商品")
        print(f"- {len(visitors)} 条访客记录")
        print(f"- {len(transactions)} 条交易记录")
        print(f"- {len(inventory)} 条库存记录")
        print(f"- {len(search_terms)} 条搜索词记录")
        print(f"- {len(price_powers)} 条价格力评估记录")
        print(f"- {len(warnings)} 条预警记录")

    finally:
        conn.close()


if __name__ == '__main__':
    main()