import pymysql
from faker import Faker
import random
from datetime import datetime, timedelta, time as dt_time
import time
import logging
import numpy as np

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

random.seed(42)
np.random.seed(42)

DB_CONFIG = {
    'host': 'cdh01',
    'user': 'root',
    'password': '123456',
    'database': 'gd',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

fake = Faker('zh_CN')


def create_db_connection():
    return pymysql.connect(**DB_CONFIG)


def input_target_date():
    """让用户输入目标日期（格式：YYYY-MM-DD）"""
    while True:
        date_str = input("请输入要生成数据的日期（格式：YYYY-MM-DD，例如 2025-08-01）：")
        try:
            return datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            logger.error("日期格式错误，请重新输入！")


def generate_random_time(target_date):
    """生成指定日期的随机时间（时分秒）"""
    random_seconds = random.randint(0, 86399)
    return datetime.combine(target_date, dt_time.min) + timedelta(seconds=random_seconds)


def generate_users(num_users=1000, target_date=None):
    """生成用户数据"""
    users = []
    logger.info(f"开始生成 {num_users} 条用户数据（日期：{target_date}）...")

    platforms = ["无线端"] * 70 + ["PC端"] * 30
    start_datetime = datetime.combine(target_date, dt_time.min)
    end_datetime = start_datetime + timedelta(days=1) - timedelta(seconds=1)

    for i in range(1, num_users + 1):
        user_id = f"user_{str(i).zfill(6)}"
        platform = random.choice(platforms)
        first_visit = fake.date_time_between(start_date=start_datetime, end_date=end_datetime)
        last_visit = first_visit + timedelta(days=random.randint(1, 30))
        create_time = generate_random_time(target_date)

        users.append((user_id, platform, first_visit, last_visit, create_time))

        if i % 100 == 0 or i == num_users:
            logger.info(f"已生成 {i}/{num_users} 条用户数据")

    return users


def generate_pages(num_pages=500, target_date=None):
    """生成页面数据"""
    pages = []
    logger.info(f"开始生成 {num_pages} 条页面数据（日期：{target_date}）...")

    page_types = ['店铺页'] * 60 + ['商品详情页'] * 30 + ['店铺其他页'] * 10
    page_names = {
        '店铺页': ['首页', '活动页', '分类页', '宝贝页', '新品页'],
        '商品详情页': ['商品详情页'],
        '店铺其他页': ['订阅页', '直播页', '会员页', '客服页']
    }
    start_datetime = datetime.combine(target_date, dt_time.min)
    end_datetime = start_datetime + timedelta(days=1) - timedelta(seconds=1)

    for i in range(1, num_pages + 1):
        page_id = f"page_{str(i).zfill(6)}"
        page_type = random.choice(page_types)
        page_name = random.choice(page_names[page_type])
        shop_id = f"shop_{random.randint(1, 50):03d}"  # 减少店铺数量以增加用户访问多个店铺的概率
        product_id = f"prod_{random.randint(1, 50000):06d}" if page_type == '商品详情页' else None
        page_create_time = fake.date_time_between(start_date=start_datetime, end_date=end_datetime)
        create_time = generate_random_time(target_date)

        pages.append((page_id, page_type, page_name, shop_id, product_id, page_create_time, create_time))

        if i % 50 == 0 or i == num_pages:
            logger.info(f"已生成 {i}/{num_pages} 条页面数据")

    return pages


def generate_wireless_access(num_records=5000, users=[], pages=[], target_date=None):
    """生成无线端访问数据"""
    if not users or not pages:
        logger.warning("无法生成无线端数据: 缺少用户或页面数据")
        return []

    wireless_users = [u for u in users if u[1] == "无线端"]
    if not wireless_users:
        logger.error("没有无线端用户，无法生成无线端访问数据")
        return []

    logger.info(f"开始生成 {num_records} 条无线端访问数据（日期：{target_date}）...")
    user_ids = [u[0] for u in wireless_users]
    page_ids = [p[0] for p in pages]
    user_time_ranges = {u[0]: (u[2], u[3]) for u in wireless_users}
    records = []

    # 确保每个用户至少有3-10次访问
    for user_id in user_ids:
        first_visit, last_visit = user_time_ranges[user_id]
        time_diff = (last_visit - first_visit).total_seconds()
        visits_count = random.randint(3, 10)

        for _ in range(visits_count):
            page_id = random.choice(page_ids)
            visit_time = first_visit + timedelta(seconds=random.uniform(0, time_diff))
            is_order = 1 if random.random() < 0.15 else 0
            create_time = generate_random_time(target_date)
            records.append((user_id, page_id, visit_time, is_order, create_time))

    # 补充剩余记录
    remaining_records = num_records - len(records)
    if remaining_records > 0:
        for _ in range(remaining_records):
            user_id = random.choice(user_ids)
            page_id = random.choice(page_ids)
            first_visit, last_visit = user_time_ranges[user_id]
            time_diff = (last_visit - first_visit).total_seconds()
            visit_time = first_visit + timedelta(seconds=random.uniform(0, time_diff))
            is_order = 1 if random.random() < 0.15 else 0
            create_time = generate_random_time(target_date)
            records.append((user_id, page_id, visit_time, is_order, create_time))

    logger.info(f"无线端访问数据生成完成，共 {len(records)} 条")
    return records


def generate_instore_paths(num_records=3000, wireless_records=[], target_date=None):
    """生成店内路径数据"""
    if not wireless_records:
        logger.warning("无法生成店内路径数据: 缺少无线端访问数据")
        return []

    logger.info(f"开始生成 {num_records} 条店内路径数据（日期：{target_date}）...")
    user_visits = {}

    for record in wireless_records:
        user_id = record[0]
        if user_id not in user_visits:
            user_visits[user_id] = []
        user_visits[user_id].append((record[2], record[1], record[3]))  # (时间, 页面ID, 是否下单)

    for user_id in user_visits:
        user_visits[user_id].sort(key=lambda x: x[0])

    valid_users = [uid for uid, visits in user_visits.items() if len(visits) >= 2]
    if not valid_users:
        logger.error("没有用户有足够的访问记录来生成路径")
        return []

    paths = []
    for user_id in valid_users:
        visits = user_visits[user_id]
        # 为每个用户生成2-5条路径
        path_count = random.randint(2, 5)
        for _ in range(path_count):
            if len(visits) < 2:
                continue
            start_idx = random.randint(0, len(visits) - 2)
            source_page = visits[start_idx][1]
            target_page = visits[start_idx + 1][1]
            time1 = visits[start_idx][0]
            time2 = visits[start_idx + 1][0]
            time_diff = (time2 - time1).total_seconds()
            jump_time = time1 + timedelta(seconds=random.uniform(0, time_diff)) if time_diff > 0 else time1
            create_time = generate_random_time(target_date)
            paths.append((user_id, source_page, target_page, jump_time, create_time))
            if len(paths) >= num_records:
                break
        if len(paths) >= num_records:
            break

    # 补充剩余记录
    remaining_records = num_records - len(paths)
    if remaining_records > 0:
        for _ in range(remaining_records):
            user_id = random.choice(valid_users)
            visits = user_visits[user_id]
            if len(visits) < 2:
                continue
            start_idx = random.randint(0, len(visits) - 2)
            source_page = visits[start_idx][1]
            target_page = visits[start_idx + 1][1]
            time1 = visits[start_idx][0]
            time2 = visits[start_idx + 1][0]
            time_diff = (time2 - time1).total_seconds()
            jump_time = time1 + timedelta(seconds=random.uniform(0, time_diff)) if time_diff > 0 else time1
            create_time = generate_random_time(target_date)
            paths.append((user_id, source_page, target_page, jump_time, create_time))

    logger.info(f"店内路径数据生成完成，共 {len(paths)} 条")
    return paths


def generate_pc_access(num_records=3000, users=[], pages=[], target_date=None):
    """生成PC端访问数据"""
    if not users or not pages:
        logger.warning("无法生成PC端数据: 缺少用户或页面数据")
        return []

    pc_users = [u for u in users if u[1] == "PC端"]
    if not pc_users:
        logger.error("没有PC端用户，无法生成PC端访问数据")
        return []

    logger.info(f"开始生成 {num_records} 条PC端访问数据（日期：{target_date}）...")
    shop_ids = list(set(p[3] for p in pages))
    records = []

    # 确保每个PC用户访问3-8个不同的店铺
    for user in pc_users:
        user_shops = random.sample(shop_ids, min(random.randint(3, 8), len(shop_ids)))
        first_visit = user[2]
        last_visit = user[3]
        time_diff = (last_visit - first_visit).total_seconds()

        for shop_id in user_shops:
            visit_time = first_visit + timedelta(seconds=random.uniform(0, time_diff))
            source_page = f"https://{fake.domain_name()}/{fake.uri_path()}"
            create_time = generate_random_time(target_date)
            records.append((user[0], shop_id, source_page, visit_time, create_time))

    # 补充剩余记录
    remaining_records = num_records - len(records)
    if remaining_records > 0:
        for _ in range(remaining_records):
            user = random.choice(pc_users)
            shop_id = random.choice(shop_ids)
            first_visit = user[2]
            last_visit = user[3]
            time_diff = (last_visit - first_visit).total_seconds()
            visit_time = first_visit + timedelta(seconds=random.uniform(0, time_diff))
            source_page = f"https://{fake.domain_name()}/{fake.uri_path()}"
            create_time = generate_random_time(target_date)
            records.append((user[0], shop_id, source_page, visit_time, create_time))

    logger.info(f"PC端访问数据生成完成，共 {len(records)} 条")
    return records


def generate_page_rank(wireless_records=[], pc_records=[], pages=[], target_date=None):
    """生成页面访问排行数据"""
    logger.info(f"开始生成页面访问排行数据（日期：{target_date}）...")
    page_to_shop = {p[0]: p[3] for p in pages}
    all_visits = []

    for record in wireless_records:
        all_visits.append((record[1], record[2]))  # (page_id, visit_time)

    for record in pc_records:
        shop_id = record[1]
        shop_pages = [p[0] for p in pages if p[3] == shop_id]
        if shop_pages:
            page_id = random.choice(shop_pages)
            all_visits.append((page_id, record[3]))  # (page_id, visit_time)

    if not all_visits:
        logger.warning("没有访问记录可用于生成页面排行")
        return []

    rank_data = {}
    for page_id, visit_time in all_visits:
        date_key = visit_time.date()
        key = (page_id, date_key)
        rank_data[key] = rank_data.get(key, 0) + 1

    rank_records = [(page, date_key, count, generate_random_time(target_date))
                    for (page, date_key), count in rank_data.items()]
    logger.info(f"已生成 {len(rank_records)} 条页面访问排行数据")
    return rank_records


def batch_insert(connection, table, columns, data, batch_size=100):
    """批量插入数据到数据库"""
    if not data:
        logger.warning(f"没有数据可插入到表 {table}")
        return 0

    start_time = time.time()
    total_records = len(data)
    logger.info(f"开始向表 {table} 插入 {total_records} 条数据...")

    try:
        with connection.cursor() as cursor:
            placeholders = ', '.join(['%s'] * len(columns))
            column_names = ', '.join(columns)
            sql = f"INSERT INTO {table} ({column_names}) VALUES ({placeholders})"

            inserted = 0
            for i in range(0, total_records, batch_size):
                batch = data[i:i + batch_size]
                cursor.executemany(sql, batch)
                connection.commit()
                inserted += len(batch)
                logger.info(f"已提交 {inserted}/{total_records} 条记录")

            logger.info(f"成功向表 {table} 插入 {total_records} 条数据, 耗时: {time.time() - start_time:.2f}秒")
            return total_records

    except Exception as e:
        connection.rollback()
        logger.error(f"插入数据到表 {table} 时出错: {str(e)}")
        raise


def main():
    start_time = time.time()
    logger.info("===== 开始数据生成过程 =====")
    connection = None

    try:
        # 创建数据库连接
        connection = create_db_connection()
        logger.info("数据库连接成功")

        # 让用户输入目标日期
        target_date = input_target_date()
        logger.info(f"正在生成 {target_date} 的数据...")

        # 1. 生成用户数据 (1000条)
        users = generate_users(1000, target_date)
        batch_insert(connection, 'user_info',
                     ['user_id', 'platform_type', 'first_visit_time', 'last_visit_time', 'create_time'],
                     users)

        # 2. 生成页面数据 (500条)
        pages = generate_pages(500, target_date)
        batch_insert(connection, 'page_base_info',
                     ['page_id', 'page_type', 'page_name', 'shop_id', 'product_id', 'page_create_time', 'create_time'],
                     pages)

        # 3. 生成无线端访问数据 (5000条)
        wireless_records = generate_wireless_access(5000, users, pages, target_date)
        batch_insert(connection, 'wireless_access_data',
                     ['user_id', 'page_id', 'visit_time', 'is_order', 'create_time'],
                     wireless_records)

        # 4. 生成店内路径数据 (3000条)
        instore_paths = generate_instore_paths(3000, wireless_records, target_date)
        batch_insert(connection, 'instore_path_flow',
                     ['user_id', 'source_page_id', 'target_page_id', 'jump_time', 'create_time'],
                     instore_paths)

        # 5. 生成PC端访问数据 (3000条)
        pc_records = generate_pc_access(3000, users, pages, target_date)
        batch_insert(connection, 'pc_source_base',
                     ['user_id', 'shop_id', 'source_page', 'visit_time', 'create_time'],
                     pc_records)

        # 6. 生成页面访问排行数据
        page_ranks = generate_page_rank(wireless_records, pc_records, pages, target_date)
        batch_insert(connection, 'page_visit_rank_base',
                     ['page_id', 'stat_date', 'visit_count', 'create_time'],
                     page_ranks)

        total_time = time.time() - start_time
        logger.info(f"===== 数据生成完成! 总耗时: {total_time:.2f}秒 =====")

    except Exception as e:
        logger.error(f"程序执行出错: {str(e)}")
        if connection:
            connection.rollback()
    finally:
        if connection:
            connection.close()
            logger.info("数据库连接已关闭")


if __name__ == "__main__":
    main()