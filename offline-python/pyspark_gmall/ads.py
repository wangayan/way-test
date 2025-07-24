from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HiveIntegration") \
    .master("local[*]") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.sql.warehouse.dir", "/home/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("info")
date = "20250717"

spark.sql("use pygmall;")
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict;")


spark.sql(f"""
insert into table dwd_trade_cart_add_inc partition (dt)
select
    id,
    user_id,
    sku_id,
    date_format(create_time, 'yyyy-MM-dd') date_id,
    create_time,
    sku_num,
    '{date}' as dt
from ods_cart_info
where ds = '{date}';
""")

spark.sql(f"""
insert overwrite table ads_user_action
select * from ads_user_action
union
select
    '{date}' dt,
    home_count,
    good_detail_count,
    cart_count,
    order_count,
    payment_count
from
(
    select
        1 recent_days,
        sum(if(page_id='home',1,0)) home_count,
        sum(if(page_id='good_detail',1,0)) good_detail_count
    from dws_traffic_page_visitor_page_view_1d
    where dt='{date}'
    and page_id in ('home','good_detail')
)page
join
(
    select
        1 recent_days,
        count(*) cart_count
    from dws_trade_user_cart_add_1d
    where dt='{date}'
)cart
on page.recent_days=cart.recent_days
join
(
    select
        1 recent_days,
        count(*) order_count
    from dws_trade_user_order_1d
    where dt='{date}'
)ord
on page.recent_days=ord.recent_days
join
(
    select
        1 recent_days,
        count(*) payment_count
    from dws_trade_user_payment_1d
    where dt='{date}'
)pay
on page.recent_days=pay.recent_days;
""")

spark.sql(f"""
insert overwrite table ads_order_continuously_user_count
select * from ads_order_continuously_user_count
union
select
    '{date}',
    7,
    count(distinct(user_id))
from
(
    select
        user_id,
        datediff(lead(dt,2,'{date}') over(partition by user_id order by dt),dt) diff
    from dws_trade_user_order_1d
    where dt>=date_add('{date}',-6)
)t1
where diff=2;
""")

spark.sql(f"""
insert overwrite table ads_repeat_purchase_by_tm
select * from ads_repeat_purchase_by_tm
union
select
    '{date}',
    30,
    tm_id,
    tm_name,
    cast(sum(if(order_count>=2,1,0))/sum(if(order_count>=1,1,0)) as decimal(16,2))
from
(
    select
        user_id,
        tm_id,
        tm_name,
        sum(order_count_30d) order_count
    from dws_trade_user_sku_order_nd
    where dt='{date}'
    group by user_id, tm_id,tm_name
)t1
group by tm_id,tm_name;
""")

spark.sql(f"""
insert overwrite table ads_order_stats_by_tm
select * from ads_order_stats_by_tm
union
select
    '{date}' dt,
    recent_days,
    tm_id,
    tm_name,
    order_count,
    order_user_count
from
(
    select
        1 recent_days,
        tm_id,
        tm_name,
        sum(order_count_1d) order_count,
        count(distinct(user_id)) order_user_count
    from dws_trade_user_sku_order_1d
    where dt='{date}'
    group by tm_id,tm_name
    union all
    select
        recent_days,
        tm_id,
        tm_name,
        sum(order_count),
        count(distinct(if(order_count>0,user_id,null)))
    from
    (
        select
            recent_days,
            user_id,
            tm_id,
            tm_name,
            case recent_days
                when 7 then order_count_7d
                when 30 then order_count_30d
            end order_count
        from dws_trade_user_sku_order_nd lateral view explode(array(7,30)) tmp as recent_days
        where dt='{date}'
    )t1
    group by recent_days,tm_id,tm_name
)odr;
""")


# ads_order_stats_by_cate
spark.sql(f"""
insert overwrite table ads_order_stats_by_cate
select * from ads_order_stats_by_cate
union
select
    '{date}' dt,
    recent_days,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    order_count,
    order_user_count
from
(
    select
        1 recent_days,
        category1_id,
        category1_name,
        category2_id,
        category2_name,
        category3_id,
        category3_name,
        sum(order_count_1d) order_count,
        count(distinct(user_id)) order_user_count
    from dws_trade_user_sku_order_1d
    where dt='{date}'
    group by category1_id,category1_name,category2_id,category2_name,category3_id,category3_name
    union all
    select
        recent_days,
        category1_id,
        category1_name,
        category2_id,
        category2_name,
        category3_id,
        category3_name,
        sum(order_count),
        count(distinct(if(order_count>0,user_id,null)))
    from
    (
        select
            recent_days,
            user_id,
            category1_id,
            category1_name,
            category2_id,
            category2_name,
            category3_id,
            category3_name,
            case recent_days
                when 7 then order_count_7d
                when 30 then order_count_30d
            end order_count
        from dws_trade_user_sku_order_nd lateral view explode(array(7,30)) tmp as recent_days
        where dt='{date}'
    )t1
    group by recent_days,category1_id,category1_name,category2_id,category2_name,category3_id,category3_name
)odr;
""")

spark.sql("set hive.mapjoin.optimized.hashtable=false;")
spark.sql("set hive.mapjoin.optimized.hashtable=true;")

# ads_sku_cart_num_top3_by_cate
spark.sql(f"""
insert overwrite table ads_sku_cart_num_top3_by_cate
select * from ads_sku_cart_num_top3_by_cate
union
select
    '{date}' dt,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    sku_id,
    sku_name,
    cart_num,
    rk
from
(
    select
        sku_id,
        sku_name,
        category1_id,
        category1_name,
        category2_id,
        category2_name,
        category3_id,
        category3_name,
        cart_num,
        rank() over (partition by category1_id,category2_id,category3_id order by cart_num desc) rk
    from
    (
        select
            sku_id,
            sum(sku_num) cart_num
        from dwd_trade_cart_full
        where dt='{date}'
        group by sku_id
    )cart
    left join
    (
        select
            id,
            sku_name,
            category1_id,
            category1_name,
            category2_id,
            category2_name,
            category3_id,
            category3_name
        from dim_sku_full
        where dt='{date}'
    )sku
    on cart.sku_id=sku.id
)t1
where rk<=3;
""")

spark.sql(f"""
insert overwrite table ads_sku_favor_count_top3_by_tm
select * from ads_sku_favor_count_top3_by_tm
union
select
    '{date}' dt,
    tm_id,
    tm_name,
    sku_id,
    sku_name,
    favor_add_count_1d,
    rk
from
(
    select
        tm_id,
        tm_name,
        sku_id,
        sku_name,
        favor_add_count_1d,
        rank() over (partition by tm_id order by favor_add_count_1d desc) rk
    from dws_interaction_sku_favor_add_1d
    where dt='{date}'
)t1
where rk<=3;
""")

spark.sql(f"""
insert overwrite table ads_order_to_pay_interval_avg
select * from ads_order_to_pay_interval_avg
union
select
    '{date}',
    cast(avg(to_unix_timestamp(payment_time)-to_unix_timestamp(order_time)) as bigint)
from dwd_trade_trade_flow_acc
where dt in ('{date}')
and payment_date_id='{date}';
""")

spark.sql(f"""
insert overwrite table ads_order_by_province
select * from ads_order_by_province
union
select
    '{date}' dt,
    1 recent_days,
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    order_count_1d,
    order_total_amount_1d
from dws_trade_province_order_1d
where dt='{date}'
union
select
    '{date}' dt,
    recent_days,
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    case recent_days
        when 7 then order_count_7d
        when 30 then order_count_30d
    end order_count,
    case recent_days
        when 7 then order_total_amount_7d
        when 30 then order_total_amount_30d
    end order_total_amount
from dws_trade_province_order_nd lateral view explode(array(7,30)) tmp as recent_days
where dt='{date}';
""")

spark.stop()