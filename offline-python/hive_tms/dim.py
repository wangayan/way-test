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
date = "20250719"


spark.sql("use tms01;")

spark.sql(f"""
insert overwrite table tms.dim_complex_full partition (dt = '{date}')
select complex_info.id as id,
    complex_name,
    courier_emp_id,
    province_id,
    dic_for_prov.name province_name,
    city_id,
    dic_for_city.name city_name,
    district_id,
    district_name
from (select id,
            complex_name,
            province_id,
            city_id,
            district_id,
            district_name
      from ods_base_complex
        where ds = '{date}'
        and is_deleted = '0') complex_info
join (select id,
            name
    from ods_base_region_info
    where ds = '{date}'
    and is_deleted = '0') dic_for_prov
    on complex_info.province_id = dic_for_prov.id
join (select id,
        name
    from ods_base_region_info
    where ds = '{date}'
    and is_deleted = '0') dic_for_city
    on complex_info.city_id = dic_for_city.id
left join (select courier_emp_id,
                  complex_id
    from ods_express_courier_complex
    where ds = '{date}'
    and is_deleted = '0') complex_courier
on complex_info.id = complex_courier.complex_id;
""")

spark.sql(f"""
insert overwrite table tms.dim_organ_full
    partition (dt = '{date}')
select organ_info.id,
       organ_info.org_name,
       org_level,
       region_id,
       region_info.name        region_name,
       region_info.dict_code   region_code,
       org_parent_id,
       org_for_parent.org_name org_parent_name
from (select id,
             org_name,
             org_level,
             region_id,
             org_parent_id
      from ods_base_organ
      where ds = '{date}'
        and is_deleted = '0') organ_info
         left join (
    select id,
           name,
           dict_code
    from ods_base_region_info
    where ds = '{date}'
      and is_deleted = '0'
) region_info
                   on organ_info.region_id = region_info.id
         left join (
    select id,
           org_name
    from ods_base_organ
    where ds = '{date}'
      and is_deleted = '0'
) org_for_parent
                   on organ_info.org_parent_id = org_for_parent.id;
""")


spark.sql(f"""
insert overwrite table dim_region_full
    partition (dt = '{date}')
select id,
       parent_id,
       name,
       dict_code,
       short_name
from ods_base_region_info
where ds = '{date}'
  and is_deleted = '0';
""")

spark.sql(f"""
insert overwrite table tms.dim_express_courier_full
    partition (dt = '{date}')
select express_cor_info.id,
       emp_id,
       org_id,
       org_name,
       working_phone,
       express_type,
       dic_info.name express_type_name
from (select id,
             emp_id,
             org_id,
             md5(working_phone) working_phone,
             express_type
      from ods_express_courier
      where ds = '{date}'
        and is_deleted = '0') express_cor_info
         join (
    select id,
           org_name
    from ods_base_organ
    where ds = '{date}'
      and is_deleted = '0'
) organ_info
              on express_cor_info.org_id = organ_info.id
         join (
    select id,
           name
    from ods_base_dic
    where ds = '{date}'
      and is_deleted = '0'
) dic_info
              on express_type = dic_info.id;
""")

spark.sql(f"""
insert overwrite table tms.dim_shift_full
    partition (dt = '{date}')
select shift_info.id,
       line_id,
       line_info.name line_name,
       line_no,
       line_level,
       org_id,
       transport_line_type_id,
       dic_info.name  transport_line_type_name,
       start_org_id,
       start_org_name,
       end_org_id,
       end_org_name,
       pair_line_id,
       distance,
       cost,
       estimated_time,
       start_time,
       driver1_emp_id,
       driver2_emp_id,
       truck_id,
       pair_shift_id
from (select id,
             line_id,
             start_time,
             driver1_emp_id,
             driver2_emp_id,
             truck_id,
             pair_shift_id
      from ods_line_base_shift
      where ds = '{date}'
        and is_deleted = '0') shift_info
         join
     (select id,
             name,
             line_no,
             line_level,
             org_id,
             transport_line_type_id,
             start_org_id,
             start_org_name,
             end_org_id,
             end_org_name,
             pair_line_id,
             distance,
             cost,
             estimated_time
      from ods_line_base_info
      where ds = '{date}'
        and is_deleted = '0') line_info
     on shift_info.line_id = line_info.id
         join (
    select id,
           name
    from ods_base_dic
    where ds = '{date}'
      and is_deleted = '0'
) dic_info on line_info.transport_line_type_id = dic_info.id;
""")


spark.sql(f"""
insert overwrite table tms.dim_truck_driver_full
    partition (dt = '{date}')
select driver_info.id,
       emp_id,
       org_id,
       organ_info.org_name,
       team_id,
       team_info.name team_name,
       license_type,
       init_license_date,
       expire_date,
       license_no,
       is_enabled
from (select id,
             emp_id,
             org_id,
             team_id,
             license_type,
             init_license_date,
             expire_date,
             license_no,
             is_enabled
      from ods_truck_driver
      where ds = '{date}'
        and is_deleted = '0') driver_info
         join (
    select id,
           org_name
    from ods_base_organ
    where ds = '{date}'
      and is_deleted = '0'
) organ_info
              on driver_info.org_id = organ_info.id
         join (
    select id,
           name
    from ods_truck_team
    where ds = '{date}'
      and is_deleted = '0'
) team_info
              on driver_info.team_id = team_info.id;
""")

spark.sql(f"""
insert overwrite table tms.dim_truck_full
    partition (dt = '{date}')
select truck_info.id,
       team_id,
       team_info.name     team_name,
       team_no,
       org_id,
       org_name,
       manager_emp_id,
       truck_no,
       truck_model_id,
       model_name         truck_model_name,
       model_type         truck_model_type,
       dic_for_type.name  truck_model_type_name,
       model_no           truck_model_no,
       brand              truck_brand,
       dic_for_brand.name truck_brand_name,
       truck_weight,
       load_weight,
       total_weight,
       eev,
       boxcar_len,
       boxcar_wd,
       boxcar_hg,
       max_speed,
       oil_vol,
       device_gps_id,
       engine_no,
       license_registration_date,
       license_last_check_date,
       license_expire_date,
       is_enabled
from (select id,
             team_id,

             md5(truck_no) truck_no,
             truck_model_id,

             device_gps_id,
             engine_no,
             license_registration_date,
             license_last_check_date,
             license_expire_date,
             is_enabled
      from ods_truck_info
      where ds = '{date}'
        and is_deleted = '0') truck_info
         join
     (select id,
             name,
             team_no,
             org_id,

             manager_emp_id
      from ods_truck_team
      where ds = '{date}'
        and is_deleted = '0') team_info
     on truck_info.team_id = team_info.id
         join
     (select id,
             model_name,
             model_type,

             model_no,
             brand,

             truck_weight,
             load_weight,
             total_weight,
             eev,
             boxcar_len,
             boxcar_wd,
             boxcar_hg,
             max_speed,
             oil_vol
      from ods_truck_model
      where ds = '{date}'
        and is_deleted = '0') model_info
     on truck_info.truck_model_id = model_info.id
         join
     (select id,
             org_name
      from ods_base_organ
      where ds = '{date}'
        and is_deleted = '0'
     ) organ_info
     on org_id = organ_info.id
         join
     (select id,
             name
      from ods_base_dic
      where ds = '{date}'
        and is_deleted = '0') dic_for_type
     on model_info.model_type = dic_for_type.id
         join
     (select id,
             name
      from ods_base_dic
      where ds = '{date}'
        and is_deleted = '0') dic_for_brand
     on model_info.brand = dic_for_brand.id;

""")

spark.sql(f"""
insert overwrite table dim_user_zip
    partition (dt = '9999-12-31')
select id,
       login_name,
       nick_name,
       md5(passwd)  as passwd,
       md5(real_name)                                                                                 realname,
       md5(if(phone_num regexp '^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$',
              phone_num, null))   as phone_num,
       md5(if(email regexp '^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\\.[a-zA-Z0-9_-]+)+$', email, null)) as email,
       user_level,
       date_add('1970-01-01', cast(birthday as int))  as birthday,
       gender,
       date_format(from_utc_timestamp(
                           cast(create_time as bigint), 'UTC'),
                   'yyyy-MM-dd')                                                                            start_date,
       '9999-12-31'                                                                                         end_date
from ods_user_info
where ds = '{date}'
  and is_deleted = '0';
""")


spark.sql("set hive.exec.dynamic.partition.mode=nonstrict;")

spark.sql(f"""
insert overwrite table dim_user_zip
    partition (dt)
select id,
       login_name,
       nick_name,
       passwd,
       real_name,
       phone_num,
       email,
       user_level,
       birthday,
       gender,
       start_date,
       if(rk = 1, end_date, date_add('2025-07-10', -1))as end_date,
       if(rk = 1, end_date, date_add('2025-07-10', -1)) dt
from (select id,
             login_name,
             nick_name,
             passwd,
             real_name,
             phone_num,
             email,
             user_level,
             birthday,
             gender,
             start_date,
             end_date,
             row_number() over (partition by id order by start_date desc) rk
      from (select id,
                   login_name,
                   nick_name,
                   passwd,
                   real_name,
                   phone_num,
                   email,
                   user_level,
                   birthday,
                   gender,
                   start_date,
                   end_date
            from dim_user_zip
            where dt = '9999-12-31'
            union
            select id,
                   login_name,
                   nick_name,
                   md5(passwd)                                                                              passwd,
                   md5(real_name)                                                                           realname,
                   md5(if(phone_num regexp
                          '^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$',
                          phone_num, null))                                                                 phone_num,
                   md5(if(email regexp '^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\\.[a-zA-Z0-9_-]+)+$', email, null)) email,
                   user_level,
                   cast(date_add('1970-01-01', cast(birthday as int)) as string)                            birthday,
                   gender,
                   '2025-07-10'                                                                             start_date,
                   '9999-12-31'                                                                             end_date
            from (select id,
                         login_name,
                         nick_name,
                         passwd,
                         real_name,
                         phone_num,
                         email,
                         user_level,
                         birthday,
                         gender,
                         row_number() over (partition by id order by ds desc) rn
                  from ods_user_info
                  where ds = '{date}'
                    and is_deleted = '0'
                 ) inc
            where rn = 1) full_info) final_info;
""")

spark.sql(f"""
insert overwrite table dim_user_address_zip
    partition (dt = '9999-12-31')
select id,
       user_id,
       md5(if(phone regexp
              '^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$',
              phone, null)) as phone,
       province_id,
       city_id,
       district_id,
       complex_id,
       address,
       is_default,
       concat(substr(create_time, 1, 10), ' ',
              substr(create_time, 12, 8)) start_date,
       '9999-12-31'                             end_date
from ods_user_address
where ds = '{date}'
  and is_deleted = '0';
""")


spark.sql("set hive.exec.dynamic.partition.mode=nonstrict;")

spark.sql(f"""
insert overwrite table dim_user_address_zip
    partition (dt)
select id,
       user_id,
       phone,
       province_id,
       city_id,
       district_id,
       complex_id,
       address,
       is_default,
       start_date,
       if(rk = 1, end_date, date_add('2025-07-11', -1)) as end_date,
       if(rk = 1, end_date, date_add('2025-07-11', -1)) dt
from (select id,
             user_id,
             phone,
             province_id,
             city_id,
             district_id,
             complex_id,
             address,
             is_default,
             start_date,
             end_date,
             row_number() over (partition by id order by start_date desc) rk
      from (select id,
                   user_id,
                   phone,
                   province_id,
                   city_id,
                   district_id,
                   complex_id,
                   address,
                   is_default,
                   start_date,
                   end_date
            from dim_user_address_zip
            where dt = '9999-12-31'
            union
            select id,
                   user_id,
                   phone,
                   province_id,
                   city_id,
                   district_id,
                   complex_id,
                   address,
                   is_default,
                   '2025-07-11' start_date,
                   '9999-12-31' end_date
            from (select id,
                         user_id,
                         phone,
                         province_id,
                         city_id,
                         district_id,
                         complex_id,
                         address,
                         cast(is_default as tinyint)                          is_default,
                         row_number() over (partition by id order by ds desc) rn
                  from ods_user_address
                  where ds = '{date}'
                    and is_deleted = '0') inc
            where rn = 1
           ) union_info
     ) with_rk;
""")

spark.stop()