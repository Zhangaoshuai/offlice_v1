DROP TABLE IF EXISTS ads_new_order_user_stats;
CREATE EXTERNAL TABLE ads_new_order_user_stats
(
    `ds`                   STRING COMMENT '统计日期',
    `recent_days`          BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `new_order_user_count` BIGINT COMMENT '新增下单人数'
) COMMENT '新增下单用户统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_new_order_user_stats/';

insert overwrite table ads_new_order_user_stats
select * from ads_new_order_user_stats
union
select
    '20250930' ds,
    recent_days,
    count(*) new_order_user_count
from dws_trade_user_order_td lateral view explode(array(1,7,30)) tmp as recent_days
where ds='20250930'
  and order_date_first>=date_add('20250930',-recent_days+1)
group by recent_days;


insert overwrite table ads_repeat_purchase_by_tm
select * from ads_repeat_purchase_by_tm
union
select
    '20250917',
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
        where ds='20250917'
        group by user_id, tm_id,tm_name
    )t1
group by tm_id,tm_name;


-- DROP TABLE IF EXISTS dim_user_zip;
-- CREATE EXTERNAL TABLE dim_user_zip
-- (
--     `id`           STRING COMMENT '用户ID',
--     `name`         STRING COMMENT '用户姓名',
--     `phone_num`    STRING COMMENT '手机号码',
--     `email`        STRING COMMENT '邮箱',
--     `user_level`   STRING COMMENT '用户等级',
--     `birthday`     STRING COMMENT '生日',
--     `gender`       STRING COMMENT '性别',
--     `create_time`  STRING COMMENT '创建时间',
--     `operate_time` STRING COMMENT '操作时间',
--     `start_date`   STRING COMMENT '开始日期',
--     `end_date`     STRING COMMENT '结束日期'
-- ) COMMENT '用户维度表'
--     PARTITIONED BY (`ds` STRING)
--     STORED AS ORC
--     LOCATION '/warehouse/gmall/dim/dim_user_zip/'
--     TBLPROPERTIES ('orc.compress' = 'snappy');

DROP TABLE IF EXISTS tmp_dim_date_info;
CREATE EXTERNAL TABLE tmp_dim_date_info (
                                            `date_id`       STRING COMMENT '日',
                                            `week_id`       STRING COMMENT '周ID',
                                            `week_day`      STRING COMMENT '周几',
                                            `day`            STRING COMMENT '每月的第几天',
                                            `month`          STRING COMMENT '第几月',
                                            `quarter`       STRING COMMENT '第几季度',
                                            `year`           STRING COMMENT '年',
                                            `is_workday`    STRING COMMENT '是否是工作日',
                                            `holiday_id`    STRING COMMENT '节假日'
) COMMENT '时间维度表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION 'hdfs://cdh01:8020/warehouse/gmall/tmp/tmp_dim_date_info/';

DROP TABLE IF EXISTS dim_date;
CREATE EXTERNAL TABLE dim_date
(
    `date_id`    STRING COMMENT '日期ID',
    `week_id`    STRING COMMENT '周ID,一年中的第几周',
    `week_day`   STRING COMMENT '周几',
    `day`         STRING COMMENT '每月的第几天',
    `month`       STRING COMMENT '一年中的第几月',
    `quarter`    STRING COMMENT '一年中的第几季度',
    `year`        STRING COMMENT '年份',
    `is_workday` STRING COMMENT '是否是工作日',
    `holiday_id` STRING COMMENT '节假日'
) COMMENT '日期维度表'
    STORED AS ORC
    LOCATION '/warehouse/gmall/dim/dim_date/'
    TBLPROPERTIES ('orc.compress' = 'snappy');
insert overwrite table dim_date select * from tmp_dim_date_info;
select * from dim_date;


insert overwrite table ads_user_stats
select * from ads_user_stats
union
select '20250918' ds,
       recent_days,
       sum(if(login_date_first >= date_add('20250918', -recent_days + 1), 1, 0)) new_user_count,
       count(*) active_user_count
from dws_user_user_login_td lateral view explode(array(1, 7, 30)) tmp as recent_days
where ds = '20250918'
  and login_date_last >= date_add('20250918', -recent_days + 1)
group by recent_days;

insert overwrite table ads_user_action
select * from ads_user_action
union
select
    '20250917' ds,
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
        where ds='20250917'
          and page_id in ('home','good_detail')
    )page
        join
    (
        select
            1 recent_days,
            count(*) cart_count
        from dws_trade_user_cart_add_1d
        where ds='20250917'
    )cart
    on page.recent_days=cart.recent_days
        join
    (
        select
            1 recent_days,
            count(*) order_count
        from dws_trade_user_order_1d
        where ds='2025-08-14'
    )ord
    on page.recent_days=ord.recent_days
        join
    (
        select
            1 recent_days,
            count(*) payment_count
        from dws_trade_user_payment_1d
        where ds='2025-08-14'
    )pay
    on page.recent_days=pay.recent_days;


insert overwrite table ads_user_retention
select * from ads_user_retention
union
select '20250918' ds,
       login_date_first create_date,
       datediff('2025-08-14', login_date_first) retention_day,
       sum(if(login_date_last = '20250918', 1, 0)) retention_count,
       count(*) new_user_count,
       cast(sum(if(login_date_last = '20250918', 1, 0)) / count(*) * 100 as decimal(16, 2)) retention_rate
from (
         select user_id,
                login_date_last,
                login_date_first
         from dws_user_user_login_td
         where ds = '20250918'
           and login_date_first >= date_add('2025-08-14', -7)
           and login_date_first < '2025-08-14'
     ) t1
group by login_date_first;

insert overwrite table dws_trade_province_order_nd partition(ds='2025-08-14')
select
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    sum(if(ds>=date_add('2025-09-17',-6),order_count_1d,0)),
    sum(if(ds>=date_add('2025-09-17',-6),order_original_amount_1d,0)),
    sum(if(ds>=date_add('2025-09-17',-6),activity_reduce_amount_1d,0)),
    sum(if(ds>=date_add('2025-09-17',-6),coupon_reduce_amount_1d,0)),
    sum(if(ds>=date_add('2025-09-17',-6),order_total_amount_1d,0)),
    sum(order_count_1d),
    sum(order_original_amount_1d),
    sum(activity_reduce_amount_1d),
    sum(coupon_reduce_amount_1d),
    sum(order_total_amount_1d)
from dws_trade_province_order_1d
where ds>=date_add('2025-08-14',-29)
  and ds<='2025-08-14'
group by province_id,province_name,area_code,iso_code,iso_3166_2;


set hive.exec.dynamic.partition.mode=nonstrict;
-- insert overwrite table dwd_trade_cart partition(ds='20250917')
-- select
--     id,
--     user_id,
--     sku_id,
--     sku_name,
--     sku_num
-- from ods_cart_info
-- where ds='20250917'
--   and is_ordered='0';

DROP TABLE IF EXISTS dws_trade_province_order_nd;
CREATE EXTERNAL TABLE dws_trade_province_order_nd
(
    `province_id`                STRING COMMENT '省份ID',
    `province_name`              STRING COMMENT '省份名称',
    `area_code`                  STRING COMMENT '地区编码',
    `iso_code`                   STRING COMMENT '旧版国际标准地区编码',
    `iso_3166_2`                 STRING COMMENT '新版国际标准地区编码',
    `order_count_7d`             BIGINT COMMENT '最近7日下单次数',
    `order_original_amount_7d`   DECIMAL(16, 2) COMMENT '最近7日下单原始金额',
    `activity_reduce_amount_7d`  DECIMAL(16, 2) COMMENT '最近7日下单活动优惠金额',
    `coupon_reduce_amount_7d`    DECIMAL(16, 2) COMMENT '最近7日下单优惠券优惠金额',
    `order_total_amount_7d`      DECIMAL(16, 2) COMMENT '最近7日下单最终金额',
    `order_count_30d`            BIGINT COMMENT '最近30日下单次数',
    `order_original_amount_30d`  DECIMAL(16, 2) COMMENT '最近30日下单原始金额',
    `activity_reduce_amount_30d` DECIMAL(16, 2) COMMENT '最近30日下单活动优惠金额',
    `coupon_reduce_amount_30d`   DECIMAL(16, 2) COMMENT '最近30日下单优惠券优惠金额',
    `order_total_amount_30d`     DECIMAL(16, 2) COMMENT '最近30日下单最终金额'
) COMMENT '交易域省份粒度订单最近n日汇总表'
    PARTITIONED BY (`ds` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_province_order_nd'
    TBLPROPERTIES ('orc.compress' = 'snappy');

DROP TABLE IF EXISTS dws_trade_province_order_nd;
CREATE EXTERNAL TABLE dws_trade_province_order_nd
(
    `province_id`                STRING COMMENT '省份ID',
    `province_name`              STRING COMMENT '省份名称',
    `area_code`                  STRING COMMENT '地区编码',
    `iso_code`                   STRING COMMENT '旧版国际标准地区编码',
    `iso_3166_2`                 STRING COMMENT '新版国际标准地区编码',
    `order_count_7d`             BIGINT COMMENT '最近7日下单次数',
    `order_original_amount_7d`   DECIMAL(16, 2) COMMENT '最近7日下单原始金额',
    `activity_reduce_amount_7d`  DECIMAL(16, 2) COMMENT '最近7日下单活动优惠金额',
    `coupon_reduce_amount_7d`    DECIMAL(16, 2) COMMENT '最近7日下单优惠券优惠金额',
    `order_total_amount_7d`      DECIMAL(16, 2) COMMENT '最近7日下单最终金额',
    `order_count_30d`            BIGINT COMMENT '最近30日下单次数',
    `order_original_amount_30d`  DECIMAL(16, 2) COMMENT '最近30日下单原始金额',
    `activity_reduce_amount_30d` DECIMAL(16, 2) COMMENT '最近30日下单活动优惠金额',
    `coupon_reduce_amount_30d`   DECIMAL(16, 2) COMMENT '最近30日下单优惠券优惠金额',
    `order_total_amount_30d`     DECIMAL(16, 2) COMMENT '最近30日下单最终金额'
) COMMENT '交易域省份粒度订单最近n日汇总表'
    PARTITIONED BY (`ds` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_province_order_nd'
    TBLPROPERTIES ('orc.compress' = 'snappy');

insert overwrite table dws_trade_province_order_nd partition(ds='2025-08-14')
select
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    sum(if(ds>=date_add('20250918',-6),order_count_1d,0)),
    sum(if(ds>=date_add('20250918',-6),order_original_amount_1d,0)),
    sum(if(ds>=date_add('20250918',-6),activity_reduce_amount_1d,0)),
    sum(if(ds>=date_add('20250918',-6),coupon_reduce_amount_1d,0)),
    sum(if(ds>=date_add('20250918',-6),order_total_amount_1d,0)),
    sum(order_count_1d),
    sum(order_original_amount_1d),
    sum(activity_reduce_amount_1d),
    sum(coupon_reduce_amount_1d),
    sum(order_total_amount_1d)
from dws_trade_province_order_1d
where ds>=date_add('2025-08-14',-29)
  and ds<='2025-08-14'
group by province_id,province_name,area_code,iso_code,iso_3166_2;


DROP TABLE IF EXISTS dws_trade_user_order_1d;
CREATE EXTERNAL TABLE dws_trade_user_order_1d
(
    `user_id`                   STRING COMMENT '用户ID',
    `order_count_1d`            BIGINT COMMENT '最近1日下单次数',
    `order_num_1d`              BIGINT COMMENT '最近1日下单商品件数',
    `order_original_amount_1d`  DECIMAL(16, 2) COMMENT '最近1日下单原始金额',
    `activity_reduce_amount_1d` DECIMAL(16, 2) COMMENT '最近1日下单活动优惠金额',
    `coupon_reduce_amount_1d`   DECIMAL(16, 2) COMMENT '最近1日下单优惠券优惠金额',
    `order_total_amount_1d`     DECIMAL(16, 2) COMMENT '最近1日下单最终金额'
) COMMENT '交易域用户粒度订单最近1日汇总表'
    PARTITIONED BY (`ds` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_user_order_1d/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trade_province_order_1d partition(ds)
select
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    order_count_1d,
    order_original_amount_1d,
    activity_reduce_amount_1d,
    coupon_reduce_amount_1d,
    order_total_amount_1d,
    ds
from
    (
        select
            province_id,
            count(distinct(order_id)) order_count_1d,
            sum(split_original_amount) order_original_amount_1d,
            sum(nvl(split_activity_amount,0)) activity_reduce_amount_1d,
            sum(nvl(split_coupon_amount,0)) coupon_reduce_amount_1d,
            sum(split_total_amount) order_total_amount_1d,
            ds
        from dwd_trade_order_detail
        group by province_id,ds
    )o
        left join
    (
        select
            id,
            province_name,
            area_code,
            iso_code,
            iso_3166_2
        from dim_province
        where ds='20250917'
    )p
    on o.province_id=p.id;



DROP TABLE IF EXISTS dws_trade_user_sku_order_nd;
CREATE EXTERNAL TABLE dws_trade_user_sku_order_nd
(
    `user_id`                     STRING COMMENT '用户ID',
    `sku_id`                      STRING COMMENT 'SKU_ID',
    `sku_name`                    STRING COMMENT 'SKU名称',
    `category1_id`               STRING COMMENT '一级品类ID',
    `category1_name`             STRING COMMENT '一级品类名称',
    `category2_id`               STRING COMMENT '二级品类ID',
    `category2_name`             STRING COMMENT '二级品类名称',
    `category3_id`               STRING COMMENT '三级品类ID',
    `category3_name`             STRING COMMENT '三级品类名称',
    `tm_id`                       STRING COMMENT '品牌ID',
    `tm_name`                     STRING COMMENT '品牌名称',
    `order_count_7d`             STRING COMMENT '最近7日下单次数',
    `order_num_7d`               BIGINT COMMENT '最近7日下单件数',
    `order_original_amount_7d`   DECIMAL(16, 2) COMMENT '最近7日下单原始金额',
    `activity_reduce_amount_7d`  DECIMAL(16, 2) COMMENT '最近7日活动优惠金额',
    `coupon_reduce_amount_7d`    DECIMAL(16, 2) COMMENT '最近7日优惠券优惠金额',
    `order_total_amount_7d`      DECIMAL(16, 2) COMMENT '最近7日下单最终金额',
    `order_count_30d`            BIGINT COMMENT '最近30日下单次数',
    `order_num_30d`              BIGINT COMMENT '最近30日下单件数',
    `order_original_amount_30d`  DECIMAL(16, 2) COMMENT '最近30日下单原始金额',
    `activity_reduce_amount_30d` DECIMAL(16, 2) COMMENT '最近30日活动优惠金额',
    `coupon_reduce_amount_30d`   DECIMAL(16, 2) COMMENT '最近30日优惠券优惠金额',
    `order_total_amount_30d`     DECIMAL(16, 2) COMMENT '最近30日下单最终金额'
) COMMENT '交易域用户商品粒度订单最近n日汇总表'
    PARTITIONED BY (`ds` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_user_sku_order_nd/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

insert overwrite table dws_trade_user_sku_order_nd partition(ds='2025-08-14')
select
    user_id,
    sku_id,
    sku_name,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    tm_id,
    tm_name,
    sum(if(ds>=date_add('2025-08-14',-6),order_count_1d,0)),
    sum(if(ds>=date_add('2025-08-14',-6),order_num_1d,0)),
    sum(if(ds>=date_add('2025-08-14',-6),order_original_amount_1d,0)),
    sum(if(ds>=date_add('2025-08-14',-6),activity_reduce_amount_1d,0)),
    sum(if(ds>=date_add('2025-08-14',-6),coupon_reduce_amount_1d,0)),
    sum(if(ds>=date_add('2025-08-14',-6),order_total_amount_1d,0)),
    sum(order_count_1d),
    sum(order_num_1d),
    sum(order_original_amount_1d),
    sum(activity_reduce_amount_1d),
    sum(coupon_reduce_amount_1d),
    sum(order_total_amount_1d)
from dws_trade_user_sku_order_1d
where ds>=date_add('2025-08-14',-29)
group by  user_id,sku_id,sku_name,category1_id,category1_name,category2_id,category2_name,category3_id,category3_name,tm_id,tm_name;

DROP TABLE IF EXISTS dws_traffic_page_visitor_page_view_1d;
CREATE EXTERNAL TABLE dws_traffic_page_visitor_page_view_1d
(
    `mid_id`         STRING COMMENT '访客ID',
    `brand`          string comment '手机品牌',
    `model`          string comment '手机型号',
    `operate_system` string comment '操作系统',
    `page_id`        STRING COMMENT '页面ID',
    `during_time_1d` BIGINT COMMENT '最近1日浏览时长',
    `view_count_1d`  BIGINT COMMENT '最近1日访问次数'
) COMMENT '流量域访客页面粒度页面浏览最近1日汇总表'
    PARTITIONED BY (`ds` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_traffic_page_visitor_page_view_1d/'
    TBLPROPERTIES ('orc.compress' = 'snappy');
insert overwrite table dws_traffic_page_visitor_page_view_1d partition(ds='20250917')
select
    mid_id,
    brand,
    model,
    operate_system,
    page_id,
    sum(during_time),
    count(*)
from dwd_traffic_page_view
where ds='20250917'
group by mid_id,brand,model,operate_system,page_id;


DROP TABLE IF EXISTS dwd_traffic_page_view;
CREATE EXTERNAL TABLE dwd_traffic_page_view
(
    `province_id`    STRING COMMENT '省份ID',
    `brand`           STRING COMMENT '手机品牌',
    `channel`         STRING COMMENT '渠道',
    `is_new`          STRING COMMENT '是否首次启动',
    `model`           STRING COMMENT '手机型号',
    `mid_id`          STRING COMMENT '设备ID',
    `operate_system` STRING COMMENT '操作系统',
    `user_id`         STRING COMMENT '会员ID',
    `version_code`   STRING COMMENT 'APP版本号',
    `page_item`       STRING COMMENT '目标ID',
    `page_item_type` STRING COMMENT '目标类型',
    `last_page_id`    STRING COMMENT '上页ID',
    `page_id`          STRING COMMENT '页面ID ',
    `from_pos_id`     STRING COMMENT '点击坑位ID',
    `from_pos_seq`    STRING COMMENT '点击坑位位置',
    `refer_id`         STRING COMMENT '营销渠道ID',
    `date_id`          STRING COMMENT '日期ID',
    `view_time`       STRING COMMENT '跳入时间',
    `session_id`      STRING COMMENT '所属会话ID',
    `during_time`     BIGINT COMMENT '持续时间毫秒'
) COMMENT '流量域页面浏览事务事实表'
    PARTITIONED BY (`ds` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_traffic_page_view_inc'
    TBLPROPERTIES ('orc.compress' = 'snappy');
set hive.cbo.enable=false;
insert overwrite table dwd_traffic_page_view partition (ds='20250917')
select
    get_json_object(log, '$.common.ar') province_id,
    get_json_object(log, '$.common.ba') brand,
    get_json_object(log, '$.common.ch') channel,
    get_json_object(log, '$.common.is_new') is_new,
    get_json_object(log, '$.common.md') model,
    get_json_object(log, '$.common.mid') mid_id,
    get_json_object(log, '$.common.os') operate_system,
    get_json_object(log, '$.common.uid') user_id,
    get_json_object(log, '$.common.vc') version_code,
    get_json_object(log, '$.page.item') page_item,
    get_json_object(log, '$.page.item_type') page_item_type,
    get_json_object(log, '$.page.last_page_id'),
    get_json_object(log, '$.page.page_id'),
    get_json_object(log, '$.page.from_pos_id'),
    get_json_object(log, '$.page.from_pos_seq'),
    get_json_object(log, '$.page.refer_id'),
    date_format(from_utc_timestamp(get_json_object(log, '$.ts'),'GMT+8'),'yyyy-MM-dd') date_id,
    date_format(from_utc_timestamp(get_json_object(log, '$.ts'),'GMT+8'),'yyyy-MM-dd HH:mm:ss') view_time,
    get_json_object(log, '$.common.sid') session_id,
    get_json_object(log, '$.page.during_time')
from ods_z_log
where ds='20250917';
set hive.cbo.enable=true;


insert overwrite table dws_traffic_session_page_view_1d partition(ds='20250917')
select
    session_id,
    mid_id,
    brand,
    model,
    operate_system,
    version_code,
    channel,
    sum(during_time),
    count(*)
from dwd_traffic_page_view
where ds='20250917'
group by session_id,mid_id,brand,model,operate_system,version_code,channel;





insert overwrite table dws_user_user_login_td partition (ds = '20250918')
select u.id                                                         user_id,
       nvl(login_date_last, date_format(create_time, 'yyyy-MM-dd')) login_date_last,
       date_format(create_time, 'yyyy-MM-dd')                       login_date_first,
       nvl(login_count_td, 1)                                       login_count_td
from (
         select id,
                create_time
         from dim_user_zip
         where ds = '9999-12-31'
     ) u
         left join
     (
         select user_id,
                max(ds)  login_date_last,
                count(*) login_count_td
         from dwd_user_login
         group by user_id
     ) l
     on u.id = l.user_id;



set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_user_login partition (ds = '20250917')
select user_id,
       date_format(from_utc_timestamp(ts/1000, 'GMT+8'), 'yyyy-MM-dd')          date_id,
       date_format(from_utc_timestamp(ts/1000, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') login_time,
       channel,
       province_id,
       version_code,
       mid_id,
       brand,
       model,
       operate_system
from (
         select user_id,
                channel,
                province_id,
                version_code,
                mid_id,
                brand,
                model,
                operate_system,
                ts
         from (select get_json_object(log, '$.common.uid') user_id,
                      get_json_object(log, '$.common.ch')  channel,
                      get_json_object(log, '$.common.ar')  province_id,
                      get_json_object(log, '$.common.vc')  version_code,
                      get_json_object(log, '$.common.mid') mid_id,
                      get_json_object(log, '$.common.ba')  brand,
                      get_json_object(log, '$.common.md')  model,
                      get_json_object(log, '$.common.os')  operate_system,
                      get_json_object(log, '$.ts') ts,
                      row_number() over (partition by get_json_object(log, '$.common.sid') order by get_json_object(log, '$.ts')) rn
               from ods_z_log
               where ds = '20250917') t1
         where rn = 1
     ) t2;


insert overwrite table ads_user_stats
select * from ads_user_stats
union
select '20250918' ds,
       recent_days,
       sum(if(login_date_first >= date_add('20250918', -recent_days + 1), 1, 0)) new_user_count,
       count(*) active_user_count
from dws_user_user_login_td lateral view explode(array(1, 7, 30)) tmp as recent_days
where ds = '20250918'
  and login_date_last >= date_add('20250918', -recent_days + 1)
group by recent_days;

insert overwrite table ads_user_action
select * from ads_user_action
union
select
    '20250917' ds,
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
        where ds='20250917'
          and page_id in ('home','good_detail')
    )page
        join
    (
        select
            1 recent_days,
            count(*) cart_count
        from dws_trade_user_cart_add_1d
        where ds='20250917'
    )cart
    on page.recent_days=cart.recent_days
        join
    (
        select
            1 recent_days,
            count(*) order_count
        from dws_trade_user_order_1d
        where ds='2025-08-14'
    )ord
    on page.recent_days=ord.recent_days
        join
    (
        select
            1 recent_days,
            count(*) payment_count
        from dws_trade_user_payment_1d
        where ds='2025-08-14'
    )pay
    on page.recent_days=pay.recent_days;


insert overwrite table ads_user_retention
select * from ads_user_retention
union
select '20250918' ds,
       login_date_first create_date,
       datediff('2025-08-14', login_date_first) retention_day,
       sum(if(login_date_last = '20250918', 1, 0)) retention_count,
       count(*) new_user_count,
       cast(sum(if(login_date_last = '20250918', 1, 0)) / count(*) * 100 as decimal(16, 2)) retention_rate
from (
         select user_id,
                login_date_last,
                login_date_first
         from dws_user_user_login_td
         where ds = '20250918'
           and login_date_first >= date_add('2025-08-14', -7)
           and login_date_first < '2025-08-14'
     ) t1
group by login_date_first;

insert overwrite table ads_new_order_user_stats
select * from ads_new_order_user_stats
union
select
    '2025-08-14' ds,
    recent_days,
    count(*) new_order_user_count
from dws_trade_user_order_td lateral view explode(array(1,7,30)) tmp as recent_days
where ds='20250917'
  and order_date_first>=date_add('2025-08-14',-recent_days+1)
group by recent_days;

insert overwrite table ads_repeat_purchase_by_tm
select * from ads_repeat_purchase_by_tm
union
select
    '2025-08-14',
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
        where ds='2025-08-14'
        group by user_id, tm_id,tm_name
    )t1
group by tm_id,tm_name;


set hive.mapjoin.optimized.hashtable=false;
insert overwrite table ads_sku_cart_num_top3_by_cate
select * from ads_sku_cart_num_top3_by_cate
union
select
    '20250917' ds,
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
                where ds='20250917'
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
                from dim_sku
                where ds='20250917'
            )sku
            on cart.sku_id=sku.id
    )t1
where rk<=3;
-- 优化项不应一直禁用，受影响的SQL执行完毕后打开
set hive.mapjoin.optimized.hashtable=true;


DROP TABLE IF EXISTS dwd_trade_cart_full;
CREATE EXTERNAL TABLE dwd_trade_cart_full
(
    `id`         STRING COMMENT '编号',
    `user_id`   STRING COMMENT '用户ID',
    `sku_id`    STRING COMMENT 'SKU_ID',
    `sku_name`  STRING COMMENT '商品名称',
    `sku_num`   BIGINT COMMENT '现存商品件数'
) COMMENT '交易域购物车周期快照事实表'
    PARTITIONED BY (`ds` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_cart_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_cart_full partition(ds='20250917')
select
    id,
    user_id,
    sku_id,
    sku_name,
    sku_num
from ods_cart_info
where ds='20250917'
  and is_ordered='0';


insert overwrite table ads_traffic_stats_by_channel
select * from ads_traffic_stats_by_channel
union
select
    '20250917' ds,
    recent_days,
    channel,
    cast(count(distinct(mid_id)) as bigint) uv_count,
    cast(avg(during_time_1d)/1000 as bigint) avg_duration_sec,
    cast(avg(page_count_1d) as bigint) avg_page_count,
    cast(count(*) as bigint) sv_count,
    cast(sum(if(page_count_1d=1,1,0))/count(*) as decimal(16,2)) bounce_rate
from dws_traffic_session_page_view_1d lateral view explode(array(1,7,30)) tmp as recent_days
group by recent_days,channel;



insert overwrite table ads_user_retention
select * from ads_user_retention
union
select '20250918' ds,
       login_date_first create_date,
       datediff('2025-08-14', login_date_first) retention_day,
       sum(if(login_date_last = '20250918', 1, 0)) retention_count,
       count(*) new_user_count,
       cast(sum(if(login_date_last = '20250918', 1, 0)) / count(*) * 100 as decimal(16, 2)) retention_rate
from (
         select user_id,
                login_date_last,
                login_date_first
         from dws_user_user_login_td
         where ds = '20250918'
           and login_date_first >= date_add('2025-08-14', -7)
           and login_date_first < '2025-08-14'
     ) t1
group by login_date_first;