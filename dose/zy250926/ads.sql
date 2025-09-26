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
    LOCATION '/warehouse/gmall/tmp/tmp_dim_date_info';