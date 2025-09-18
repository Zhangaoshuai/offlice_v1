use realtime_v1_hive;
--交易域加购事务事实表
DROP TABLE IF EXISTS dwd_trade_cart_add;
CREATE EXTERNAL TABLE dwd_trade_cart_add
(
    `id`                  STRING COMMENT '编号',
    `user_id`            STRING COMMENT '用户ID',
    `sku_id`             STRING COMMENT 'SKU_ID',
    `date_id`            STRING COMMENT '日期ID',
    `create_time`        STRING COMMENT '加购时间',
    `sku_num`            BIGINT COMMENT '加购物车件数'
) COMMENT '交易域加购事务事实表'
    PARTITIONED BY (`ds` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_cart_add/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_cart_add partition (ds)
select
    id,
    user_id,
    sku_id,
    date_format(create_time,'yyyy-MM-dd') date_id,
    create_time,
    sku_num,
    date_format(create_time, 'yyyy-MM-dd')
from ods_cart_info
where ds = '20250917'
;

--交易域下单事务事实表
DROP TABLE IF EXISTS dwd_trade_order_detail;
CREATE EXTERNAL TABLE dwd_trade_order_detail
(
    `id`                     STRING COMMENT '编号',
    `order_id`              STRING COMMENT '订单ID',
    `user_id`               STRING COMMENT '用户ID',
    `sku_id`                STRING COMMENT '商品ID',
    `province_id`          STRING COMMENT '省份ID',
    `activity_id`          STRING COMMENT '参与活动ID',
    `activity_rule_id`    STRING COMMENT '参与活动规则ID',
    `coupon_id`             STRING COMMENT '使用优惠券ID',
    `date_id`               STRING COMMENT '下单日期ID',
    `create_time`           STRING COMMENT '下单时间',
    `sku_num`                BIGINT COMMENT '商品数量',
    `split_original_amount` DECIMAL(16, 2) COMMENT '原始价格',
    `split_activity_amount` DECIMAL(16, 2) COMMENT '活动优惠分摊',
    `split_coupon_amount`   DECIMAL(16, 2) COMMENT '优惠券优惠分摊',
    `split_total_amount`    DECIMAL(16, 2) COMMENT '最终价格分摊'
) COMMENT '交易域下单事务事实表'
    PARTITIONED BY (`ds` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_order_detail/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_order_detail partition (ds)
select
    od.id,
    order_id,
    user_id,
    sku_id,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,
    date_format(create_time, 'yyyy-MM-dd') date_id,
    create_time,
    sku_num,
    split_original_amount,
    nvl(split_activity_amount,0.0),
    nvl(split_coupon_amount,0.0),
    split_total_amount,
    date_format(create_time,'yyyy-MM-dd')
from
    (
        select
            id,
            order_id,
            sku_id,
            create_time,
            sku_num,
            sku_num * order_price split_original_amount,
            split_total_amount,
            split_activity_amount,
            split_coupon_amount
        from ods_order_detail
        where ds = '20250917'

    ) od
        left join
    (
        select
            id,
            user_id,
            province_id
        from ods_order_info
        where ds = '20250917'

    ) oi
    on od.order_id = oi.id
        left join
    (
        select
            order_detail_id,
            activity_id,
            activity_rule_id
        from ods_order_detail_activity
        where ds = '20250917'

    ) act
    on od.id = act.order_detail_id
        left join
    (
        select
            order_detail_id,
            coupon_id
        from ods_order_detail_coupon
        where ds = '20250917'

    ) cou
    on od.id = cou.order_detail_id;

insert overwrite table dwd_trade_order_detail partition (ds='20250918')
select
    od.id,
    order_id,
    user_id,
    sku_id,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,
    date_id,
    create_time,
    sku_num,
    split_original_amount,
    nvl(split_activity_amount,0.0),
    nvl(split_coupon_amount,0.0),
    split_total_amount
from
    (
        select
            id,
            order_id,
            sku_id,
            date_format(create_time, 'yyyy-MM-dd') date_id,
            create_time,
            sku_num,
            sku_num * order_price split_original_amount,
            split_total_amount,
            split_activity_amount,
            split_coupon_amount
        from ods_order_detail
        where ds = '20250918'

    ) od
        left join
    (
        select
            id,
            user_id,
            province_id
        from ods_order_info
        where ds = '20250918'

    ) oi
    on od.order_id = oi.id
        left join
    (
        select
            order_detail_id,
            activity_id,
            activity_rule_id
        from ods_order_detail_activity
        where ds = '20250918'

    ) act
    on od.id = act.order_detail_id
        left join
    (
        select
            order_detail_id,
            coupon_id
        from ods_order_detail_coupon
        where ds = '20250918'

    ) cou
    on od.id = cou.order_detail_id;

--交易域支付成功事务事实表
DROP TABLE IF EXISTS dwd_trade_pay_detail_suc;
CREATE EXTERNAL TABLE dwd_trade_pay_detail_suc
(
    `id`                      STRING COMMENT '编号',
    `order_id`               STRING COMMENT '订单ID',
    `user_id`                STRING COMMENT '用户ID',
    `sku_id`                 STRING COMMENT 'SKU_ID',
    `province_id`           STRING COMMENT '省份ID',
    `activity_id`           STRING COMMENT '参与活动ID',
    `activity_rule_id`     STRING COMMENT '参与活动规则ID',
    `coupon_id`              STRING COMMENT '使用优惠券ID',
    `payment_type_code`     STRING COMMENT '支付类型编码',
    `payment_type_name`     STRING COMMENT '支付类型名称',
    `date_id`                STRING COMMENT '支付日期ID',
    `callback_time`         STRING COMMENT '支付成功时间',
    `sku_num`                 BIGINT COMMENT '商品数量',
    `split_original_amount` DECIMAL(16, 2) COMMENT '应支付原始金额',
    `split_activity_amount` DECIMAL(16, 2) COMMENT '支付活动优惠分摊',
    `split_coupon_amount`   DECIMAL(16, 2) COMMENT '支付优惠券优惠分摊',
    `split_payment_amount`  DECIMAL(16, 2) COMMENT '支付金额'
) COMMENT '交易域支付成功事务事实表'
    PARTITIONED BY (`ds` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_pay_detail_suc/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_pay_detail_suc partition (ds)
select
    od.id,
    od.order_id,
    user_id,
    sku_id,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,
    payment_type,
    pay_dic.dic_name,
    date_format(callback_time,'yyyy-MM-dd') date_id,
    callback_time,
    sku_num,
    split_original_amount,
    nvl(split_activity_amount,0.0),
    nvl(split_coupon_amount,0.0),
    split_total_amount,
    date_format(callback_time,'yyyy-MM-dd')
from
    (
        select
            id,
            order_id,
            sku_id,
            sku_num,
            sku_num * order_price split_original_amount,
            split_total_amount,
            split_activity_amount,
            split_coupon_amount
        from ods_order_detail
        where ds = '20250917'

    ) od
        join
    (
        select
            user_id,
            order_id,
            payment_type,
            callback_time
        from ods_payment_info
        where ds='20250917'

          and payment_status='1602'
    ) pi
    on od.order_id=pi.order_id
        left join
    (
        select
            id,
            province_id
        from ods_order_info
        where ds = '20250917'

    ) oi
    on od.order_id = oi.id
        left join
    (
        select
            order_detail_id,
            activity_id,
            activity_rule_id
        from ods_order_detail_activity
        where ds = '20250917'

    ) act
    on od.id = act.order_detail_id
        left join
    (
        select
            order_detail_id,
            coupon_id
        from ods_order_detail_coupon
        where ds = '20250917'

    ) cou
    on od.id = cou.order_detail_id
        left join
    (
        select
            dic_code,
            dic_name
        from ods_base_dic
        where ds='20250917'
          and parent_code='11'
    ) pay_dic
    on pi.payment_type=pay_dic.dic_code;

insert overwrite table dwd_trade_pay_detail_suc partition (ds='20250918')
select
    od.id,
    od.order_id,
    user_id,
    sku_id,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,
    payment_type,
    pay_dic.dic_name,
    date_format(callback_time,'yyyy-MM-dd') date_id,
    callback_time,
    sku_num,
    split_original_amount,
    nvl(split_activity_amount,0.0),
    nvl(split_coupon_amount,0.0),
    split_total_amount
from
    (
        select
            id,
            order_id,
            sku_id,
            sku_num,
            sku_num * order_price split_original_amount,
            split_total_amount,
            split_activity_amount,
            split_coupon_amount
        from ods_order_detail
        where (ds = '20250918' or ds = date_add('20250918',-1))

    ) od
        join
    (
        select
            user_id,
            order_id,
            payment_type,
            callback_time
        from ods_payment_info
        where ds='20250918'
          and payment_status='1602'
    ) pi
    on od.order_id=pi.order_id
        left join
    (
        select
            id,
            province_id
        from ods_order_info
        where (ds = '20250918' or ds = date_add('20250918',-1))

    ) oi
    on od.order_id = oi.id
        left join
    (
        select
            order_detail_id,
            activity_id,
            activity_rule_id
        from ods_order_detail_activity
        where (ds = '20250918' or ds = date_add('20250918',-1))

    ) act
    on od.id = act.order_detail_id
        left join
    (
        select
            order_detail_id,
            coupon_id
        from ods_order_detail_coupon
        where (ds = '20250918' or ds = date_add('20250918',-1))

    ) cou
    on od.id = cou.order_detail_id
        left join
    (
        select
            dic_code,
            dic_name
        from ods_base_dic
        where ds='20250918'
          and parent_code='11'
    ) pay_dic
    on pi.payment_type=pay_dic.dic_code;

--交易域购物车周期快照事实表
DROP TABLE IF EXISTS dwd_trade_cart;
CREATE EXTERNAL TABLE dwd_trade_cart
(
    `id`         STRING COMMENT '编号',
    `user_id`   STRING COMMENT '用户ID',
    `sku_id`    STRING COMMENT 'SKU_ID',
    `sku_name`  STRING COMMENT '商品名称',
    `sku_num`   BIGINT COMMENT '现存商品件数'
) COMMENT '交易域购物车周期快照事实表'
    PARTITIONED BY (`ds` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_cart/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

insert overwrite table dwd_trade_cart partition(ds='20250917')
select
    id,
    user_id,
    sku_id,
    sku_name,
    sku_num
from ods_cart_info
where ds='20250917'
  and is_ordered='0';

--交易域交易流程累积快照事实表
DROP TABLE IF EXISTS dwd_trade_trade_flow_acc;
CREATE EXTERNAL TABLE dwd_trade_trade_flow_acc
(
    `order_id`               STRING COMMENT '订单ID',
    `user_id`                STRING COMMENT '用户ID',
    `province_id`           STRING COMMENT '省份ID',
    `order_date_id`         STRING COMMENT '下单日期ID',
    `order_time`             STRING COMMENT '下单时间',
    `payment_date_id`        STRING COMMENT '支付日期ID',
    `payment_time`           STRING COMMENT '支付时间',
    `finish_date_id`         STRING COMMENT '确认收货日期ID',
    `finish_time`             STRING COMMENT '确认收货时间',
    `order_original_amount` DECIMAL(16, 2) COMMENT '下单原始价格',
    `order_activity_amount` DECIMAL(16, 2) COMMENT '下单活动优惠分摊',
    `order_coupon_amount`   DECIMAL(16, 2) COMMENT '下单优惠券优惠分摊',
    `order_total_amount`    DECIMAL(16, 2) COMMENT '下单最终价格分摊',
    `payment_amount`         DECIMAL(16, 2) COMMENT '支付金额'
) COMMENT '交易域交易流程累积快照事实表'
    PARTITIONED BY (`ds` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_trade_flow_acc/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_trade_flow_acc partition(ds)
select
    oi.id,
    user_id,
    province_id,
    date_format(create_time,'yyyy-MM-dd'),
    create_time,
    date_format(callback_time,'yyyy-MM-dd'),
    callback_time,
    date_format(finish_time,'yyyy-MM-dd'),
    finish_time,
    original_total_amount,
    activity_reduce_amount,
    coupon_reduce_amount,
    total_amount,
    nvl(payment_amount,0.0),
    nvl(date_format(finish_time,'yyyy-MM-dd'),'99991231')
from
    (
        select
            id,
            user_id,
            province_id,
            create_time,
            original_total_amount,
            activity_reduce_amount,
            coupon_reduce_amount,
            total_amount
        from ods_order_info
        where ds='20250917'

    )oi
        left join
    (
        select
            order_id,
            callback_time,
            total_amount payment_amount
        from ods_payment_info
        where ds='20250917'

          and payment_status='1602'
    )pi
    on oi.id=pi.order_id
        left join
    (
        select
            order_id,
            create_time finish_time
        from ods_order_status_log
        where ds='20250917'

          and order_status='1004'
    )log
    on oi.id=log.order_id;

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_trade_flow_acc partition(ds)
select
    oi.order_id,
    user_id,
    province_id,
    order_date_id,
    order_time,
    nvl(oi.payment_date_id,pi.payment_date_id),
    nvl(oi.payment_time,pi.payment_time),
    nvl(oi.finish_date_id,log.finish_date_id),
    nvl(oi.finish_time,log.finish_time),
    order_original_amount,
    order_activity_amount,
    order_coupon_amount,
    order_total_amount,
    nvl(oi.payment_amount,pi.payment_amount),
    nvl(nvl(oi.finish_time,log.finish_time),'99991231')
from
    (
        select
            order_id,
            user_id,
            province_id,
            order_date_id,
            order_time,
            payment_date_id,
            payment_time,
            finish_date_id,
            finish_time,
            order_original_amount,
            order_activity_amount,
            order_coupon_amount,
            order_total_amount,
            payment_amount
        from dwd_trade_trade_flow_acc
        where ds='99991231'
        union all
        select
            id,
            user_id,
            province_id,
            date_format(create_time,'yyyy-MM-dd') order_date_id,
            create_time,
            null payment_date_id,
            null payment_time,
            null finish_date_id,
            null finish_time,
            original_total_amount,
            activity_reduce_amount,
            coupon_reduce_amount,
            total_amount,
            null payment_amount
        from ods_order_info
        where ds='20250918'

    )oi
        left join
    (
        select
            order_id,
            date_format(callback_time,'yyyy-MM-dd') payment_date_id,
            callback_time payment_time,
            total_amount payment_amount
        from ods_payment_info
        where ds='20250918'
          and payment_status='1602'
    )pi
    on oi.order_id=pi.order_id
        left join
    (
        select
            order_id,
            date_format(create_time,'yyyy-MM-dd') finish_date_id,
            create_time finish_time
        from ods_order_status_log
        where ds='20250918'

          and order_status='1004'
    )log
    on oi.order_id=log.order_id;

--优惠券使用（支付）事务事实表
DROP TABLE IF EXISTS dwd_tool_coupon_used;
CREATE EXTERNAL TABLE dwd_tool_coupon_used
(
    `id`           STRING COMMENT '编号',
    `coupon_id`    STRING COMMENT '优惠券ID',
    `user_id`      STRING COMMENT '用户ID',
    `order_id`     STRING COMMENT '订单ID',
    `date_id`      STRING COMMENT '日期ID',
    `payment_time` STRING COMMENT '使用(支付)时间'
) COMMENT '优惠券使用（支付）事务事实表'
    PARTITIONED BY (`ds` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_tool_coupon_used/'
    TBLPROPERTIES ("orc.compress" = "snappy");

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_tool_coupon_used partition(ds)
select
    id,
    coupon_id,
    user_id,
    order_id,
    date_format(used_time,'yyyy-MM-dd') date_id,
    used_time,
    date_format(used_time,'yyyy-MM-dd')
from ods_coupon_use
where ds='20250917'

  and used_time is not null;

insert overwrite table dwd_tool_coupon_used partition(ds='20250918')
select
    id,
    coupon_id,
    user_id,
    order_id,
    date_format(used_time,'yyyy-MM-dd') date_id,
    used_time
from ods_coupon_use
where ds='20250918';

--互动域收藏商品事务事实表
DROP TABLE IF EXISTS dwd_interaction_favor_add;
CREATE EXTERNAL TABLE dwd_interaction_favor_add
(
    `id`          STRING COMMENT '编号',
    `user_id`     STRING COMMENT '用户ID',
    `sku_id`      STRING COMMENT 'SKU_ID',
    `date_id`     STRING COMMENT '日期ID',
    `create_time` STRING COMMENT '收藏时间'
) COMMENT '互动域收藏商品事务事实表'
    PARTITIONED BY (`ds` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_interaction_favor_add/'
    TBLPROPERTIES ("orc.compress" = "snappy");

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_interaction_favor_add partition(ds)
select
    id,
    user_id,
    sku_id,
    date_format(create_time,'yyyy-MM-dd') date_id,
    create_time,
    date_format(create_time,'yyyy-MM-dd')
from ods_favor_info
where ds='20250917'
;

insert overwrite table dwd_interaction_favor_add partition(ds='20250918')
select
    id,
    user_id,
    sku_id,
    date_format(create_time,'yyyy-MM-dd') date_id,
    create_time
from ods_favor_info
where ds='20250918'
;