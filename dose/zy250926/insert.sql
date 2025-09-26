with
    sku as
        (
            select
                id,
                price,
                sku_name,
                sku_desc,
                weight,
                is_sale,
                spu_id,
                category3_id,
                tm_id,
                create_time
            from ods_sku_info
            where ds='20250917'
        ),
    spu as
        (
            select
                id,
                spu_name
            from ods_spu_info
            where ds='20250917'
        ),
    c3 as
        (
            select
                id,
                name,
                category2_id
            from ods_base_category3
            where ds='20250917'
        ),
    c2 as
        (
            select
                id,
                name,
                category1_id
            from ods_base_category2
            where ds='20250917'
        ),
    c1 as
        (
            select
                id,
                name
            from ods_base_category1
            where ds='20250917'
        ),
    tm as
        (
            select
                id,
                tm_name
            from ods_base_trademark
            where ds='20250917'
        ),
    attr as
        (
            select
                sku_id,
                collect_set(named_struct('attr_id',attr_id,'value_id',value_id,'attr_name',attr_name,'value_name',value_name)) attrs
            from ods_sku_attr_value
            where ds='20250917'
            group by sku_id
        ),
    sale_attr as
        (
            select
                sku_id,
                collect_set(named_struct('sale_attr_id',sale_attr_id,'sale_attr_value_id',sale_attr_value_id,'sale_attr_name',sale_attr_name,'sale_attr_value_name',sale_attr_value_name)) sale_attrs
            from ods_sku_sale_attr_value
            where ds='20250917'
            group by sku_id
        )
insert overwrite table dim_sku partition(ds='20250917')
select
    sku.id,
    sku.price,
    sku.sku_name,
    sku.sku_desc,
    sku.weight,
    sku.is_sale,
    sku.spu_id,
    spu.spu_name,
    sku.category3_id,
    c3.name,
    c3.category2_id,
    c2.name,
    c2.category1_id,
    c1.name,
    sku.tm_id,
    tm.tm_name,
    attr.attrs,
    sale_attr.sale_attrs,
    sku.create_time
from sku
         left join spu on sku.spu_id=spu.id
         left join c3 on sku.category3_id=c3.id
         left join c2 on c3.category2_id=c2.id
         left join c1 on c2.category1_id=c1.id
         left join tm on sku.tm_id=tm.id
         left join attr on sku.id=attr.sku_id
         left join sale_attr on sku.id=sale_attr.sku_id;

insert overwrite table dim_coupon partition(ds='20250917')
select
    id,
    coupon_name,
    coupon_type,
    coupon_dic.dic_name,
    condition_amount,
    condition_num,
    activity_id,
    benefit_amount,
    benefit_discount,
    case coupon_type
        when '3201' then concat('满',condition_amount,'元减',benefit_amount,'元')
        when '3202' then concat('满',condition_num,'件打', benefit_discount,' 折')
        when '3203' then concat('减',benefit_amount,'元')
        end benefit_rule,
    create_time,
    range_type,
    range_dic.dic_name,
    limit_num,
    taken_count,
    start_time,
    end_time,
    operate_time,
    expire_time
from
    (
        select
            id,
            coupon_name,
            coupon_type,
            condition_amount,
            condition_num,
            activity_id,
            benefit_amount,
            benefit_discount,
            create_time,
            range_type,
            limit_num,
            taken_count,
            start_time,
            end_time,
            operate_time,
            expire_time
        from ods_coupon_info
        where ds='20250917'
    )ci
        left join
    (
        select
            dic_code,
            dic_name
        from ods_base_dic
        where ds='20250917'
          and parent_code='32'
    )coupon_dic
    on ci.coupon_type=coupon_dic.dic_code
        left join
    (
        select
            dic_code,
            dic_name
        from ods_base_dic
        where ds='20250917'
          and parent_code='33'
    )range_dic
    on ci.range_type=range_dic.dic_code;

insert overwrite table dim_activity partition(ds='20250917')
select
    rule.id,
    info.id,
    activity_name,
    rule.activity_type,
    dic.dic_name,
    activity_desc,
    start_time,
    end_time,
    create_time,
    condition_amount,
    condition_num,
    benefit_amount,
    benefit_discount,
    case rule.activity_type
        when '3101' then concat('满',condition_amount,'元减',benefit_amount,'元')
        when '3102' then concat('满',condition_num,'件打', benefit_discount,' 折')
        when '3103' then concat('打', benefit_discount,'折')
        end benefit_rule,
    benefit_level
from
    (
        select
            id,
            activity_id,
            activity_type,
            condition_amount,
            condition_num,
            benefit_amount,
            benefit_discount,
            benefit_level
        from ods_activity_rule
        where ds='20250917'
    )rule
        left join
    (
        select
            id,
            activity_name,
            activity_type,
            activity_desc,
            start_time,
            end_time,
            create_time
        from ods_activity_info
        where ds='20250917'
    )info
    on rule.activity_id=info.id
        left join
    (
        select
            dic_code,
            dic_name
        from ods_base_dic
        where ds='20250917'
          and parent_code='31'
    )dic
    on rule.activity_type=dic.dic_code;

insert overwrite table dim_province partition(ds='20250917')
select
    province.id,
    province.name,
    province.area_code,
    province.iso_code,
    province.iso_3166_2,
    region_id,
    region_name
from
    (
        select
            id,
            name,
            region_id,
            area_code,
            iso_code,
            iso_3166_2
        from ods_base_province
        where ds='20250917'
    )province
        left join
    (
        select
            id,
            region_name
        from ods_base_region
        where ds='20250917'
    )region
    on province.region_id=region.id;

insert overwrite table dim_promotion_pos partition(ds='20250917')
select
    `id`,
    `pos_location`,
    `pos_type`,
    `promotion_type`,
    `create_time`,
    `operate_time`
from ods_promotion_pos
where ds='20250917';

insert overwrite table dim_promotion_refer partition(ds='20250917')
select
    `id`,
    `refer_name`,
    `create_time`,
    `operate_time`
from ods_promotion_refer
where ds='20250917';

-- insert overwrite table dim_date select * from tmp_dim_date_info;
-- select * from dim_date;

insert overwrite table dim_user_zip partition (ds = '9999-12-31')
select id,
       concat(substr(name, 1, 1), '*')                name,
       if(phone_num regexp '^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$',
          concat(substr(phone_num, 1, 3), '*'), null) phone_num,
       if(email regexp '^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\\.[a-zA-Z0-9_-]+)+$',
          concat('*@', split(email, '@')[1]), null)   email,
       user_level,
       birthday,
       gender,
       create_time,
       operate_time,
       '20250917'                                        start_date,
       '9999-12-31'                                        end_date
from ods_user_info
where ds='20250917';