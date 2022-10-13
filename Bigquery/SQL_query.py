##### 해당 쿼리들은 GCP 빅쿼리에 DW를 구축하여 데이터를 추출했음을 알려드립니다.####

# 1. (신규등록)시계열_category별 등록현황
"""
WITH
temp_01 as (
SELECT 
    CAST(SUBSTR(CAST(first_reg_date as string),1,7) || '-' || '01' as date) as year_month
  , vehicle_type
  , vehicle_model
  , vehicle_cat
FROM `itruck-codes.itruck_db_dataset.new_reg`
WHERE vehicle_type = '화물'
)
select
    DISTINCT temp_01.year_month
  , vehicle_model
  , vehicle_cat
  , COUNT(vehicle_cat) OVER (PARTITION BY temp_01.year_month, vehicle_model, vehicle_cat) as cat_cnt
from 
  temp_01
order by 1, 4 desc;
"""

# 2.  (구조변경)구조변경유형_평균 최대적재_평균 배기량
"""
SELECT 
    DISTINCT date(sale_reg_date) as reg_dt,
    struc_change,
    COUNT(struc_change) OVER (PARTITION BY date(sale_reg_date), struc_change) as struc_cnt,
    ROUND(AVG(max_load) OVER (PARTITION BY date(sale_reg_date), struc_change),2) as avg_max_load,
    ROUND(AVG(displacement) OVER (PARTITION BY date(sale_reg_date), struc_change),2) as avg_displacement
FROM `itruck-codes.itruck_db_dataset.change`
WHERE 
    vehicle_cat = '소형'
AND struc_change IN ('물품적재장치', '배기가스발산방지장치')
ORDER BY 1, 3 DESC;
"""

# 3. (구조변경)시계열에 따른 변속기 선호도 추이
"""
SELECT 
    DISTINCT date(sale_reg_date) as reg_dt,
    vehicle_cat,
    transmission,
    COUNT(transmission) OVER (partition by date(sale_reg_date), vehicle_cat, transmission) as tran_cnt
FROM `itruck-codes.itruck_db_dataset.change`
WHERE vehicle_cat not in ('경형')
ORDER BY 1, 3 DESC;
"""

# 4. (구조변경) 차량종류에 따른 변속기 선호도
"""
SELECT 
    DISTINCT vehicle_cat,
    transmission,
    COUNT(transmission) OVER (partition by vehicle_cat, transmission) as tran_cnt,
    ROUND(COUNT(transmission) OVER (partition by vehicle_cat, transmission) / COUNT(transmission) OVER (partition by vehicle_cat), 2) as tran_ratio
FROM `itruck-codes.itruck_db_dataset.change`
WHERE vehicle_cat not in ('경형')
ORDER BY 1, 3 DESC;
"""

# 5. (월별)영업용 화물차등록 현황 -> 컬럼별 데이터 누적합계를 월별 증감데이터로 구조변경
"""
SELECT 
      date(reg_date) as reg_date
    , extract(year from date(reg_date)) as year
    , extract(month from date(reg_date)) as month
    , coalesce(p_c_1500cc_under - lag(p_c_1500cc_under,1) over (order by date(reg_date)),0)  as diff_p_c_1500cc_under
    , coalesce(p_c_2000cc_under - lag(p_c_2000cc_under,1) over (order by date(reg_date)),0)  as diff_p_c_2000cc_under
    , coalesce(p_c_2500cc_under - lag(p_c_2500cc_under,1) over (order by date(reg_date)),0)  as diff_p_c_2500cc_under
    , coalesce(p_c_3000cc_under - lag(p_c_3000cc_under,1) over (order by date(reg_date)),0)  as diff_p_c_3000cc_under
    , coalesce(p_c_3500cc_under - lag(p_c_3500cc_under,1) over (order by date(reg_date)),0)  as diff_p_c_3500cc_under
    , coalesce(p_c_3500cc_more - lag(p_c_3500cc_more,1) over (order by date(reg_date)),0)  as diff_p_c_3500cc_under
    , coalesce(ambul - lag(ambul, 1) over(order by date(reg_date)),0) as diff_ambul
    , coalesce(funeral - lag(funeral, 1) over (order by date(reg_date)),0) as diff_funeral
    , coalesce(van_etc - lag(van_etc, 1) over (order by date(reg_date)),0) as diff_van_etc
    , coalesce(cargo_pickup - lag(cargo_pickup, 1) over (order by date(reg_date)),0) as diff_cargo_pickup
    , coalesce(cargo_1ton_under - lag(cargo_1ton_under, 1) over (order by date(reg_date)),0) as diff_cargo_1ton_under
    , coalesce(cargo_3ton_under - lag(cargo_3ton_under, 1) over (order by date(reg_date)),0) as diff_cargo_3ton_under
    , coalesce(cargo_5ton_under - lag(cargo_5ton_under, 1) over (order by date(reg_date)),0) as diff_cargo_5ton_under
    , coalesce(cargo_8ton_under - lag(cargo_8ton_under, 1) over (order by date(reg_date)),0) as diff_cargo_8ton_under
    , coalesce(cargo_10ton_under - lag(cargo_10ton_under,1) over (order by date(reg_date)),0) as diff_cargo_10ton_under
    , coalesce(cargo_12ton_under - lag(cargo_12ton_under, 1) over (order by date(reg_date)),0) as diff_cargo_12ton_under
    , coalesce(cargo_12ton_more - lag(cargo_12ton_more, 1) over (order by date(reg_date)),0) as diff_cargo_12ton_more
    , coalesce(dump_1ton_under - lag(dump_1ton_under, 1) over (order by date(reg_date)),0) as diff_dump_1ton_under
    , coalesce(dump_5ton_under - lag(dump_5ton_under, 1) over (order by date(reg_date)),0) as diff_dump_5ton_under
    , coalesce(dump_12ton_under - lag(dump_12ton_under, 1) over (order by date(reg_date)),0) as diff_dump_12ton_under
    , coalesce(dump_12ton_more - lag(dump_12ton_more, 1) over (order by date(reg_date)),0) as diff_dump_12ton_more
    , coalesce(c_van_1ton_under - lag(c_van_1ton_under, 1) over (order by date(reg_date)),0) as diff_c_van_1ton_under
    , coalesce(c_van_5ton_under - lag(c_van_5ton_under, 1) over (order by date(reg_date)),0) as diff_c_van_5ton_under
    , coalesce(c_van_5ton_more - lag(c_van_5ton_more, 1) over (order by date(reg_date)),0) as diff_c_van_5ton_more
    , coalesce(c_spacial_sprinkler - lag(c_spacial_sprinkler, 1) over (order by date(reg_date)),0) as diff_c_spacial_sprinkler
    , coalesce(c_spacial_f_truck - lag(c_spacial_f_truck, 1) over (order by date(reg_date)),0) as diff_c_spacial_f_truck
    , coalesce(tanker_jet - lag(tanker_jet, 1) over (order by date(reg_date)),0) as diff_tanker_jet
    , coalesce(tanker_gasol - lag(tanker_gasol, 1) over (order by date(reg_date)),0) as diff_tanker_gasol
    , coalesce(tanker_diesel - lag(tanker_diesel, 1) over (order by date(reg_date)),0) as diff_tanker_diesel
    , coalesce(tanker_banca - lag(tanker_banca, 1) over (order by date(reg_date)),0) as diff_tanker_banca
    , coalesce(tanker_etc - lag(tanker_etc, 1) over (order by date(reg_date)),0) as diff_tanker_etc
    , coalesce(t_bever - lag(t_bever, 1) over (order by date(reg_date)),0) as diff_t_bever
    , coalesce(t_com_gas - lag(t_com_gas, 1) over (order by date(reg_date)),0) as diff_t_com_gas
    , coalesce(t_chemical - lag(t_chemical, 1) over (order by date(reg_date)),0) as diff_t_chemical
    , coalesce(t_etc - lag(t_etc, 1) over (order by date(reg_date)),0) as diff_t_etc
    , coalesce(towed_load - lag(towed_load, 1) over (order by date(reg_date)),0) as diff_towed_load
    , coalesce(towed_low - lag(towed_low, 1) over (order by date(reg_date)),0) as diff_towed_low
    , coalesce(towed_flat - lag(towed_flat, 1) over (order by date(reg_date)),0) as diff_towed_flat
    , coalesce(towed_contain - lag(towed_contain, 1) over (order by date(reg_date)),0) as diff_towed_contain
    , coalesce(towed_etc - lag(towed_etc, 1) over (order by date(reg_date)),0) as diff_towed_etc
    , coalesce(c_spacial_etc - lag(c_spacial_etc, 1) over (order by date(reg_date)),0) as diff_c_spacial_etc
    , coalesce(rescue_5ton_under - lag(rescue_5ton_under, 1) over (order by date(reg_date)),0) as diff_rescue_5ton_under
    , coalesce(rescue_10ton_under - lag(rescue_10ton_under, 1) over (order by date(reg_date)),0) as diff_rescue_10ton_under
    , coalesce(rescue_10ton_more - lag(rescue_10ton_more, 1) over (order by date(reg_date)),0) as diff_rescue_10ton_more
    , coalesce(tow_10ton_under - lag(tow_10ton_under, 1) over (order by date(reg_date)),0) as diff_tow_10ton_under
    , coalesce(tow_10ton_more - lag(tow_10ton_more, 1) over (order by date(reg_date)),0) as diff_tow_10ton_more
    , coalesce(tow_5ton_under - lag(tow_5ton_under, 1) over (order by date(reg_date)),0) as diff_tow_5ton_under
    , coalesce(p_c_low_spe_elec - lag(p_c_low_spe_elec, 1) over (order by date(reg_date)),0) as diff_p_c_low_spe_elec
    , coalesce(p_c_elec - lag(p_c_elec, 1) over (order by date(reg_date)),0) as diff_p_c_elec
    , coalesce(caravan - lag(caravan, 1) over (order by date(reg_date)),0) as diff_caravan
    , coalesce(c_spacial_refri_frozen - lag(c_spacial_refri_frozen, 1) over (order by date(reg_date)),0) as diff_c_spacial_refri_frozen
    , coalesce(c_spacial_feed - lag(c_spacial_feed, 1) over (order by date(reg_date)),0) as diff_c_spacial_feed
    , coalesce(spacial_towed - lag(spacial_towed, 1) over (order by date(reg_date)),0) as diff_spacial_towed
    , coalesce(spacial_high_p - lag(spacial_high_p, 1) over (order by date(reg_date)),0) as diff_spacial_high_p
    , coalesce(spacial_ladder_f_truck - lag(spacial_ladder_f_truck, 1) over (order by date(reg_date)),0) as diff_spacial_ladder_f_truck
    , coalesce(spacial_oga_crane - lag(spacial_oga_crane, 1) over (order by date(reg_date)),0) as diff_spacial_oga_crane
    , coalesce(spacial_etc - lag(spacial_etc, 1) over (order by date(reg_date)),0) as diff_spacial_etc
    , coalesce(c_spacial_road_sweeper - lag(c_spacial_road_sweeper, 1) over (order by date(reg_date)),0) as diff_c_spacial_road_sweeper
    , coalesce(c_spacial_sweeper - lag(c_spacial_sweeper, 1) over (order by date(reg_date)),0) as diff_c_spacial_sweeper
FROM 
  `itruck-codes.itruck_db_dataset.cargo_reg` 
order by 1;
"""

# 6. 톤수를 4분위 수로 분류기준 선정하여 분석쿼리 작성
"""
with
ton_grade as(
select ton
    ,ntile(4) over (order  by ton) as ntile_grade
from `itruck-codes.itruck_db_dataset.itruck_dataset` 
group by ton
order by 1
),
all_grade as(
select 
    *
    , (case when ton <= (select distinct(max(ton) over (partition by ntile_grade order by ntile_grade)) from ton_grade where ntile_grade = 1) then 'first'
           when ton <= (select distinct(max(ton) over (partition by ntile_grade order by ntile_grade)) from ton_grade where ntile_grade = 2) then 'second'
           when ton <= (select distinct(max(ton) over (partition by ntile_grade order by ntile_grade)) from ton_grade where ntile_grade = 3) then 'third'
           else 'forth'
           end) as grade
from `itruck-codes.itruck_db_dataset.itruck_dataset`
)
select 
    transac_date,
    reg_date,
    category,
    sub_category,
    brand,
    model,
    ton,
    con_sale,
    grade
from all_grade
where grade in ('first', 'second');
"""

# 7. 대/중/소형별 톤수 기준으로 인기차종 TOP3 추출
"""
with
ton_grade_table as(
  SELECT *
      , (case when ton <= 4.5 then '소형'
             when ton <= 16.5 then '중형'
        else '대형'
        end) as ton_grade
  FROM 
    `itruck-codes.itruck_db_dataset.itruck_dataset`
),
popular_cate as(
select 
        date(reg_date) as registration_date
      , ton_grade
      , category
      , brand
      , model
      , con_sale
      , count(model) as con_sale_count
from ton_grade_table
where con_sale in ('거래중', '거래완료')
group by registration_date, ton_grade, category, brand, model, con_sale
)
select *
from(
  select *
      , row_number() over (partition by registration_date order by con_sale_count desc) as ranking
  from popular_cate
)
where ranking <= 3
order by 1, 2;
"""


# 8. 기간별 INTERVAL이 짧은 순으로 인기순위 TOP3 카테고리 추출
"""
with
tran_interval_table as(
select 
    date(transac_date) as transac_date
  , date(reg_date) as reg_date
  , extract(day from date(transac_date) - date(reg_date)) as tran_interval
  , category
  , sub_category
  , brand
  , model
from `itruck_db_dataset.itruck_dataset`
where date(transac_date) is not null
),
ran as (
select 
     distinct category
     ,tran_interval
     ,count(category) over (partition by tran_interval, category) as cate_cnt
from tran_interval_table)
select category
      , tran_interval
      , cate_cnt
from
    (select *
      , row_number() over (partition by tran_interval order by cate_cnt desc) as ran_cnt      
    from ran)
where ran_cnt <=3
order by 2;
"""

# 9. 신규등록_vehicle_name기준으로 분류
"""
with
ve_name as(
SELECT 
    vehicle_name
    , CASE WHEN (substr(trim(vehicle_name), 1, instr(trim(vehicle_name), regexp_extract(trim(vehicle_name), r'[0-9]'),1)-1)) is null THEN trim(replace(vehicle_name, ' ', ''))
      ELSE (substr(trim(vehicle_name), 1, CASE WHEN (instr(trim(vehicle_name), regexp_extract(trim(vehicle_name), r'[0-9]'),1)) = 0 then null else instr(trim(vehicle_name), regexp_extract(trim(vehicle_name), r'[0-9]'),1) end-1))
      END as forward_string
    , substr(trim(vehicle_name),instr(trim(vehicle_name), regexp_extract(trim(vehicle_name), r'[0-9]'),1),instr(vehicle_name, regexp_extract(vehicle_name, r'[0-9가-힣]$',1))) as backword_string
FROM `itruck-codes.itruck_db_dataset.new_reg`
)
select
    vehicle_name
    , forward_string
    , backword_string
    , substr(backword_string, 1, CASE WHEN (instr(backword_string, regexp_extract(vehicle_name, r'[톤]', 1))) = 0 then null else instr(backword_string, regexp_extract(vehicle_name, r'[톤]', 1)) end -1) as ton
    ,trim(replace(substr(backword_string, 
        CASE WHEN instr(backword_string, regexp_extract(backword_string, r'^[0-9]X[0-9]')) is not null then instr(backword_string, regexp_extract(backword_string, r'^[0-9]X[0-9]'))+3
             WHEN instr(backword_string, regexp_extract(backword_string, r'[톤]', 1)) is not null then instr(backword_string, regexp_extract(backword_string, r'[톤]', 1))+1
             WHEN instr(backword_string, regexp_extract(backword_string, r'[㎥]', 1)) is not null then instr(backword_string, regexp_extract(backword_string, r'[㎥]', 1))+1
             WHEN instr(backword_string, regexp_extract(backword_string, r'[a-zA-Z]{2}', 1)) is not null then instr(backword_string, regexp_extract(backword_string, r'[a-zA-Z]{2}', 1))+2
             WHEN instr(backword_string, regexp_extract(backword_string, r'[가-힣]'),1) is not null then instr(backword_string, regexp_extract(backword_string, r'[가-힣]', 1))
        END
    , instr(backword_string, regexp_extract(backword_string, r'[가-힣]$',1))),' ', '')) as sub_category
from ve_name;
"""

# 10. 이전등록_vehicle_name기준으로 분류
"""
with
ve_name as(
SELECT
    date(first_reg_date) as first_reg_date
    , date(sale_reg_date) as sale_reg_date
    , vehicle_type
    , vehicle_model
    , vehicle_cat
    , vehicle_name
    , max_load
    , sub_cat
    , CASE WHEN (substr(trim(vehicle_name), 1, instr(trim(vehicle_name), regexp_extract(trim(vehicle_name), r'[0-9]'),1)-1)) is null THEN trim(replace(vehicle_name,' ',''))
      ELSE (substr(trim(vehicle_name), 1, CASE WHEN (instr(trim(vehicle_name), regexp_extract(trim(vehicle_name), r'[0-9]'),1)) = 0 then null else instr(trim(vehicle_name), regexp_extract(trim(vehicle_name), r'[0-9]'),1) end-1))
      END as vehicle_com
    , substr(trim(vehicle_name),instr(trim(vehicle_name), regexp_extract(trim(vehicle_name), r'[0-9]'),1),instr(vehicle_name, regexp_extract(vehicle_name, r'[0-9가-힣]$',1))) as backword_string
    ,tranfer_reg_cat
    ,reg_detail
    ,before_reg_detail
    ,before_member_classi
    ,member_classi
    ,dom_import
FROM `itruck-codes.itruck_db_dataset.transfer_reg`
WHERE tranfer_reg_Cat in ('매매업자거래이전','당사자거래이전')
)
select
    date(first_reg_date) as first_reg_date
    , date(sale_reg_date) as sale_reg_date
    , vehicle_type
    , vehicle_model
    , vehicle_cat
    , sub_cat
    , vehicle_name
    , vehicle_com
    , backword_string
    , substr(backword_string, 1, CASE WHEN (instr(backword_string, regexp_extract(vehicle_name, r'[톤]', 1))) = 0 then null else instr(backword_string, regexp_extract(vehicle_name, r'[톤]', 1)) end -1) as ton
    ,trim(replace(substr(backword_string, 
        CASE WHEN instr(backword_string, regexp_extract(backword_string, r'^[0-9]X[0-9]')) is not null then instr(backword_string, regexp_extract(backword_string, r'^[0-9]X[0-9]'))+3
             WHEN instr(backword_string, regexp_extract(backword_string, r'[톤]', 1)) is not null then instr(backword_string, regexp_extract(backword_string, r'[톤]', 1))+1
             WHEN instr(backword_string, regexp_extract(backword_string, r'[㎥]', 1)) is not null then instr(backword_string, regexp_extract(backword_string, r'[㎥]', 1))+1
             WHEN instr(backword_string, regexp_extract(backword_string, r'[a-zA-Z]{2}', 1)) is not null then instr(backword_string, regexp_extract(backword_string, r'[a-zA-Z]{2}', 1))+2
             WHEN instr(backword_string, regexp_extract(backword_string, r'[가-힣]'),1) is not null then instr(backword_string, regexp_extract(backword_string, r'[가-힣]', 1))
        END
          , instr(backword_string, regexp_extract(backword_string, r'[가-힣]$',1))),' ', '')) as sub_category
    , max_load
    , tranfer_reg_cat
    , reg_detail
    , before_reg_detail
    , before_member_classi
    , member_classi
    , dom_import
from ve_name
"""