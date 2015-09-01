sql("""

create table train_set as
select * 
from t_alibaba_bigdata_user_brand_total_1
where visit_datetime <= '07-15';

""")



sql("""
create table validate_set as
select user_id,brand_id
from t_alibaba_bigdata_user_brand_total_1
where type = '1' and visit_datetime > '07-15'
group by user_id,brand_id

""")

sql("""
create table validate_set_6_16_7_15 as
select user_id,brand_id
from t_alibaba_bigdata_user_brand_total_1
where type = '1' and  visit_datetime>='06-16' and   visit_datetime <= '07-15'
group by user_id,brand_id

""")

sql("""
create table buy_label_set_7_16_8_15 as
select user_id,brand_id
from t_alibaba_bigdata_user_brand_total_1
where type = '1' and  visit_datetime>='07-16' and   visit_datetime <= '08-15'
group by user_id,brand_id
""")

sql("""
create table click_label_set_7_16_8_15 as
select user_id,brand_id
from t_alibaba_bigdata_user_brand_total_1
where type = '0' and  visit_datetime>='07-16' and   visit_datetime <= '08-15'
group by user_id,brand_id
""")










