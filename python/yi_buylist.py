create table yi_buylist as
select distinct user_id,brand_id,1 as target
from t_alibaba_bigdata_user_brand_total_1
where type=1 and visit_datetime>'07-15'