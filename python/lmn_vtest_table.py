

sql("""
create table train_set as
select * 
from t_alibaba_bigdata_user_brand_total_1
where visit_datetime <= '07-15';
""")

sql("""
create table validate_set as
select
    user_id,
    wm_concat(',',brand_id) as brand
from(
    select distinct user_id,brand_id
    from t_alibaba_bigdata_user_brand_total_1
    where type = '1'and visit_datetime > '07-15'
)a
group by user_id;
""")


sql("""
create table evaluation as
select (hits/pnums) precision , (hits/vnums) recall , (2*hits/(pnums+vnums)) F1 , 
        hits , pnums , vnums , getdate() eval_time
from (
    select sum(count_hits(p.brand,v.brand) ) hits,
           sum(regexp_count(p.brand,',')+1) pnums,
          sum(regexp_count(v.brand,',')+1) vnums
    from t_tmall_add_user_brand_predict_dh p
        full outer join validate_set v on p.user_id = v.user_id
)a;
""")



