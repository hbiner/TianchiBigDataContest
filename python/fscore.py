create table test as 
select user_id as user_id,
       brand_id as brand_id,
       click_day_after as clickday,
       buy_day_after as buyday,
       last_day_after/92 as lastday
from yi_train_2_user_brand_information

# calculate score
drop table result;
create table result as
select user_id,
       brand_id,
       0.6*clickday+0.8*buyday+0.8*lastday as score
from test  

# final submit result
drop table t_tmall_add_user_brand_predict_dh;
create table t_tmall_add_user_brand_predict_dh as
select user_id,wm_concat(',',brand_id) as brand
from (select user_id,brand_id from result where score>1.61)a
group by user_id;

insert into table evaluation 
select round((hits/pnums),4) precision , round((hits/vnums),4) recall , round((2*hits/(pnums+vnums)),4) F1 , 
        hits , pnums , vnums , getdate() eval_time
from (
    select sum(count_hits(p.brand,v.brand)) hits,
           sum(regexp_count(p.brand,',')+1) pnums,
           sum(regexp_count(v.brand,',')+1) vnums
    from t_tmall_add_user_brand_predict_dh p
         full outer join yi_validate_set v on p.user_id = v.user_id
)a;
