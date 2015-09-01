
# final submit result
sql("""
drop table if exists t_tmall_add_user_brand_predict_dh;
create table t_tmall_add_user_brand_predict_dh as
select user_id,wm_concat(',',brand_id) as brand
from (select user_id,brand_id
 	from  lm_score_output_all
 	)a
group by user_id;
""")



sql("""
insert into table evaluation 
select round((hits/pnums),4) precision , round((hits/vnums),4) recall , round((2*hits/(pnums+vnums)),4) F1 , 
        hits , pnums , vnums , getdate() eval_time
from (
    select sum(count_hits(p.brand,v.brand)) hits,
           sum(regexp_count(p.brand,',')+1) pnums,
           sum(regexp_count(v.brand,',')+1) vnums
    from t_tmall_add_user_brand_predict_dh p
         full outer join lm_validate_set v on p.user_id = v.user_id  
)a;
""")
