drop table t_tmall_add_user_brand_predict_dh;
create table t_tmall_add_user_brand_predict_bh as
select
   user_id,
   wm_concat(',',brand_id) as brand
from (
	select
		user_id,
		brand_id,
		row_number() over(partition by user_id order by num desc) as rank
form (
	select
		user_id,
		brand_id,
		count(brand_id) as num
	from
		t_alibaba_bigdata_user_brand_total_1
	where type = '0';
group by
		user_id,
		brand_id
		)a
)b
where 
  randk<=10
group by
user_id;
