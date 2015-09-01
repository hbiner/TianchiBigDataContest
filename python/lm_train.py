
sql("""
drop table if exists lm_train ;
create table lm_train as
select 
	lm_user_brand_information_train.user_id,
	lm_user_brand_information_train.brand_id,
	n_click,
	n_buy,
	n_collect,
	n_basket,
	click_label
from lm_user_brand_information_train
left outer join
	lm_user_brand_information_label
on  lm_user_brand_information_train.user_id = lm_user_brand_information_label.user_id
 and lm_user_brand_information_train.brand_id = lm_user_brand_information_label.brand_id
;
""")