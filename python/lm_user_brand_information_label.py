sql("""
drop table if exists lm_user_brand_information_label ;
create table lm_user_brand_information_label as
select distinct user_id,
                brand_id,
                case 
                when nclick.n_click is null then 0
                when nclick.n_click is not null then 1
                end as click_label        
from  (
    select user_id,brand_id,count(brand_id) as n_click
    from  small_sample
    where type='0' and visit_datetime > "07-15"
    group by user_id,brand_id) nclick

;
""")

  


