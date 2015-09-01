sql("""
-- 把表中的 visit_date 转化成 距离 04.15 的天数
create table lm_total_date_changed_small_sample as 
select 
  user_id ,
  brand_id ,
  type ,
  datediff(concat('2013-',visit_datetime,' 12:01:01'),'2013-04-15 12:01:01','dd') as dateDistance  --假设是2013年的资料 
from small_sample ;
""")