
s1 = "split_part(split_part(classgroupdist,',',1),':',2)"
s2 = "split_part(split_part(classgroupdist,',',2),':',2)"
table = "lmn_train_area_validate_4_11_7_15_lab01_union_x_rf_model";
print sql("select * from 
(select colname,
sum(decode(%s,'',0,cast(%s as bigint)) + decode(%s,'',0,cast(%s as bigint))) as cnt "%(s1,s1,s2,s2) + 
"from %s where not classgroupdist is null group by colname)a order by cnt desc limit 2000"%(table))   