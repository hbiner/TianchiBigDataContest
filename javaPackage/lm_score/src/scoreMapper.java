import java.io.IOException;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.TaskContext;


public class scoreMapper extends MapperBase {
	Record key;
	Record value;
	
	@Override
	public void setup(TaskContext context) throws IOException {
		key = context.createMapOutputKeyRecord();
		value = context.createMapOutputValueRecord();
	}

	@Override
	public void map(long recordNum, Record record, TaskContext context)
			throws IOException {
		key.set("user_id",record.getString(0));
		key.set("fre_click",record.getDouble(6));
		key.set("fre_buy",record.getDouble(7));
		key.set("actions",record.getBigint(8));
		//key.set("count_buy_brand_kinds",record.getBigint(8));
		
		
		value.set("brand_id",record.getString(1));
		value.set("n_click",record.getBigint(2));
		value.set("n_buy",record.getBigint(3));
		value.set("n_collect",record.getBigint(4));
		value.set("n_basket",record.getBigint(5));
	
		context.write(key,value);	 
	}
}
