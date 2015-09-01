import java.io.IOException;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.TaskContext;


public class getUOrBFMapper extends MapperBase {

	Record key ;
	Record value;
	
	@Override
	public void setup(TaskContext context) throws IOException {
		key = context.createMapOutputKeyRecord() ;
		value = context.createMapOutputValueRecord();
	}

	@Override
	public void map(long recordNum, Record record, TaskContext context)
			throws IOException {
		if(context.getJobConf().get("item").compareTo("user_id") == 0 ){
			key.set("user_id", record.get("user_id"));
			value.set("brand_id", record.get("brand_id"));
		}
		else if(context.getJobConf().get("item").compareTo("brand_id") == 0 ){
			key.set("brand_id", record.get("brand_id"));
			value.set("user_id", record.get("user_id"));
		}
		
		value.setString("type", record.getString("type"));
		value.setString("visit_datetime",record.getString("visit_datetime"));
		
		context.write(key, value);
		
	}

}
