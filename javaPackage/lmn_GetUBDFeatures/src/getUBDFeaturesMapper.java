import java.io.IOException;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.TaskContext;


public class getUBDFeaturesMapper extends MapperBase {
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
		key.set("user_id", record.get("user_id"));
		key.set("brand_id",record.get("brand_id"));
		
		value.set("type", record.get("type"));
		value.set("visit_datetime",record.get("visit_datetime"));
		
		context.write(key, value);
		
	}

}
