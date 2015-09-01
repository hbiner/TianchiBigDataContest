import java.io.IOException;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.TaskContext;


public class TbUserBrandMapper extends MapperBase {
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
		key.set("brand_id",record.getString(1));
		value.set("type",record.getString(2));
		value.set("visit_datetime",record.getString(3));
		
		context.write(key, value);
	}

}
