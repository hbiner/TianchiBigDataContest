import java.io.IOException;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.TaskContext;


public class scaleMapper extends MapperBase {
	Record key ;
	Record value;
	
	@Override
	public void setup(TaskContext context) throws IOException {
		key = context.createMapOutputKeyRecord();
		value = context.createMapOutputValueRecord();
	}

	@Override
	public void map(long recordNum, Record record, TaskContext context)
			throws IOException {
	key.setString("user_id",record.getString("user_id"));
	key.setString("brand_id",record.getString("brand_id"));
	value.setString("last_datetime", record.getString("last_datetime"));
	//value.setDouble("var",record.getDouble( context.getJobConf().get("var") ) );	
	value.setDouble("var",record.getDouble( context.getJobConf().get("var_name") ) );
	context.write(key, value);
	
	}

}
