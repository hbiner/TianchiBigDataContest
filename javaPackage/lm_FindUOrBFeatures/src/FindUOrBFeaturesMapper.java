

import java.io.IOException;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.TaskContext;

public class FindUOrBFeaturesMapper extends MapperBase {
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
		if (context.getJobConf().get("Item").compareTo("brand" ) == 0 ){
		  key.set("brand_id",record.getString(0));
		  value.set("user_id",record.getString(1));	
		}
		else if(  context.getJobConf().get("Item").compareTo("user") ==0 ){
			key.set("user_id",record.getString(0));
			value.set("brand_id",record.getString(1));	
		}
		
		value.set("clicked",record.getDouble(2));
		value.set("bought",record.getDouble(3));
		value.set("collected",record.getDouble(4));
		value.set("basketed",record.getDouble(5));
		value.set("actions",record.getDouble(6));
		
		context.write(key,value);
		
	}

}
