import java.io.IOException;
import java.util.Iterator;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.TaskContext;

import java.util.Set;
import java.util.HashSet;

public class TbUserBrandReducer extends ReducerBase {
	Record output;
	@Override
	public void setup(TaskContext context) throws IOException {
		output = context.createOutputRecord();
	}

	@Override
	public void reduce(Record key, Iterator<Record> values, TaskContext context)
			throws IOException {
		
		Long ub_clicks = 0L;
		Long ub_buy = 0L;
		Set<String> dates = new HashSet<String>();
		while (values.hasNext()) {
			Record val = values.next();

			if(Integer.parseInt(val.getString("type"))==0){
				ub_clicks++;
			}
			if(Integer.parseInt(val.getString("type"))==1){
				ub_buy++;
			}
			dates.add(val.getString("visit_datetime"));
		}
		output.set(0, key.getString("user_id"));
		output.set(1, key.getString("brand_id"));
		output.set(2, ub_clicks);
		output.set(3, ub_buy);
		output.set(4, Long.valueOf(dates.size()));
		
		context.write(output);
	}

}
