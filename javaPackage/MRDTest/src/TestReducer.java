

import java.io.IOException;
import java.util.Iterator;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.TaskContext;


import java.util.Map;
import java.util.TreeMap;



public class TestReducer extends ReducerBase {
    Record output; //
	
	@Override
	public void setup(TaskContext context) throws IOException {
			output = context.createOutputRecord();  //init
	}

	@Override
	public void reduce(Record key, Iterator<Record> values, TaskContext context)
			throws IOException {
		// map 
		Map<String , Long[]> typeCounter = new TreeMap<String,Long[]>();
		

		while (values.hasNext()) {
			Record val = values.next();
			String date = val.getString("visit_datetime");
			int type = Integer.parseInt(val.getString("type"));
		
			if (typeCounter.containsKey(date)){//if "date"  exist in typeCounter 
			typeCounter.get(date)[type]++;
			}else{
				Long[] counter = new Long[]{0L,0L,0L,0L};
				counter[type]++;
				typeCounter.put(date, counter);
			 }
		    }// end of: while (values.hasNext())
			// TODO process value
		
		output.set(0,key.getString("user_id"));
		output.set(1,key.getString("brand_id"));
		Long cumClicks = 0L;
		
		for (String date : typeCounter.keySet()){// traversal
			 
			output.set(2,date);
			output.set(3,typeCounter.get(date)[0]);
			output.set(4, typeCounter.get(date)[1]);
			output.set(5,typeCounter.get(date)[2]);
			output.set(6,typeCounter.get(date)[3]);
			
			
			cumClicks += typeCounter.get(date)[0];
			output.set(7,cumClicks);
			context.write(output);
	
		}//end of : for String date : typeCounter.keySet())
			
		}

} // end of function
