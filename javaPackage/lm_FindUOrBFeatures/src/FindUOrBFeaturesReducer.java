import java.io.IOException;

import java.util.ArrayList;

import java.util.Iterator;


import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.TaskContext;


public class FindUOrBFeaturesReducer extends ReducerBase {
	Record output ;
	
	@Override
	public void setup(TaskContext context) throws IOException {
		output = context.createOutputRecord();
	}

	
	@Override
	public void reduce(Record key, Iterator<Record> values,  TaskContext context)
			throws IOException {
		
		double clickThis = 0 , buyThis = 0 , collectThis = 0 , basketThis = 0  , actionThis = 0;
		
		double InteractCounter = 0.0 ;
		ArrayList<String> clickCounter = new ArrayList<String > ();
		ArrayList<String> buyCounter = new ArrayList<String > ();
		ArrayList<String> collectCounter = new ArrayList<String > ();
		ArrayList<String> basketCounter = new ArrayList<String > ();
		
		String InteractItem; //

		
		while (values.hasNext()) {
			
			
			Record val= values.next();
			InteractCounter ++; // as  the sql ( select distinct )
			if ( InteractCounter % 100 == 99 ) context.progress();  // 模拟定时器，发出心跳信息
			
			// TODO process value
			clickThis = clickThis + val.getDouble("clicked");
			buyThis = buyThis + val.getDouble("bought") ;
			collectThis = collectThis + val.getDouble("collected");
			basketThis = basketThis + val.getDouble("basketed");
			actionThis = actionThis + val.getDouble("actions");
			InteractItem = val.getString(0);
			
			if ( val.getDouble("clicked") > 0  ){
				if(! clickCounter.contains(InteractItem)  ) 
					clickCounter.add(InteractItem);
			}
			if ( val.getDouble("bought") > 0  ){
				if(! buyCounter.contains(InteractItem)  ) 
					buyCounter.add(InteractItem);
			}
			if ( val.getDouble("collected") > 0  ){
				if(! collectCounter.contains(InteractItem)  ) 
					collectCounter.add(InteractItem);
			}
			if ( val.getDouble("basketed") > 0  ){
				if(! basketCounter.contains(InteractItem)  ) 
					basketCounter.add(InteractItem);
			}
					
		}
		
		output.set(0, key.get(0));
		output.setDouble(1,clickThis);
		output.setDouble(2, buyThis);
		output.setDouble(3, collectThis);
		output.setDouble(4, basketThis );
		
		output.setDouble(5, actionThis );
		
		output.setDouble(6, (double)clickCounter.size());
		output.setDouble(7, (double)buyCounter.size());
		output.setDouble(8, (double)collectCounter.size());
		output.setDouble(9, (double)basketCounter.size());
		output.setDouble(10, InteractCounter );
			
		context.write(output);
			
	}
	
	
	

}
