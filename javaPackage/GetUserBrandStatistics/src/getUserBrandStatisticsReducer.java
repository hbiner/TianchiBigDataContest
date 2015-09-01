import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;


import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.TaskContext;




public class getUserBrandStatisticsReducer extends ReducerBase {
	Record output ;
	
	@Override
	public void setup(TaskContext context) throws IOException {
		output = context.createOutputRecord();
	}

	@Override
	public void reduce(Record key, Iterator<Record> values, TaskContext context)
			throws IOException {
		long [] counter = new   long[123] ; //count
		//initialize
		for (int i = 0; i < counter.length; i++) {
			 counter[i] = 0 ;
		}
		
		ArrayList< String [] > backUp_RecordList = new ArrayList<  String []> ();
		
		// statistics
		while (values.hasNext()) {
			Record val = values.next();
			String []  _strVal = new String[2];
			// TODO process value
			_strVal[0] = val.getString("type")  ;
			_strVal[1] = val.getString("visit_datetime")  ;
			backUp_RecordList.add( _strVal );  // save for the next traversal 
			
			if (val.getString("type").compareTo("1") == 0) 
			{
				String  visit_datetime = val.getString("visit_datetime") ;
				visit_datetime = "2013-" + visit_datetime;
				int quot = Integer.parseInt( String.valueOf( getQuot(visit_datetime , "2013-04-15")) ) ;
				counter[quot] = counter[quot] +1 ;
						
			}
			else continue ;
				
		}
		
		//output 
		Iterator<String []> iter = backUp_RecordList.iterator();
		while ( iter.hasNext() ){
			String []  _strVal = new String[2];
			_strVal = iter.next();
			
			String  visit_datetime = _strVal[1];
			visit_datetime = "2013-" + visit_datetime;
			
			int quot = Integer.parseInt( String.valueOf( getQuot(visit_datetime , "2013-04-15")) ) ;			
			
			long days1 =0L ,days7=0L , days15 =0L, days25=0L ;
			
			
			// 1days , mean the same day
			for(int i =quot ; i <quot+1; i++){
				if (i < counter.length )
				days1 = days1 + counter[i];
				else break ;
			}
			// 7days
			for(int i =quot ; i <quot+7; i++){
				if (i < counter.length )
				days7 = days7 + counter[i];
				else break ;
			}
			// 15days
			for(int i =quot ; i <quot+15; i++){
				if (i < counter.length )
				days15 = days15 + counter[i];
				else break ;
			}
			// 25days
			for(int i =quot ; i <quot+25; i++){
				if (i < counter.length )
				days25 = days25 + counter[i];
				else break ;
			}
					
			//output 
			output.setString(0, key.getString("user_id"));
			output.setString(1, key.getString("brand_id"));
			output.setString(2, _strVal[0]);//get "type"
			output.set(3, (long)quot );
			output.set(4, days1);
			output.set(5, days7);
			output.set(6, days15);
			output.set(7, days25);
			
			// even  all columns equal  0 , write still	
			context.write(output);
			
		}
	    
	
		
	}
	
	
	/**
	 * date_diff(time1 , time2 ) ; time1 -time2
	 * @param time1
	 * @param time2
	 * @return long 
	 */
	 private  long getQuot(String time1, String time2){
		  long quot = 0;
		  SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd");
		  try {
		   Date date1 = ft.parse( time1 );
		   Date date2 = ft.parse( time2 );
		   quot = date1.getTime() - date2.getTime();
		   quot = quot / 1000 / 60 / 60 / 24;
		  } catch (ParseException e) {
		   e.printStackTrace();
		  }
		  return quot;
		 }

	

}
