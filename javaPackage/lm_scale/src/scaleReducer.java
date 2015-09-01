import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.TaskContext;


public class scaleReducer extends ReducerBase {
	Record output;
	@Override
	public void setup(TaskContext context) throws IOException {
		output = context.createOutputRecord();
	}

	@Override
	public void reduce(Record key, Iterator<Record> values, TaskContext context)
			throws IOException {
		String  date_slice = context.getJobConf().get("date_slice") ;
		date_slice = "2013-" + date_slice ;
		double min = Double.parseDouble( context.getJobConf().get("min") ) ;
		double max = Double.parseDouble( context.getJobConf().get("max") ) ;
		double scalePara =1;
		while (values.hasNext()) {
			Record val = values.next();
			// TODO process value
			String  last_datetime = val.getString("last_datetime");
			last_datetime = "2013-" + last_datetime;
			
			int x = Integer.parseInt( String.valueOf( getQuot( date_slice ,last_datetime  )) ) ;
			scalePara = LineScaling( x , min , max ) ;
			output.setString(0,key.getString("user_id"));
			output.setString(1,key.getString("brand_id"));
			output.setString(2, last_datetime);
			output.setDouble(3,val.getDouble("var") );
			output.setDouble(4, scalePara);
			output.setDouble(5, val.getDouble("var") * scalePara );
			
		    context.write(output);
		   
		}
	}

	/**
	 *  return  x*slope + min 
	 * @param x		x:difference from data_slice
	 * @param max	
	 * @param min	
	 * @return
	 */
	public double LineScaling(double x , double min , double max){
		double difference = max - min ;
		double slope = difference / 65   ;
		return  (65-x)*slope + min ;
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
