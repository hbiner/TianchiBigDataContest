import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.TaskContext;


public class getUOrBFReducer extends ReducerBase {
	 Record output;
	@Override
	public void setup(TaskContext context) throws IOException {
		output = context.createOutputRecord();
	}

	@Override
	public void reduce(Record key, Iterator<Record> values, TaskContext context)
			throws IOException {

        String startDate = context.getJobConf().get("startDate");
        String endDate = context.getJobConf().get("endDate");
        startDate  =  "2013-"+ startDate;
		endDate    =  "2013-"+ endDate;
		 int length=Integer.parseInt( String.valueOf( getQuot(endDate , startDate )) ) ;
        
		int [][] actionArray = new int[4][length+1] ;
		 
		//initial
		for( int i = 0; i<actionArray[0].length; i++){
			actionArray[0][i] =0 ;  // clickArray
			actionArray[1][i] =0 ;	// buyArray
			actionArray[2][i] =0 ;	// collectArray
			actionArray[3][i] =0 ;  // basketArray
		}
		
		int type ;
		int quot;
		while (values.hasNext()) {
			Record val = values.next();
			// TODO process value
		type = Integer.parseInt( val.getString("type") );
		String visit_datetime = val.getString("visit_datetime") ;
		visit_datetime = "2013-" + visit_datetime;
		
		if(visit_datetime.compareTo(startDate)<0 || 
			visit_datetime.compareTo(endDate) >0  ) continue ;
	
		 quot = Integer.parseInt( String.valueOf( getQuot(endDate , visit_datetime )) ) ;
		 actionArray[type][quot] =1;
		 
		}
		
		double  click_days=0 ,
				buy_days=0,
				collect_days=0 ,
				basket_days=0 ;
		double [] sum_days = new double [length+1] ;
		for ( int i =0 ; i <length+1 ; i++){   // 4  features 
			click_days += actionArray[0][i];
			buy_days	+= actionArray[1][i];
			collect_days +=actionArray[2][i];
			basket_days += actionArray[3][i];
			sum_days[i] = actionArray[0][i]+actionArray[1][i]+actionArray[2][i]+actionArray[3][i] ;
		}
		double dist_days = 0;
		for( int i = 0 ; i <length+1 ; i ++){
			if(sum_days[i]>0 ) dist_days ++;
		}
		
		//set the output 
		output.setString(0, key.getString(0));
		output.setDouble(1, click_days);
		output.setDouble(2, buy_days);
		output.setDouble(3, collect_days);
		output.setDouble(4, basket_days);
		output.setDouble(5, click_days + buy_days + collect_days + basket_days);
		output.setDouble(6, dist_days);
		
		context.write(output);
			
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
