import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.TaskContext;


public class getUBDFeaturesReducer extends ReducerBase {
	Record output;
	@Override
	public void setup(TaskContext context) throws IOException {
		output = context.createOutputRecord();
	}

	@Override
	public void reduce(Record key, Iterator<Record> values, TaskContext context)
			throws IOException {

		String date_slice = context.getJobConf().get("date_slice");
		date_slice    =  "2013-"+ date_slice;
		double [][] actionArray = new double[4][92] ;
		 
		//initial
		for( int i = 0; i<92; i++){
			actionArray[0][i] =0 ;  // clickArray
			actionArray[1][i] =0 ;	// buyArray
			actionArray[2][i] =0 ;	// collectArray
			actionArray[3][i] =0 ;  // basketArray
		}
		
	
		int  type =0 ;
		int  quot ;
		int n_cross = Integer.parseInt( context.getJobConf().get("n_cross") );// 7 or 8
		int []crossStep = {1,3,5,7,10,15,25,26};
		
		int endDays=0;
		for (int i=0; i<n_cross;i++){
			endDays += crossStep[i] ;
		}
	
		while (values.hasNext() ) {
			  
			 Record val = values.next();
			// TODO process value
			  type = Integer.parseInt( val.getString("type") );
			 String visit_datetime=val.getString("visit_datetime");
			 
			 visit_datetime = "2013-" + visit_datetime;
			 
			 quot = Integer.parseInt( String.valueOf( getQuot(date_slice , visit_datetime )) ) ;
			
			if (quot >= 0  && quot < endDays )  //if beyond the date area
		 	actionArray[type][quot] = 1;
			
		}
		
		double actionDayDist=0  ; //1 features
		for( int i=0 ;i < endDays ; i++){
			if (actionArray[0][i] > 0)  {actionDayDist ++ ;continue ; }
			if (actionArray[1][i] > 0)  {actionDayDist ++ ;continue ; }
			if (actionArray[2][i] > 0)  {actionDayDist ++ ;continue ; }
			if (actionArray[3][i] > 0)  {actionDayDist ++ ;continue ; }
		}
		
		
		int pointer = 0; 
		double [][]typeActCross = new double[4][n_cross];  // 4 * n_cross features
		for (int i = 0 ; i<n_cross;i++ ){
			double _click=0 , _buy=0 , _collect=0 , _basket=0 ;
			for( int j = pointer ; j<pointer+crossStep[i] ; j++){
				// calculate the w_i
				_click += actionArray[0][j] ;
				_buy  += actionArray[1][j] ;
				_collect += actionArray[2][j];
				_basket  += actionArray[3][j] ;
			}
			pointer +=crossStep[i]; //update the pointer
			typeActCross[0][i]=_click;
			typeActCross[1][i]=_buy;
			typeActCross[2][i]=_collect;
			typeActCross[3][i]=_basket;
		}
		
		double ub_click_days=0 ,
				ub_buy_days=0,
				ub_collect_days=0 ,
				ub_basket_days=0 ;
		for ( int i =0 ; i <n_cross ; i++){   // 4  features 
			ub_click_days += typeActCross[0][i];
			ub_buy_days	+= typeActCross[1][i];
			ub_collect_days +=typeActCross[2][i];
			ub_basket_days += typeActCross[3][i];
		}
		// 1 features 
		double ub_sum_days =  ub_click_days	 + ub_buy_days+  
				             ub_collect_days + ub_basket_days ;
		
		if  (ub_sum_days ==0 ) return ; // if no action  ,then return 
		
		// output
		output.setString(0, key.getString("user_id"));
		output.setString(1, key.getString("brand_id"));
		// 32 features
		int f_index=0;// feature index
		for(int i = 0 ; i<4; i++){
			for(int j = 0 ; j<n_cross; j++){
				output.setDouble(2+f_index , typeActCross[i][j]);
				f_index++;// point to next feature 
			}
		}
		//  5 features 
		output.setDouble(2+f_index , ub_click_days );
		f_index++;
		output.setDouble(2+f_index , ub_buy_days );
		f_index++;
		output.setDouble(2+f_index , ub_collect_days );
		f_index++;
		output.setDouble(2+f_index , ub_basket_days );
		f_index++;
		output.setDouble(2+f_index , ub_sum_days );
		f_index++;
		output.setDouble(2+f_index, actionDayDist);
		
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
