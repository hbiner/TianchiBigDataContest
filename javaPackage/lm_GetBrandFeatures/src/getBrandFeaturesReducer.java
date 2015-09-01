import java.io.IOException;
//import java.util.ArrayList;
import java.util.Iterator;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.TaskContext;


public class getBrandFeaturesReducer extends ReducerBase {
	Record output ;
	
	
	@Override
	public void setup(TaskContext context) throws IOException {
		output = context.createOutputRecord();
	}

	
	@Override
	public void reduce(Record key, Iterator<Record> values, TaskContext context)
			throws IOException {

		        // count the number of users
				//ArrayList<String> brandiFeature = new ArrayList<String > ();
				//other features
				// b_n_click, b_n_buy, b_n_collect, b_n_basket, b_n_actions
				double []row = {0. , 0. , 0. , 0. , 0. } ;
				
				String strStart = context.getJobConf().get("date_start") ;
				String strEnd = context.getJobConf().get("date_end") ;
				
				while (values.hasNext()) {
					Record val = values.next();
					// find   visit_date in [ date_start , date_end ]
					if( val.getString("visit_datetime" ).compareTo(strStart)< 0 ||
							val.getString("visit_datetime" ).compareTo(strEnd)>0 )	
						continue;
					
				//	String _strUser = val.getString("user_id");
					int _type = Integer.parseInt( val.getString("type") ) ;
					
					/*if(  ! brandiFeature.contains( _strUser ) )
					brandiFeature.add( _strUser ) ;
					*/
					
					row[_type] = row[_type] + 1;		
				}
				//calculate sum of row
				double sum =0.;
				for (int i = 0 ; i<row.length-1 ; i++){
					sum = sum+ row[i];
				}
				row[4] = sum; // n_actions
				
				if (sum == 0)return ; // if all columns equal  0 ,don't write
				
				//output
				output.set(0, key.getString("brand_id"));
				output.set(1, key.getString("user_id"));
				//output.set(1,(double)brandiFeature.size()) ;
				
				for (int i = 0 ; i<row.length ; i++){
					output.set( i + 2, row[i]);
				}
				
				context.write(output);
				
	}
	

}
