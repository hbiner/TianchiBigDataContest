package lm_GetUserFeature;

import java.io.IOException;
import java.util.Iterator;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.TaskContext;

import java.util.ArrayList;


public class getUserFeatureReducer extends ReducerBase {
	
	Record output ;
	@Override
	public void setup(TaskContext context) throws IOException {
		output = context.createOutputRecord();
	}

	@Override
	public void reduce(Record key, Iterator<Record> values, TaskContext context)
			throws IOException {
		// count the number of brands
		ArrayList<String> useriFeature = new ArrayList<String > ();
		//other features
		// n_click, n_buy, n_collect, n_basket, n_actions
		double []row = {0. , 0. , 0. , 0. , 0. } ;
		
		String strStart = context.getJobConf().get("date_start") ;
		String strEnd = context.getJobConf().get("date_end") ;
		
		while (values.hasNext()) {
			Record val = values.next();
			// find   visit_date in [ date_start , date_end ]
			if( val.getString("visit_datetime" ).compareTo(strStart)< 0 ||
					val.getString("visit_datetime" ).compareTo(strEnd)> 0 )	
				continue;
			
			String _strBrand = val.getString("brand_id");
			int _type = Integer.parseInt( val.getString("type") ) ;
			// TODO process value
			if(  ! useriFeature.contains( _strBrand ) )
			useriFeature.add( _strBrand ) ;
			
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
		output.set(0, key.getString("user_id"));
		output.set(1,(double)useriFeature.size()) ;
		for (int i = 0 ; i<row.length ; i++){
			output.set( i + 2, row[i]);
		}
		
		context.write(output);
		
	}

}
