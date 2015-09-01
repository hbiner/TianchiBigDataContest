import java.io.IOException;

import java.util.Iterator;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.TaskContext;




public class GetFeaturesReducer extends ReducerBase {
	
	Record output ;
	@Override
	public void setup(TaskContext context) throws IOException {
		output = context.createOutputRecord();
	}
	
	@Override
	public void reduce(Record key, Iterator<Record> values, TaskContext context)
			throws IOException {
        double  []row = new double[32] ;
        //initialize
        for(int i = 0; i <row.length;i++){
        	row[i] = 0;
        }
        int _index = 0;
        int _featuresEachAction = 6;
        int  set_period = Integer.parseInt( context.getJobConf().get("set_period") );
        
        String last_visit_datetime = "04-14" ; // save the last visit time
        
		while (values.hasNext()) {
			// TODO process value
			Record val = values.next();
			
			String strTime = val.getString("visit_datetime");			 
			String strType = val.getString("type");
			
			switch ( set_period )
			{ // if visite_datetime is out of the scan range ,then return -1
			case 420615 : ///////
			{_index = getDimIndex_4_20_6_15(strTime ,strType ) ; break;}
			case 413608 : ///////
			{_index = getDimIndex_4_13_6_08(strTime ,strType ) ; break;}
			case 422617 : ///////
			{_index = getDimIndex_4_22_6_17(strTime ,strType ) ; break;}
			case 429624 : ///////
			{_index = getDimIndex_4_29_6_24(strTime ,strType ) ; break;}
			case 506701 : ///////
			{ _index = getDimIndex_5_06_7_01(strTime ,strType ) ; break;}
			case 513708 : ///////
			{ _index = getDimIndex_5_13_7_08(strTime ,strType ) ; break;}
			case 520715  : ///////
			{ _index = getDimIndex_5_20_7_15(strTime ,strType ) ; break;}
			case 620815 : ///////
			{ _index = getDimIndex_6_20_8_15(strTime ,strType ) ; break;}
			case 411615: ////// 7 features 
			{ _index = getDimIndex_4_11_6_15(strTime ,strType ) ; _featuresEachAction =7; break;}
			case 511715: ////// 7 features 
			{ _index = getDimIndex_5_11_7_15(strTime ,strType ) ; _featuresEachAction =7; break;}
			case 611815: ////// 7 features 
			{ _index = getDimIndex_6_11_8_15(strTime ,strType ) ; _featuresEachAction =7; break;}
			case 8516815: ////// 8 features 
			{ _index = getDimIndex_5_16_8_15(strTime ,strType ) ; _featuresEachAction =8; break;}
			case 8415715: ////// 8 features 
			{ _index = getDimIndex_4_15_7_15(strTime ,strType ) ; _featuresEachAction =8; break;}
			
			 default: // illegal input 
			 { _index = -100 ; } //error input XXXXXX
			}
			
			if( _index != -1 ) // 如果其行为时间不在范围内，则不考虑
			{
				row[_index] = row[_index] + 1 ;	
				if ( strTime.compareTo(last_visit_datetime) > 0 )//find the  last visit time
				last_visit_datetime = strTime ;
			}
			
		}
		
		
		//output 
			output.setString(0, key.getString("user_id"));
			output.setString(1, key.getString("brand_id"));
		
		double sum =0.;	
		int _featuresNum = _featuresEachAction * 4 ;
		for( int i = 0; i< _featuresNum  ; i ++){	
			output.setDouble(i+2, row[i]);	
			sum = sum + row[i] ;
		}
		
		if (sum == 0)return ; // if all columns equal  0 ,don't write
		
		//click_num , buy_num , collect_num , basket_num 
		double total_actions = 0;	
			for (int ty = 0 ; ty <4 ;ty ++){
			sum =0 ;// clear sum ;
			for( int i = _featuresEachAction * ty ; i< _featuresEachAction * (ty+1)  ; i ++){	
				sum = sum + row[i] ;
			}
			total_actions = total_actions  +  sum ;
			output.setDouble(ty+2+_featuresNum, sum);	
		 }
			
		output.setDouble(4+2+_featuresNum, total_actions);	// total	
		output.setString(4+2+_featuresNum + 1, last_visit_datetime);	// last visit datetime
		
		
		context.write(output);
	}
	
	/**
	 * get the index in feature vector
	 * @param visit_date	visit_datetime
	 * @param type			type
	 * @return				index in feature vector
	 */
	public int getDimIndex_4_20_6_15(String visit_date , String type){
		if ( visit_date.compareTo("06-14")>=0 & visit_date.compareTo("06-15")<=0 ){ //0~1 days
			return 0 + 6 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("06-11")>=0 & visit_date.compareTo("06-13")<=0 ){//3 days
			return 1 + 6 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("06-06")>=0 & visit_date.compareTo("06-10")<=0 ){//5 days
			return 2 + 6 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("05-30")>=0 & visit_date.compareTo("06-05")<=0 ){//7 days
			return 3 + 6 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("05-15")>=0 & visit_date.compareTo("05-29")<=0 ){//15 days
			return 4 + 6 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("04-20")>=0 & visit_date.compareTo("05-14")<=0 ){//25 days
			return 5 + 6 * Integer.parseInt(type);}
		else return -1;//beyond the period
		
	}	
	
	/**
	 * get the index in feature vector
	 * @param visit_date	visit_datetime
	 * @param type			type
	 * @return				index in feature vector
	 */
	public int getDimIndex_4_13_6_08(String visit_date , String type){
		if ( visit_date.compareTo("06-07")>=0 & visit_date.compareTo("06-08")<=0 ){ //0~1 days
			return 0 + 6 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("06-04")>=0 & visit_date.compareTo("06-06")<=0 ){//3 days
			return 1 + 6 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("05-30")>=0 & visit_date.compareTo("06-03")<=0 ){//5 days
			return 2 + 6 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("05-23")>=0 & visit_date.compareTo("05-29")<=0 ){//7 days
			return 3 + 6 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("05-08")>=0 & visit_date.compareTo("05-22")<=0 ){//15 days
			return 4 + 6 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("04-13")>=0 & visit_date.compareTo("05-07")<=0 ){//25 days
			return 5 + 6 * Integer.parseInt(type);}
		else return -1;//beyond the period
		
	}	
	
	
	
	
	/**
	 * get the index in feature vector
	 * @param visit_date	visit_datetime
	 * @param type			type
	 * @return				index in feature vector
	 */
	public int getDimIndex_5_20_7_15(String visit_date , String type){
		if ( visit_date.compareTo("07-14")>=0 & visit_date.compareTo("07-15")<=0 ){ //0~1 days
			return 0 + 6 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("07-11")>=0 & visit_date.compareTo("07-13")<=0 ){//3 days
			return 1 + 6 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("07-06")>=0 & visit_date.compareTo("07-10")<=0 ){//5 days
			return 2 + 6 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("06-29")>=0 & visit_date.compareTo("07-05")<=0 ){//7 days
			return 3 + 6 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("06-14")>=0 & visit_date.compareTo("06-28")<=0 ){//15 days
			return 4 + 6 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("05-20")>=0 & visit_date.compareTo("06-13")<=0 ){//25 days
			return 5 + 6 * Integer.parseInt(type);}
		else return -1;//beyond the period
		
	}
	
	public int getDimIndex_5_13_7_08(String visit_date , String type){
		if ( visit_date.compareTo("07-07")>=0 & visit_date.compareTo("07-08")<=0 ){ //0~1 days
			return 0 + 6 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("07-04")>=0 & visit_date.compareTo("07-06")<=0 ){//3 days
			return 1 + 6 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("06-29")>=0 & visit_date.compareTo("07-03")<=0 ){//5 days
			return 2 + 6 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("06-22")>=0 & visit_date.compareTo("06-28")<=0 ){//7 days
			return 3 + 6 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("06-07")>=0 & visit_date.compareTo("06-21")<=0 ){//15 days
			return 4 + 6 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("05-13")>=0 & visit_date.compareTo("06-06")<=0 ){//25 days
			return 5 + 6 * Integer.parseInt(type);}
		
		else return -1;//beyond the period
		
	}
		
	public int getDimIndex_5_06_7_01(String visit_date , String type){
		if ( visit_date.compareTo("06-30")>=0 & visit_date.compareTo("07-01")<=0 ){ //0~1 days
			return 0 + 6 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("06-27")>=0 & visit_date.compareTo("06-29")<=0 ){//3 days
			return 1 + 6 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("06-22")>=0 & visit_date.compareTo("06-26")<=0 ){//5 days
			return 2 + 6 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("06-15")>=0 & visit_date.compareTo("06-21")<=0 ){//7 days
			return 3 + 6 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("05-31")>=0 & visit_date.compareTo("06-14")<=0 ){//15 days
			return 4 + 6 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("05-06")>=0 & visit_date.compareTo("05-30")<=0 ){//25 days
			return 5 + 6 * Integer.parseInt(type);}
		
		else return -1;//beyond the period
		
	}
	
	public int getDimIndex_4_29_6_24(String visit_date , String type){
		if ( visit_date.compareTo("06-23")>=0 & visit_date.compareTo("06-24")<=0 ){ //0~1 days
			return 0 + 6 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("06-20")>=0 & visit_date.compareTo("06-22")<=0 ){//3 days
			return 1 + 6 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("06-15")>=0 & visit_date.compareTo("06-19")<=0 ){//5 days
			return 2 + 6 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("06-08")>=0 & visit_date.compareTo("06-14")<=0 ){//7 days
			return 3 + 6 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("05-24")>=0 & visit_date.compareTo("06-07")<=0 ){//15 days
			return 4 + 6 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("04-29")>=0 & visit_date.compareTo("05-23")<=0 ){//25 days
			return 5 + 6 * Integer.parseInt(type);}
		
		else return -1;//beyond the period
		
	}
	
	public int getDimIndex_4_22_6_17(String visit_date , String type){
		if ( visit_date.compareTo("06-16")>=0 & visit_date.compareTo("06-17")<=0 ){ //0~1 days
			return 0 + 6 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("06-13")>=0 & visit_date.compareTo("06-15")<=0 ){//3 days
			return 1 + 6 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("06-08")>=0 & visit_date.compareTo("06-12")<=0 ){//5 days
			return 2 + 6 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("06-01")>=0 & visit_date.compareTo("06-07")<=0 ){//7 days
			return 3 + 6 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("05-17")>=0 & visit_date.compareTo("05-31")<=0 ){//15 days
			return 4 + 6 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("04-22")>=0 & visit_date.compareTo("05-16")<=0 ){//25 days
			return 5 + 6 * Integer.parseInt(type);}
		
		else return -1;//beyond the period
		
	}
	
	
	
	
	
	/**
	 * get the index in feature vector
	 * @param visit_date	visit_datetime
	 * @param type			type
	 * @return				index in feature vector
	 */
	public int getDimIndex_6_20_8_15(String visit_date , String type){
		if ( visit_date.compareTo("08-14")>=0 & visit_date.compareTo("08-15")<=0 ){ //0~1 days
			return 0 + 6 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("08-11")>=0 & visit_date.compareTo("08-13")<=0 ){//3 days
			return 1 + 6 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("08-06")>=0 & visit_date.compareTo("08-10")<=0 ){//5 days
			return 2 + 6 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("07-30")>=0 & visit_date.compareTo("08-05")<=0 ){//7 days
			return 3 + 6 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("07-15")>=0 & visit_date.compareTo("07-29")<=0 ){//15 days
			return 4 + 6 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("06-20")>=0 & visit_date.compareTo("07-14")<=0 ){//25 days //注意，这里比较特殊，以后要改，把4 / 5月份的资料也用上
			return 5 + 6 * Integer.parseInt(type);}
		else return -1;//beyond the period
		
	}
	/**
	 * get the index in feature vector
	 * @param visit_date	visit_datetime
	 * @param type			type
	 * @return				index in feature vector
	 */
	public int getDimIndex_6_13_8_08(String visit_date , String type){
		if ( visit_date.compareTo("08-07")>=0 & visit_date.compareTo("08-08")<=0 ){ //0~1 days
			return 0 + 6 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("08-04")>=0 & visit_date.compareTo("08-06")<=0 ){//3 days
			return 1 + 6 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("07-30")>=0 & visit_date.compareTo("08-03")<=0 ){//5 days
			return 2 + 6 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("07-23")>=0 & visit_date.compareTo("07-29")<=0 ){//7 days
			return 3 + 6 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("07-08")>=0 & visit_date.compareTo("07-22")<=0 ){//15 days
			return 4 + 6 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("06-13")>=0 & visit_date.compareTo("07-07")<=0 ){//25 days //注意，这里比较特殊，以后要改，把4 / 5月份的资料也用上
			return 5 + 6 * Integer.parseInt(type);}
		else return -1;//beyond the period
		
	}
	
	
	/**
	 * get the index in feature vector
	 * @param visit_date	visit_datetime
	 * @param type			type
	 * @return				index in feature vector
	 */
	//  each type has 7 features here
	public int getDimIndex_5_25_8_15(String visit_date , String type){
		if ( visit_date.compareTo("08-14")>=0 & visit_date.compareTo("08-15")<=0 ){ //0~1 days
			return 0 + 7 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("08-11")>=0 & visit_date.compareTo("08-13")<=0 ){//3 days
			return 1 + 7 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("08-06")>=0 & visit_date.compareTo("08-10")<=0 ){//5 days
			return 2 + 7 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("07-30")>=0 & visit_date.compareTo("08-05")<=0 ){//7 days
			return 3 + 7 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("07-15")>=0 & visit_date.compareTo("07-29")<=0 ){//15 days
			return 4 + 7 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("06-25")>=0 & visit_date.compareTo("07-14")<=0 ){//25 days //注意，这里比较特殊，以后要改，把4 / 5月份的资料也用上
			return 5 + 7 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("05-25")>=0 & visit_date.compareTo("06-23")<=0 ){//25 days //注意，这里比较特殊，以后要改，把4 / 5月份的资料也用上
			return 6 + 7 * Integer.parseInt(type);}
		else return -1;//beyond the period
		
	}
	
	
	
	/**
	 * get the index in feature vector
	 * @param visit_date	visit_datetime
	 * @param type			type
	 * @return				index in feature vector
	 */
	public int getDimIndex_4_11_6_15(String visit_date , String type){
		if ( visit_date.compareTo("06-15")>=0 & visit_date.compareTo("06-15")<=0 ){ //1 days
			return 0 + 7 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("06-12")>=0 & visit_date.compareTo("06-14")<=0 ){//3 days
			return 1 + 7 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("06-07")>=0 & visit_date.compareTo("06-11")<=0 ){//5 days
			return 2 + 7 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("05-31")>=0 & visit_date.compareTo("06-06")<=0 ){//7 days
			return 3 + 7 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("05-21")>=0 & visit_date.compareTo("05-30")<=0 ){//10 days
			return 4 + 7 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("05-06")>=0 & visit_date.compareTo("05-20")<=0 ){//15 days
			return 5 + 7 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("04-11")>=0 & visit_date.compareTo("05-05")<=0 ){//25 days
			return 6 + 7 * Integer.parseInt(type);}
		
		else return -1;//beyond the period
		
	}	
	
	/**
	 * get the index in feature vector
	 * @param visit_date	visit_datetime
	 * @param type			type
	 * @return				index in feature vector
	 */
	public int getDimIndex_5_11_7_15(String visit_date , String type){
		if ( visit_date.compareTo("07-15")>=0 & visit_date.compareTo("07-15")<=0 ){ //1 days
			return 0 + 7 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("07-12")>=0 & visit_date.compareTo("07-14")<=0 ){//3 days
			return 1 + 7 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("07-07")>=0 & visit_date.compareTo("07-11")<=0 ){//5 days
			return 2 + 7 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("06-30")>=0 & visit_date.compareTo("07-06")<=0 ){//7 days
			return 3 + 7 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("06-20")>=0 & visit_date.compareTo("06-29")<=0 ){//10 days
			return 4 + 7 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("06-05")>=0 & visit_date.compareTo("06-19")<=0 ){//15 days
			return 5 + 7 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("05-11")>=0 & visit_date.compareTo("06-04")<=0 ){//25 days
			return 6 + 7 * Integer.parseInt(type);}
		
		else return -1;//beyond the period
		
	}	
	
	
	/**
	 * get the index in feature vector
	 * @param visit_date	visit_datetime
	 * @param type			type
	 * @return				index in feature vector
	 */
	public int getDimIndex_4_15_7_15(String visit_date , String type){
		if ( visit_date.compareTo("07-15")>=0 & visit_date.compareTo("07-15")<=0 ){ //1 days
			return 0 + 8 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("07-12")>=0 & visit_date.compareTo("07-14")<=0 ){//3 days
			return 1 + 8 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("07-07")>=0 & visit_date.compareTo("07-11")<=0 ){//5 days
			return 2 + 8 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("06-30")>=0 & visit_date.compareTo("07-06")<=0 ){//7 days
			return 3 + 8 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("06-20")>=0 & visit_date.compareTo("06-29")<=0 ){//10 days
			return 4 + 8 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("06-05")>=0 & visit_date.compareTo("06-19")<=0 ){//15 days
			return 5 + 8 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("05-11")>=0 & visit_date.compareTo("06-04")<=0 ){//25 days
			return 6 + 8 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("04-15")>=0 & visit_date.compareTo("05-10")<=0 ){//26 days
			return 7 + 8 * Integer.parseInt(type);}
		else return -1;//beyond the period
		
	}	
	
	
	
	
	/**
	 * get the index in feature vector
	 * @param visit_date	visit_datetime
	 * @param type			type
	 * @return				index in feature vector
	 */
	public int getDimIndex_6_11_8_15(String visit_date , String type){
		if ( visit_date.compareTo("08-15")>=0 & visit_date.compareTo("08-15")<=0 ){ //1 days
			return 0 + 7 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("08-12")>=0 & visit_date.compareTo("08-14")<=0 ){//3 days
			return 1 + 7 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("08-07")>=0 & visit_date.compareTo("08-11")<=0 ){//5 days
			return 2 + 7 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("07-31")>=0 & visit_date.compareTo("08-06")<=0 ){//7 days
			return 3 + 7 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("07-21")>=0 & visit_date.compareTo("07-30")<=0 ){//10 days
			return 4 + 7 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("07-06")>=0 & visit_date.compareTo("07-20")<=0 ){//15 days
			return 5 + 7 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("06-11")>=0 & visit_date.compareTo("07-05")<=0 ){//25 days
			return 6 + 7 * Integer.parseInt(type);}
		
		else return -1;//beyond the period
		
	}	
	
	public int getDimIndex_5_16_8_15(String visit_date , String type){
		if ( visit_date.compareTo("08-15")>=0 & visit_date.compareTo("08-15")<=0 ){ //1 days
			return 0 + 8 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("08-12")>=0 & visit_date.compareTo("08-14")<=0 ){//3 days
			return 1 + 8 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("08-07")>=0 & visit_date.compareTo("08-11")<=0 ){//5 days
			return 2 + 8 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("07-31")>=0 & visit_date.compareTo("08-06")<=0 ){//7 days
			return 3 + 8 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("07-21")>=0 & visit_date.compareTo("07-30")<=0 ){//10 days
			return 4 + 8 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("07-06")>=0 & visit_date.compareTo("07-20")<=0 ){//15 days
			return 5 + 8 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("06-11")>=0 & visit_date.compareTo("07-05")<=0 ){//25 days
			return 6 + 8 * Integer.parseInt(type);}
		else if ( visit_date.compareTo("05-16")>=0 & visit_date.compareTo("06-10")<=0 ){//25 days
			return 7 + 8 * Integer.parseInt(type);}
		
		else return -1;//beyond the period
		
	}	
	
	
	
	
	
	
	
	
	

}
