import java.io.IOException;
import java.util.Iterator;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.TaskContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;





public class scoreReducer extends ReducerBase {

	Record output;
	
	@Override
	public void setup(TaskContext context) throws IOException {
		output = context.createOutputRecord();
	
	}

	@Override
	public void reduce(Record key, Iterator<Record> values, TaskContext context)
			throws IOException {
		double score = 0;
		double weightClick = 0, weightBuy = 0;
		double weightCollect = 0, weightBasket = 0;
		double fre_buy = 0 ,fre_click = 0 ;//actions = 0;
		// 设置各个行为的权重
		fre_click = key.getDouble("fre_click");
	    fre_buy = key.getDouble("fre_buy");
	  //  actions = key.getBigint("actions");
	    
	    if (fre_click == 0 )
			fre_click = 1;
		weightBuy = 1/fre_click;
		if(  fre_buy == 0 )
			fre_buy= 1;
		
		weightClick = 3*1/fre_buy;
		weightCollect = (2*weightBuy + weightCollect)/3 ;
		weightBasket = weightBuy ;
		
		user_brand_preValue tmp ;
		List<user_brand_preValue> iUserjBrandPreSet = new ArrayList<user_brand_preValue>();
		// 计算当前用户对某品牌的喜好分数
		while (values.hasNext()) {
			// TODO process value
			Record val = values.next();
		    
			score =   weightClick*val.getBigint("n_click") 
					+ weightBuy*val.getBigint("n_buy")
					+ weightCollect*val.getBigint("n_collect")
					+ weightBasket *val.getBigint("n_basket");
		
			output.set(0,key.getString("user_id") );
			output.set(1,val.getString("brand_id"));
			output.set(2,score);
			tmp = new user_brand_preValue(output.getString(0),output.getString(1),output.getDouble(2));			
			iUserjBrandPreSet.add(tmp );		
		}
		// sort 
		//descending order by preValue
        Comparators comp = new Comparators();
        Collections.sort(iUserjBrandPreSet, comp);
        
		 // how much result do you wanna output
        float times = (float) 4.5;
        long count_buy_brand_kinds = key.getBigint("count_buy_brand_kinds");
        
        int num = Math.round( (float)( count_buy_brand_kinds  ) / 10 * times) ;
        if ( num > iUserjBrandPreSet.size() )
             num = iUserjBrandPreSet.size();
        
		for (int i = 0; i <num;i++ ){
			
			 tmp =  iUserjBrandPreSet.get(i);
			 output.set(0, tmp.getUser_id());
			 output.set(1,tmp.getBrand_id());
			 output.set(2,tmp.getPreValue());
			 
			 context.write(output);		
		}

	}

}
