import helper.Cumulative;
import helper.TbActions;
import java.io.IOException;
import java.util.Iterator;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.TaskContext;



import java.util.Map;
import java.util.TreeMap;

public class TbActionsReducer extends ReducerBase {
	Record output;
	@Override
	public void setup(TaskContext context) throws IOException {
		output = context.createOutputRecord();
	}

	@Override
	public void reduce(Record key, Iterator<Record> values, TaskContext context)
			throws IOException {
		Map<String,Long[]> typeCounter = new TreeMap<String,Long[]>();
		
		while (values.hasNext()) {
			Record val = values.next();
			String date = val.getString("visit_datetime");
			int type = Integer.parseInt(val.getString("type"));
	
			if(typeCounter.containsKey(date)){
				typeCounter.get(date)[type]++;
		 	} else {
				Long[] counter = new Long[]{0L,0L,0L,0L};
				counter[type] = 1L;
				typeCounter.put(date, counter);
			}
		}
		
		String[] visit_datetime = typeCounter.keySet().toArray(new String[typeCounter.size()]);
		Long[] clicks = new Long[typeCounter.size()];
		Long[] buy = new Long[typeCounter.size()];
		Long[] collect = new Long[typeCounter.size()];
		Long[] basket = new Long[typeCounter.size()];
		
		for (int i = 0; i < typeCounter.size(); i++){
			clicks[i] = typeCounter.get(visit_datetime[i])[0];
			buy[i] = typeCounter.get(visit_datetime[i])[1];
			collect[i] = typeCounter.get(visit_datetime[i])[2];
			basket[i] = typeCounter.get(visit_datetime[i])[3];
		}
		
		Long[] cum_clicks = Cumulative.Sum(clicks);
		boolean[] collect_before = TbActions.BuyBefore(collect);
		boolean[] buy_before = TbActions.BuyBefore(buy);
		Long[] clks_aft_last_buy = TbActions.ClicksAfterLastBuy(clicks, buy);
		Long[] days_focus_aft_last_buy = TbActions.DaysFocusAfterLastBuy(visit_datetime, buy);
		Long[] days_since_know = TbActions.DaysSinceKnow(visit_datetime);
		
		output.set(0, key.getString("user_id"));
		output.set(1, key.getString("brand_id"));
		
		for (int i = 0; i < typeCounter.size(); i++){
			output.set(2, visit_datetime[i]);
			output.set(3, clicks[i]);
			output.set(4, buy[i]);
			output.set(5, cum_clicks[i]);
			output.set(6, collect_before[i]);
			output.set(7, buy_before[i]);
			output.set(8, clks_aft_last_buy[i]);
			output.set(9, days_focus_aft_last_buy[i]);
			output.set(10, days_since_know[i]);
			context.write(output);
		}
	}

}
