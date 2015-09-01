package helper;
import java.text.SimpleDateFormat;
import java.util.Date;

//generate actions table
public class TbActions {
	public static boolean[] BuyBefore(Long[] buy){
		boolean[] buy_before = new boolean[buy.length];
		boolean b = false;
		for (int i=0; i<buy.length; i++){
			buy_before[i] = b;
			if(buy[i] > 0) b = true;
		}
		return buy_before;
	}
	
	public static Long[] ClicksAfterLastBuy(Long[] clicks, Long[] buy){
		Long[] clicks_after_last_buy = new Long[buy.length];
		Long count = 0L;
		for (int i = 0; i < buy.length; i ++){
			count += clicks[i];
			clicks_after_last_buy[i] = count;
			if(buy[i]>0){
				count = 0L;
			}
		}
		return clicks_after_last_buy;
	}
	
	public static Long[] DaysSinceKnow(String[] visit_datetime){
		SimpleDateFormat formatter = new SimpleDateFormat("MM-dd");
		Date[] dates = new Date[visit_datetime.length];
		Long[] days_since_know = new Long[visit_datetime.length];
		
		for(int i = 0; i < visit_datetime.length; i++){
			try {
				dates[i] = formatter.parse(visit_datetime[i]);
			} catch(Exception e) {
				
			}
		}
		
		for(int i=0; i<visit_datetime.length;i++){
			days_since_know[i] = (dates[i].getTime() - dates[0].getTime())/(1000*60*60*24);
		}
		
		return days_since_know;
	}
	
	public static Long[] DaysFocusAfterLastBuy(String[] visit_datetime, Long[] buy){
		Long[] days_focus_after_last_buy = new Long[buy.length];
		SimpleDateFormat formatter = new SimpleDateFormat("MM-dd");
		Date[] dates = new Date[visit_datetime.length];
		for(int i = 0; i < visit_datetime.length; i++){
			try {
				dates[i] = formatter.parse(visit_datetime[i]);
			} catch(Exception e) {
				
			}
		}
		
		Date last_buy_date = dates[0];
		for (int i = 0; i < buy.length; i++){
			days_focus_after_last_buy[i] = (dates[i].getTime() - last_buy_date.getTime())/(1000*60*60*24);
			if(buy[i]>0){
				last_buy_date = dates[i];
			}
		}
		return days_focus_after_last_buy;
	}
}
