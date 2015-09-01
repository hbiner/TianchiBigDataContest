package helper;

public class Cumulative {
	public static Long[] Sum(Long[] in){
		Long[] out = new Long[in.length];
		out[0] = in[0];
		
		for(int i=1;i<in.length;i++){
			out[i]=out[i-1]+in[i];
		}
		return out;
	}
	
	public static Long[] Max(Long[] in){
		Long[] out = new Long[in.length];
		out[0] = in[0];
		Long max = in[0];
		for(int i = 1; i < in.length; i++){
			max = Math.max(max, in[i]);
			out[i] = max;
		}
		return out;
	}
	
	public static Long[] Min(Long[] in){
		Long[] out = new Long[in.length];
		out[0] = in[0];
		Long min = in[0];
		for(int i = 1; i < in.length; i++){
			min = Math.min(min, in[i]);
			out[i] = min;
		}
		return out;
	}
}
