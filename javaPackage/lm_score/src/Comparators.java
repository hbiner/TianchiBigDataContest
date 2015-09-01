// Comparators
import java.util.Comparator;

public  class Comparators implements Comparator<user_brand_preValue>{
// descending order by preValue	
@Override
public int compare(user_brand_preValue o1, user_brand_preValue o2) {
	  if (o1.getPreValue() < o2.getPreValue())
		  return 1;
	     else if ( o1.getPreValue() > o2.getPreValue() )
		  return -1;
	  else
		 return 0;
}     
}