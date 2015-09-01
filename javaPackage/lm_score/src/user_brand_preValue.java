
// predict format
public class user_brand_preValue {
	 public	String user_id;
	 public	String brand_id;
	 public double preValue;	
 public String getUser_id() {
		return user_id;
	}
	public void setUser_id(String userId) {
		user_id = userId;
	}
	public String getBrand_id() {
		return brand_id;
	}
	public void setBrand_id(String brandId) {
		brand_id = brandId;
	}
	public double getPreValue() {
		return preValue;
	}
	public void setPreValue(double preValue) {
		this.preValue = preValue;
	}
	public void printAllMessage(){
		System.out.println(user_id+" "+brand_id+" "+preValue );
		
	}
	public user_brand_preValue(String userId, String brandId, double preValue) {
		super();
		user_id = userId;
		brand_id = brandId;
		this.preValue = preValue;
	}
	
}
