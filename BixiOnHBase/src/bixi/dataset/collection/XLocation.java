package bixi.dataset.collection;

public class XLocation {
	private double latitude;
	private double longitude;
	
	public XLocation(){};
	
	public XLocation(double lat,double lon){
		this.latitude = lat;
		this.longitude = lon;
	}

	public double getLatitude() {
		return latitude;
	}

	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}

	public double getLongitude() {
		return longitude;
	}

	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}
	
	public void print_location(){
		System.out.println("("+this.latitude+","+this.longitude+")");
	}
	
	
}
