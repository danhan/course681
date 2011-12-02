package bixi.dataset.collection;

import java.util.Iterator;
import java.util.StringTokenizer;

/*
 * It is the data structure for one station
 */
//<station>
//<id>25</id>
//<name>de la Commune / Place Jacques-Cartier</name>
//<terminalName>6026</terminalName>
//<lat>45.50719279267449</lat>
//<long>-73.5520076751709</long>
//<installed>true</installed>
//<locked>false</locked>
//<installDate/>
//<removalDate/>
//<temporary>false</temporary>
//<nbBikes>4</nbBikes>
//<nbEmptyDocks>45</nbEmptyDocks>
//</station>


public class XStation {

	private String id;
	private String name;
	private String terminalName;
	private double latitude;
	private double longitude;
	private XLocation point;
	private boolean installed;
	private boolean locked;
	private long installDate;
	private long removeDate;
	private boolean temporary;
	private int nbBikes;
	private int nbEmptyDocks;
	private String clusterId;
	
	public XStation(){
		this.point = new XLocation();
	}
	
	public String getMetadata(){
		
		String result = "";
		result += "name="+this.name+";";
		result += "terminalName="+this.terminalName+";";
		result += "latitude="+this.latitude+";";
		result += "longitude="+this.longitude+";";
		result += "installed="+this.installed+";";
		result += "locked="+this.locked+";";
		result += "installDate="+this.installDate+";";
		result += "removeDate="+this.removeDate+";";
		result += "temporary="+this.temporary+";";
		
		return result;		
	}

	
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getTerminalName() {
		return terminalName;
	}
	public void setTerminalName(String terminalName) {
		this.terminalName = terminalName;
	}
	public double getLatitude() {
		return latitude;
	}
	public void setLatitude(double latitude) {
		this.latitude = latitude;
		this.point.setLatitude(latitude);
	}
	public double getlongitude() {
		return longitude;
	}
	public void setlongitude(double longitude) {
		this.longitude = longitude;
		this.point.setLongitude(this.longitude);
	}
	public boolean isInstalled() {
		return installed;
	}
	public void setInstalled(boolean installed) {
		this.installed = installed;
	}
	public boolean isLocked() {
		return locked;
	}
	public void setLocked(boolean locked) {
		this.locked = locked;
	}
	public long getInstallDate() {
		return installDate;
	}
	public void setInstallDate(long installDate) {
		this.installDate = installDate;
	}
	public long getRemoveDate() {
		return removeDate;
	}
	public void setRemoveDate(long removeDate) {
		this.removeDate = removeDate;
	}
	public boolean isTemporary() {
		return temporary;
	}
	public void setTemporary(boolean temporary) {
		this.temporary = temporary;
	}
	public int getNbBikes() {
		return nbBikes;
	}
	public void setNbBikes(int nbBikes) {
		this.nbBikes = nbBikes;
	}
	public int getNbEmptyDocks() {
		return nbEmptyDocks;
	}
	public void setNbEmptyDocks(int nbEmptyDocks) {
		this.nbEmptyDocks = nbEmptyDocks;
	}
	
	public XLocation getPoint() {
		return point;
	}
	public String getClusterId() {
		return clusterId;
	}
	public void setClusterId(String clusterId) {
		this.clusterId = clusterId;
	}
	public void print(){
		String output = "id" + "=>" + this.id +";"+
						"name" + "=>" + this.name +";"+
						"latitude" + "=>" + this.latitude +";"+ 
						"longitude" + "=>" + this.longitude +";"+ 
						"nbBikes" + "=>" + this.nbBikes + ";"+
						"nbEmptyDocks" + "=>" + this.nbEmptyDocks + ";"+ 
						"terminalName" + "=>" + this.terminalName;						
				
		System.out.println(output);
	}
	
	
	
	
}
