package util.raster;

public class XBox {
	
	String row = "";
	String column = "'";
	int objectCount = 0;
	
	public XBox(String r,String c){
		this.row = r;
		this.column = c;		
		
	}
/**
 * Record the number of objects in order to insert the object to version dimension
 */
	public void addObject(){
		this.objectCount++;
	}

	public String toString(){
		String msg = "("+this.row+","+
					this.column+")=>"+this.objectCount;
		return msg;
	}
	
	
	public String getRow() {
		return row;
	}
	public String getColumn() {
		return column;
	}
	public int getObjectCount() {
		return objectCount;
	}	
}
