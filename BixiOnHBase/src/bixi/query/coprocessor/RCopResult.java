package bixi.query.coprocessor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class RCopResult implements Serializable{	
	
	private static final long serialVersionUID = 1L;
	
	long start = 0;
	long end = 0;
	List<String> res = null;
	int rows = 0; // the number of row scanned
	int cells = 0;
	String parameter = null;
	int kvLength = 0;
	
	public RCopResult(){
		this.res = new ArrayList<String>();		
	}

	public List<String> getRes() {
		return res;
	}

	public long getStart() {
		return start;
	}

	public void setStart(long start) {
		this.start = start;
	}

	public long getEnd() {
		return end;
	}

	public void setEnd(long end) {
		this.end = end;
	}

	public int getRows() {
		return rows;
	}

	public void setRows(int rows) {
		this.rows = rows;
	}

	public String getParameter() {
		return parameter;
	}

	public void setParameter(String parameter) {
		this.parameter = parameter;
	}

	public int getCells() {
		return cells;
	}

	public void setCells(int cells) {
		this.cells = cells;
	}

	public int getKvLength() {
		return kvLength;
	}

	public void setKvLength(int kvLength) {
		this.kvLength = kvLength;
	}

	
	

}
