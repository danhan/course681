package bixi.query.coprocessor;

import java.io.Serializable;

public class TotalNum implements Serializable {
	/**
	 * 
	 */	
	public long total;
	public int num;
	
	TotalNum(){
		num = 0;
		total = 0;
	}
	
	public void add(long t){
		total += t;
		num++;
	}
	
	public void merge(TotalNum tn){
		num += tn.num;
		total += tn.total;
	}
}
