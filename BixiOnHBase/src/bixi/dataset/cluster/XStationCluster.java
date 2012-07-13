package bixi.dataset.cluster;

import java.util.LinkedList;
import java.util.List;

import bixi.dataset.collection.XStation;

//deprecated
// till now, there is no reference on this
public class XStationCluster {

	private String cluster_id = null;
	private List<String> ids = null;
	private List<XStation> sub_stations = null;
	
	public XStationCluster(){
		ids = new LinkedList<String>();
		sub_stations = new LinkedList<XStation>();
	}
	
	
	
	
}
