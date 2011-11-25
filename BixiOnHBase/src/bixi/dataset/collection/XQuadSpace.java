package bixi.dataset.collection;


public class XQuadSpace {
		
    private XQuadNode root = null;
    private XLocation left_top = null;
    private double dx = 0;
    private double dy = 0;
    private int maxLevels = 0;

    public XQuadSpace(XLocation left_top,double dx, double dy,int maxLevels) {        
        //int maxLevel, int level, XQuadNode parent, XLocation left_top,double distance
    	this.left_top = new XLocation(left_top.getLatitude(),left_top.getLongitude());
    	this.dx = dx;
    	this.dy = dy;
    	this.maxLevels = maxLevels;
        this.root = new XQuadNode(maxLevels, 0, null,left_top,dx,dy);        
    }
    
    public XQuadNode locate(XLocation point){    	
    	return this.root.locate(point.getLatitude(),point.getLongitude());
    }    
    
    public void print_tree() {
        System.out.println("REGULAR QUADTREE");
        System.out.println(this.root.size() + " nodes in quad tree");        
        
        this.root.print();
    }
    
    public void print_space(){
    	System.out.println("space: ( " + left_top.getLatitude() + ","+ left_top.getLongitude()+") "
    					+"x distance: "+dx +", y distance: "+dy + "; maxLevel: "+maxLevels);
    }
    
    
    public static void main(String[] args){
    	XLocation location = new XLocation(45,-73);
    	XQuadSpace space = new XQuadSpace(location,3,2,3);
    	
    	
    	
    	//space.print_tree();
    	
    	XLocation current = new XLocation(46.3,-73);    	
    	XQuadNode node = space.locate(current);
    	if(node != null){
    		System.out.println(node.getId());	
    	}else{
    		System.out.println("out of the scope");
    	}
    }
    
	
}
