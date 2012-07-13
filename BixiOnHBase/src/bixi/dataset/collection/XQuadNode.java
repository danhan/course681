package bixi.dataset.collection;


public class XQuadNode {

	private XLocation left_top = null;
	private double dx = 0;  // distance of x
	private double dy = 0; // distance of y
	private int level = 0;   
	private String id = null;
	
    private XQuadNode parent = null;
    private boolean hasChild = true;
    
    private XQuadNode first = null;
    private XQuadNode second = null;
    private XQuadNode third = null;
    private XQuadNode fourth = null;

   
    
    public XQuadNode( int maxLevel, int level, XQuadNode parent, XLocation left_top,double dx,double dy){   	
        this.parent = parent;
        this.left_top = left_top;
        this.dx = dx;
        this.dy = dy;
        this.level = level;
        if (this.level < maxLevel) {
        	double x_left = left_top.getLatitude();
        	double y_left = left_top.getLongitude();
        	double sub_dx = this.dx / 2.0;
        	double sub_dy = this.dy / 2.0;
            XLocation right_top = new XLocation(x_left+sub_dx, y_left);
            XLocation left_bottom = new XLocation(x_left,y_left-sub_dy);
            XLocation right_bottom = new XLocation(x_left+sub_dx,y_left-sub_dy);            
            
            this.first = new XQuadNode(maxLevel, level + 1, this, left_top,sub_dx,sub_dy);
            this.second = new XQuadNode(maxLevel, level + 1,this, right_top,sub_dx,sub_dy);
            this.third = new XQuadNode(maxLevel, level + 1, this, left_bottom,sub_dx,sub_dy);
            this.fourth = new XQuadNode(maxLevel, level + 1, this, right_bottom,sub_dx,sub_dy);
            this.hasChild = true;
        }
        else {
            this.first = null;
            this.second = null;
            this.third = null;
            this.fourth = null;
            this.hasChild = false;
            this.id = this.left_top.getLatitude()+":"+this.left_top.getLongitude()+":"+this.dx+":"+this.dy;
        }    	

    }
     
   /*
    * x increase from left to right
    * y increase from bottom to top 
    */
    public boolean isInside(double x, double y) {
    	double x1 = this.left_top.getLatitude();
    	double x2 = this.left_top.getLatitude()+this.dx;
    	double y1 = this.left_top.getLongitude();
    	double y2 = this.left_top.getLongitude()-this.dy;
        if ((x < x1) || (x > x2)) {        	
            return false;
        }
        if ((y < y2) || (y > y1)) {        	
        	return false;
        }        
        return true;
    }  
    
    public XQuadNode locate(double x, double y) {
        if (this.hasChild) {
            // check children
            if (this.first.isInside(x,y)) {            	
                return this.first.locate(x,y);
            }
            if (this.second.isInside(x,y)) {            	
                return this.second.locate(x, y);
            }
            if (this.third.isInside(x,y)) {            	
            	return this.third.locate(x,y);
            }
            if (this.fourth.isInside(x,y)) {            	
                return this.fourth.locate(x,y);
            }
            return null;
        }
        else {
            return this;
        }
    }  

    public XQuadNode locate_debug(double x, double y) {
        boolean isThere = false;
        XQuadNode node = this;

    	if (this.hasChild) {
            if (this.first.isInside(x,y)) {
            	if(x== 45.514565 && y == -73.690909) System.out.println("first");
                return this.first.locate(x,y);
            }
            if (this.second.isInside(x,y)) {
            	if(x== 45.514565 && y == -73.690909) System.out.println("second");
                return this.second.locate(x, y);
            }
            if (this.third.isInside(x,y)) {
            	if(x== 45.514565 && y == -73.690909) System.out.println("third");
            	return this.third.locate(x,y);
            }
            if (this.fourth.isInside(x,y)) {
            	if(x== 45.514565 && y == -73.690909) System.out.println("fourth");
                return this.fourth.locate(x,y);
            }
            return null;
        }
        else {
            return this;
        }
    }    
    
    public int size() {
        if (!this.hasChild) {
            return 1;
        }        
        return 1 + this.first.size()+ this.second.size() + this.third.size() + this.fourth.size();
    }
    
    public void print(){
    	if(!this.hasChild){
    		 System.out.println("level:"+this.level+" (" + this.getLeft_top().getLatitude() + ", " + this.getLeft_top().getLongitude()
    				 + ",("+ this.getDx()+","+this.getDy()+")");
    	}else{
    		this.first.print();
    		this.second.print();
    		this.third.print();
    		this.fourth.print();
    	}
    }
    
    
    
    public XQuadNode getLeaf(double x, double y) {
        if (!this.hasChild) {
            return this;
        }

        if (this.first.isInside(x, y)) {
            return this.first.getLeaf(x, y);
        }
        if (this.second.isInside(x, y)) {
            return this.second.getLeaf(x, y);
        }
        if (this.third.isInside(x, y)) {
            return this.third.getLeaf(x, y);
        }
        if (this.fourth.isInside(x, y)) {
            return this.fourth.getLeaf(x, y);
        }
        return null;
    }    
 
	public int getLevel() {
		return level;
	}


	public void setLevel(int level) {
		this.level = level;
	}


	public boolean isHasChild() {
		return hasChild;
	}


	public void setHasChild(boolean hasChild) {
		this.hasChild = hasChild;
	}


	public XLocation getLeft_top() {
		return left_top;
	}

	public void setLeft_top(XLocation left_top) {
		this.left_top = left_top;
	}


	public double getDx() {
		return dx;
	}

	public void setDx(double dx) {
		this.dx = dx;
	}

	public double getDy() {
		return dy;
	}

	public void setDy(double dy) {
		this.dy = dy;
	}

	public XQuadNode getParent() {
		return parent;
	}

	public void setParent(XQuadNode parent) {
		this.parent = parent;
	}

	public XQuadNode getFirst() {			
		return first;
	}

	public void setFirst(XQuadNode first) {
		this.first = first;
	}

	public XQuadNode getSecond() {	
		return second;
	}

	public void setSecond(XQuadNode second) {
		this.second = second;
	}

	public XQuadNode getThird() {		
		return third;
	}

	public void setThird(XQuadNode third) {
		this.third = third;
	}

	public XQuadNode getFourth() {			
		return fourth;
	}

	public void setFourth(XQuadNode fourth) {
		this.fourth = fourth;
	}




	public String getId() {
		return id;
	}




	public void setId(String id) {
		this.id = id;
	}
    
    
    
}
