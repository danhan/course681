package util.quadtree.based.point;

import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;

/*
 * This is the subspace which is got by splitting the space with QuadTree.
 */
public class XNode {
	
	private Rectangle2D.Float rect;
	private String index;
	private String value = null;

	public XNode(int x,int y,int w,int h){
		rect = new Rectangle2D.Float(x,y,w,h);		
	}
	
	public Rectangle2D.Float getRect() {
		return rect;
	}

	public String getIndex() {
		return index;
	}

	public void setIndex(String index) {
		this.index = index;
	}
	
	public Point2D.Float getLocation(){
		return new Point2D.Float((float)this.rect.getX(),(float)this.rect.getY());
	}
	
	
	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String toString(){
		String msg = "x=>"+this.rect.x+";"+
					";y=>"+this.rect.y+					
					";index=>"+this.index+
					";value=>"+this.value;
		return msg;
	}	
	
}
