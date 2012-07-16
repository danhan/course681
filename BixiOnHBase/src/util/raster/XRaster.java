package util.raster;

import java.awt.geom.Rectangle2D;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import util.quadtree.based.trie.XQuadTree;
/**
 * This is a raster of the space, the stripe of row is the minimum size of subspace, 
 * The number of columns is defined as a constant number(see 100), 
 * and the version dimension will store the multiple objects which are located into the same box(row and column)
 * @author dan
 *
 */
public class XRaster {

    // min width of subspace;
    private double min_size_of_height = 1;
    /**
     * The number of columns defined by users, this can be caculated with the space scope and density of data.
     */
    private double min_size_of_width = 1;
        
    
    private Rectangle2D.Double m_rect;            // The area this QuadTree represents
    private ArrayList<XBox> m_boxes = null;
    private DecimalFormat IndexKeyFormatter = null;
    private DecimalFormat IndexColumnFormatter = null;
    
    public XRaster(Rectangle2D.Double rect, double min_size_of_height,int columnNum){
    	this.m_rect = rect;
    	this.min_size_of_height = min_size_of_height;
    	this.min_size_of_width = m_rect.getWidth() / columnNum;    	
    	this.m_boxes = new ArrayList<XBox>();
    	
    	int num_of_row = (int) (this.m_rect.getHeight() / this.min_size_of_height); 	
    	
    	IndexKeyFormatter = this.getKeyFormatter(num_of_row);   
    	IndexColumnFormatter = this.getKeyFormatter(columnNum);
    }
    
    /**
     * Determine the subspace which the point belongs to.
     * It is used to get the point's index
     * @param x
     * @param y
     * @return Box, it indicates the row and column this point belongs to and the number of objects the box has already
     */  
    public XBox locate(double x, double y) {
    	int row = (int) ((y-this.m_rect.getY()) / this.min_size_of_height );   	
    	int column = (int)((x - this.m_rect.getX()) / this.min_size_of_width );  		
    		
    	String f_row = this.IndexKeyFormatter.format(row);
    	String f_column = this.IndexColumnFormatter.format(column);
    	for(XBox box:m_boxes){
    		if(box.getRow().equals(f_row) && box.getColumn().equals(f_column)){    			
    			return box;
    		}
    	} 
    	
    	XBox box = new XBox(this.IndexKeyFormatter.format(row),this.IndexColumnFormatter.format(column));
    	return box;
    }
    
    public XBox addPoint(double x, double y){
    	XBox box = this.locate(x, y);
    	box.addObject();
    	this.m_boxes.add(box);
    	return box;
    }
    
    public XBox[] match(double x, double y,double radius){
   	
    	double minX = (m_rect.getMinX()<(x-radius))? (x-radius):m_rect.getMinX(); 
    	double minY = (m_rect.getMinY()<(y-radius))? (y-radius):m_rect.getMinY(); 
    	double maxX = (m_rect.getMaxX()>(x+radius))? (x+radius):m_rect.getMaxX();
    	double maxY = (m_rect.getMaxY()>(y+radius))? (y+radius):m_rect.getMaxY();
    	System.out.println("bounder: ("+minX+","+minY+")("+maxX+","+maxY+")");
    	
    	XBox tl = this.locate(minX, minY);    	
    	XBox br = this.locate(maxX, maxY);    	
    	return new XBox[]{tl,br};    	   	
    }
    
    public String[] getColumns(XBox top_left, XBox bottom_right){
    	List<String> columns = new ArrayList<String>();
    	System.out.println(top_left.toString());
    	System.out.println(bottom_right.toString());
    	for(int i=Integer.valueOf(top_left.getColumn()); i<= Integer.valueOf(bottom_right.getColumn()); i++){
    		String c = this.IndexColumnFormatter.format(i);
    		System.out.println("column: "+c);
    		columns.add(c);
    	}
		String[] c = new String[columns.size()];

		c = columns.toArray(c);
		return c;
    }
    
	private DecimalFormat getKeyFormatter(int num_of_key){
		DecimalFormat xIndexFormatter = null;
		if(num_of_key<10){
			xIndexFormatter = new DecimalFormat("0");
		}else if(num_of_key<100){
			xIndexFormatter = new DecimalFormat("00");
		}else if(num_of_key<1000){
			xIndexFormatter = new DecimalFormat("000");
		}else if(num_of_key<10000){
			xIndexFormatter = new DecimalFormat("0000");
		}else if(num_of_key<100000){
			xIndexFormatter = new DecimalFormat("00000");
		}else if(num_of_key<1000000){
			xIndexFormatter = new DecimalFormat("000000");
		}else if(num_of_key<10000000){
			xIndexFormatter = new DecimalFormat("0000000");
		}else{
			xIndexFormatter = new DecimalFormat("00000000");
		}
		return xIndexFormatter;
	}  
    
	public void addBox(XBox box){
		this.m_boxes.add(box);
	}
	
	public void print(){
		String msg = "";
		int num_of_row = (int) (this.m_rect.getHeight() / this.min_size_of_height); 
		int num_of_column = (int) (this.m_rect.getWidth() / this.min_size_of_width);
    	double br_x = this.m_rect.getX()+(num_of_column-1)*this.min_size_of_width;
    	double br_y = this.m_rect.getY()+(num_of_row-1)*this.min_size_of_height;    
    	msg = "row=>"+num_of_row+
    				  ";column=>" + num_of_column+
    				  ";br_x=>"+br_x+
    				  ";br_y=>"+br_y;
    	for(XBox box:m_boxes){
    		msg += box.toString()+"\n";
    	}
    	System.out.println(msg);				
	}
}
