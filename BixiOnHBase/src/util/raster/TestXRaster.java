package util.raster;

import java.awt.geom.Rectangle2D;

import bixi.hbase.query.BixiConstant;


public class TestXRaster {

	
	static double  min_size_of_height = BixiConstant.MIN_SIZE_OF_SUBSPACE;
	static int  max_num_of_column = BixiConstant.MAX_NUM_OF_COLUMN;
	
    public static void main(String args[]){
    	
    	Rectangle2D.Double space = new Rectangle2D.Double(
    			BixiConstant.MONTREAL_TOP_LEFT_X,
    			BixiConstant.MONTREAL_TOP_LEFT_Y,
    			1,
    			1);
    	
    	XRaster raster = new XRaster(space,	0.2,10); 
    	
    	raster.print();
    	
    	
    	// get one point's index
    	XBox box = raster.locate(0.1342332,0.2333);
    	System.out.println("============");
    	System.out.println(box.toString());
    	
    	XBox[] boxes = raster.match(0.10,0.20,0.0023);
    	System.out.println("=============match...");
    	System.out.println(boxes[0].toString());
    	System.out.println(boxes[1].toString());
    	
    	// test station 97
    	
        box = raster.locate(45.528448693514136,73.55108499526978);
    	System.out.println("============");
    	System.out.println(box.toString()); // (024,07)=>0
    	
    	
    }
}
