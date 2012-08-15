package util.quadtree.based.trie;


import java.awt.geom.Rectangle2D;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import bixi.hbase.query.BixiConstant;


public class TestXQuadTree {

    public static void main(String args[]){
    	
    	List<String> result = new ArrayList<String>();
    	result.add("110000");
    	result.add("00");
    	result.add("0010");
    	result.add("0011");
    	result.add("000011");
    	result.add("000010");
    	
    	System.out.println(result.toString());
    	Object[] objs = result.toArray();
    	Arrays.sort(objs);
    	System.out.println("======================");
    	System.out.println(Arrays.asList(objs).toString());
    	
/*    	Rectangle2D.Double rect = new Rectangle2D.Double(
   			 0,0,10,10);
   	
    	XQuadTree tree = new XQuadTree(rect,0.1);
    	tree.buildTree();
    	//tree.print();
    	
       	List<String> result = tree.match(1.15,0.1,1,0.01);
    	System.out.println(result.size()+"match========="+result.toString()); //0100,0110
*/    	
    	// get one point's index
//    	XQuadTree subspace = tree.locate(0.15,0.15);
//    	System.out.println("============");
//    	subspace.print();


    	
    	
/*    	Rectangle2D.Double rect = new Rectangle2D.Double(
    			 BixiConstant.MONTREAL_TOP_LEFT_X,
    			 BixiConstant.MONTREAL_TOP_LEFT_Y,
    			 BixiConstant.MONTREAL_AREA_WIDTH,
    			 BixiConstant.MONTREAL_AREA_HEIGHT);
    	
    	XQuadTree tree = new XQuadTree(rect,BixiConstant.MIN_SIZE_OF_SUBSPACE);
    	tree.buildTree();
    	tree.print();
    	
    	// get one point's index
    	XQuadTree subspace = tree.locate(45.52830025,73.608938);
    	System.out.println("============");
    	subspace.print();
    	
    	String[] spaces=tree.match(45.51038,73.55653,0.02,0.02);
    	System.out.println("match========="+spaces.length); //0100,0110
    	for(String s:spaces){
    		if(s!=null)
    			System.out.println(s);
    	}
    	
    	//Test for the query neighbor
       	subspace = tree.locate(45.49520,73.56328);
    	System.out.println("============");
    	subspace.print();*/
    }
}
