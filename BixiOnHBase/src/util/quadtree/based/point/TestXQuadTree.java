package util.quadtree.based.point;


import java.awt.geom.Rectangle2D;
import java.util.ArrayList;

public class TestXQuadTree {

    public static void main(String args[]){
    	
    	XQuadTree tree = new XQuadTree(0,0,(float)10.1,(float)10.1,3);
    	for(int i=0;i<=10;i++){
    		XNode node1 = new XNode(i,i,0,0);
    		tree.insert(node1);
    	}   	
    	ArrayList<XNode> results = new ArrayList<XNode>();
    	tree.getAllObjects(results);
    	System.out.println(tree.Count());
    	for(XNode node:results){
    		System.out.println(node.toString());
    	}
    	
    	System.out.println("===========");
    	results.clear();
    	tree.GetObjects(new Rectangle2D.Float(0,0,5,5), results);
    	for(XNode node:results){
    		System.out.println(node.toString());
    	}
    	
    }
}
