package util.quadtree.based.trie;


import java.awt.geom.Rectangle2D;


public class TestXQuadTree {

    public static void main(String args[]){
    	
    	Rectangle2D.Float rect = new Rectangle2D.Float((float)45.415714,
    							(float)73.526967,
    							(float)0.15011499999999955,
    							(float)0.1639420000000058);
    	XQuadTree tree = new XQuadTree(rect,(float)0.002);
    	tree.buildTree();
    	tree.print();
    	
    	// get one point's index
    	XQuadTree subspace = tree.locate((float)45.52830025,(float)73.608938);
    	System.out.println("============");
    	subspace.print();
    	
    	String[] spaces=tree.match(new Rectangle2D.Float(5,5,1,1));
    	for(String s:spaces){
    		System.out.println(s);
    	}
    }
}
