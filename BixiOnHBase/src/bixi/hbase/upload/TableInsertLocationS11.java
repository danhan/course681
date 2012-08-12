package bixi.hbase.upload;

import java.io.IOException;

import util.quadtree.based.trie.XQuadTree;
import bixi.hbase.query.BixiConstant;

/**
 * the subspace is 1
 * @author dan
 *
 */
public class TableInsertLocationS11 extends TableInsertLocationS1{
	
	public TableInsertLocationS11() throws IOException {		
		
		tableName = BixiConstant.LOCATION_TABLE_NAME_1+"1";
		this.min_size_of_subspace = 1;
		familyName = BixiConstant.LOCATION_FAMILY_NAME;
		try{
			this.setHBase();	
		}catch(Exception e){
			e.printStackTrace();
		}
				
		// build up a quadtree.
		quadTree = new XQuadTree(space, min_size_of_subspace);
		quadTree.buildTree();
	}
}
