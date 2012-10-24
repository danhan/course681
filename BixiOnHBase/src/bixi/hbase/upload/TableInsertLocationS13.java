package bixi.hbase.upload;

import java.io.IOException;

import util.quadtree.based.trie.XQuadTree;
import bixi.hbase.query.BixiConstant;

/**
 * the subspace is 10
 * @author dan
 *
 */
public class TableInsertLocationS13 extends TableInsertLocationS1{
	
	public TableInsertLocationS13() throws IOException {		
		
		tableName = BixiConstant.LOCATION_TABLE_NAME_1+"3";
		this.min_size_of_subspace = BixiConstant.MIN_SIZE_OF_SUBSPACE3;
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
