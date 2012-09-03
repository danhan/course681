package bixi.hbase.upload;

import java.io.IOException;

import util.quadtree.based.trie.XQuadTree;
import bixi.hbase.query.BixiConstant;

/**
 * the subspace is 4
 * @author dan
 *
 */
public class TableInsertLocationS16 extends TableInsertLocationS1{
	
	public TableInsertLocationS16() throws IOException {		
		
		tableName = BixiConstant.LOCATION_TABLE_NAME_1+"6";
		this.min_size_of_subspace = 4;
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
