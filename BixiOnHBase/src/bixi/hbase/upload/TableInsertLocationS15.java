package bixi.hbase.upload;

import java.io.IOException;

import util.quadtree.based.trie.XQuadTree;
import bixi.hbase.query.BixiConstant;

/**
 * the subspace is 5
 * @author dan
 *
 */
public class TableInsertLocationS15 extends TableInsertLocationS1{
	
	public TableInsertLocationS15() throws IOException {		
		
		tableName = BixiConstant.LOCATION_TABLE_NAME_1+"5";
		this.min_size_of_subspace = BixiConstant.MIN_SIZE_OF_SUBSPACE5;
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
