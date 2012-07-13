package util.quadtree.based.point;

import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.util.LinkedList;
import java.util.List;

/*
 * It provides 
 * 1) insert the node and build up a quad tree
 * 2) Get the objects based on a rectangle
 * 
 */
public class XQuadTree  {

	
    // how many obejcts in a node;
    private int max_objets_per_node = 10;


    private List<XNode> m_objects = null;  // The objects in this QuadTree
    private Rectangle2D.Float m_rect;            // The area this QuadTree represents
    private String index = "";    

    private XQuadTree m_tl_child = null;   // Top Left Child
    private XQuadTree m_tr_child = null;   // Top Right Child
    private XQuadTree m_bl_child = null;   // Bottom Left Child
    private XQuadTree m_br_child = null;   // Bottom Right Child
	

    //region Constructor
    
    /// Creates a QuadTree for the specified area.
    /// </summary>
    /// <param name="rect">The area this QuadTree object will encompass.</param>
    public XQuadTree(Rectangle2D.Float rect,int num_per_nodes)
    {
        m_rect = rect;
        this.max_objets_per_node = num_per_nodes;
    }
 
    /// Creates a QuadTree for the specified area.
    /// </summary>
    /// <param name="x">The top-left position of the area rectangle.</param>
    /// <param name="y">The top-right position of the area reactangle.</param>
    /// <param name="width">The width of the area rectangle.</param>
    /// <param name="height">The height of the area rectangle.</param>
    public XQuadTree(float x, float y, float width, float height,int num_per_nodes)
    {
        m_rect = new Rectangle2D.Float(x, y, width, height);
        this.max_objets_per_node = num_per_nodes;
    }    
    
    
    
    /// <summary>
    /// Insert an item into this QuadTree object.
    /// </summary>
    /// <param name="item">The item to insert.</param>
    public void insert(XNode item)
    {
        // If this quad doesn't intersect the items rectangle, do nothing
        if (!m_rect.intersects((item.getRect()))
        		&& !m_rect.contains(item.getLocation())){        	
        	return;
        }
            
        if (m_objects == null || 
            (this.m_tl_child == null && m_objects.size() +1 <= this.max_objets_per_node))
        {
            // If there's room to add the object, just add it
            this.addObject(item);
            System.out.println("insert.... object are "+item.getRect().getX()+","+item.getRect().getY()+";now this subspace has "+this.m_objects.size()+" objects");
        }
        else
        {
            // No quads, create them and bump objects down where appropriate
            if (this.m_tl_child == null)
            {
                subDivide();
            }

            // Find out which tree this object should go in and add it there
            XQuadTree destTree = getDestinationTree(this,item);
            if (destTree == this)
            {
                addObject(item);
                System.out.println("!!!!!!!!!this.index"+this.index);
            }
            else
            {
                destTree.insert(item);
                System.out.println("!!!!!!!!!!! Expect: (4,4) <> ("+item.getRect().getX()+","+item.getRect().getY()+")");
            }
        }
    }
        
    
    /// <summary>
    /// Subdivide this QuadTree and move it's children into the appropriate Quads where applicable.
    /// </summary>
    private void subDivide()
    {
    	System.out.println("in subdivide......");
        // We've reached capacity, subdivide...        
        float bi_width = m_rect.width / 2;
        float bi_height = m_rect.height / 2;
        
        Point2D.Float mid = new Point2D.Float(m_rect.x + bi_width, m_rect.y + bi_height);

        m_tl_child = new XQuadTree(new Rectangle2D.Float(m_rect.x, m_rect.y, bi_width, bi_height),this.max_objets_per_node);
        m_tr_child = new XQuadTree(new Rectangle2D.Float(mid.x, m_rect.y,bi_width, bi_height),this.max_objets_per_node);
        m_bl_child = new XQuadTree(new Rectangle2D.Float(m_rect.x, mid.y, bi_width, bi_height),this.max_objets_per_node);
        m_br_child = new XQuadTree(new Rectangle2D.Float(mid.x, mid.y,bi_width, bi_height),this.max_objets_per_node);
        
        m_tl_child.index = this.index+"00";
        m_tr_child.index = this.index+"01";
        m_bl_child.index = this.index+"10";
        m_br_child.index = this.index+"11";

        // If they're completely contained by the quad, bump objects down
        for (int i = 0; i < m_objects.size(); i++)
        {
            XQuadTree destTree = getDestinationTree(this,(XNode)m_objects.get(i));
            System.out.println("========"+destTree.index+";("+destTree.m_rect.getX()
            			+","+destTree.m_rect.getY()+","+destTree.m_rect.getWidth()+","+destTree.m_rect.getHeight()+")");
            if (destTree != this)
            {
                // Insert to the appropriate tree, remove the object, and back up one in the loop
                destTree.insert((XNode)m_objects.get(i));
                this.removeObject((XNode)m_objects.get(i));
                i--;
            }
        }
    }

    /// <summary>
    /// Get the child Quad that would contain an object.
    /// </summary>
    /// <param name="item">The object to get a child for.</param>
    /// <returns></returns>
    private XQuadTree getDestinationTree(XQuadTree destTree,XNode item)
    {
        // If a child can't contain an object, it will live in this Quad
        //XQuadTree destTree = this;
        if (destTree.m_tl_child == null){
        	System.out.println(destTree.Count()+"====");
        	return null;
        }
        	
        
        if (destTree.m_tl_child.QuadRect().contains(item.getRect()) 
        		|| destTree.m_tl_child.QuadRect().contains(item.getLocation()))
        {
            destTree = destTree.m_tl_child;
        }
        else if (destTree.m_tr_child.QuadRect().contains(item.getRect())
        		|| destTree.m_tr_child.QuadRect().contains(item.getLocation()))
        {
            destTree = destTree.m_tr_child;
        }
        else if (destTree.m_bl_child.QuadRect().contains(item.getRect())
        		|| destTree.m_bl_child.QuadRect().contains(item.getLocation()))
        {
            destTree = destTree.m_bl_child;
        }
        else if (destTree.m_br_child.QuadRect().contains(item.getRect())
        		|| destTree.m_br_child.QuadRect().contains(item.getLocation()))
        {
            destTree = destTree.m_br_child;
        }
        /*else{
        	if(destTree != this)
        		destTree = null;
        	System.out.println("===this");
        }*/

        return destTree;
    }

    
    
    /// The area this QuadTree represents.
    public Rectangle2D.Float QuadRect(){
    	return m_rect;
    }

   
    /// The top left child for this QuadTree
    /// </summary>
    public XQuadTree TopLeftChild (){
    	return this.m_tl_child;
    }

   
    /// The top right child for this QuadTree
    /// </summary>
    public XQuadTree TopRightChild(){
    	return this.m_tr_child;
    }

   
    /// The bottom left child for this QuadTree
    /// </summary>
    public XQuadTree BottomLeftChild(){
    	return this.m_bl_child;
    }

   
    /// The bottom right child for this QuadTree
    /// </summary>
    public XQuadTree BottomRightChild(){
    	return this.m_br_child;
    }

   
    /// The nodes contained in this QuadTree at it's level (ie, excludes children)
    /// </summary>
    public List nodes(){
    	return m_objects;
    }

   
    /// How many total objects are contained within this QuadTree (ie, includes children)
    /// </summary>
    public int Count(){
    	return this.nodeCount();
    }
    
    /// <summary>
    /// Get the total for all objects in this QuadTree, including children.
    /// </summary>
    /// <returns>The number of objects contained within this QuadTree and its children.</returns>
    private int nodeCount()
    {
        int count = 0;

        // Add the objects at this level
        if (m_objects != null) count += m_objects.size();

        // Add the objects that are contained in the children
        if (m_tl_child != null)
        {
            count += m_tl_child.nodeCount();
            count += m_tr_child.nodeCount();
            count += m_bl_child.nodeCount();
            count += m_br_child.nodeCount();
        }

        return count;
    }    
   


    //region Private Members
    /// <summary>
    /// Add an item to the object list.
    /// </summary>
    /// <param name="item">The item to add.</param>
    private void addObject(XNode item)
    {
        if (this.m_objects == null)
            this.m_objects = new LinkedList<XNode>();        
        item.setIndex(this.index);
        this.m_objects.add(item);
        
    }

    /// <summary>
    /// Remove an item from the object list.
    /// </summary>
    /// <param name="item">The object to remove.</param>
    private void removeObject(XNode item)
    {
        if (m_objects != null && m_objects.contains(item))
            m_objects.remove(item);
    }    
    

    
    
    //endregion

    //region Public Methods
    /// <summary>
    /// Clears the QuadTree of all objects, including any objects living in its children.
    /// </summary>
    public void Clear()
    {
        // Clear out the children, if we have any
        if (this.m_tl_child != null)
        {
            m_tl_child.Clear();
            m_tr_child.Clear();
            m_bl_child.Clear();
            m_br_child.Clear();
        }

        // Clear any objects at this level
        if (m_objects != null)
        {
            m_objects.clear();
            m_objects = null;
        }

        // Set the children to null
        m_tl_child = null;
        m_tr_child = null;
        m_bl_child = null;
        m_br_child = null;
    }
    

    public String match(XNode item){
        // If this quad doesn't intersect the items rectangle, do nothing
        if (!m_rect.intersects((item.getRect()))
        		&& !m_rect.contains(item.getLocation())){   
        	System.out.println("========");
        	return null;
        }
        XQuadTree destTree = null;
        String index = "0"; // the root
        String parent = index;
        destTree = getDestinationTree(this,item);
        while(destTree != null){
        	parent = index;
        	destTree = getDestinationTree(destTree,item);
        }

        System.out.println("========"+parent);
        return parent;
    }
    
    
    
    public void printTree(){
    	if(this.m_objects != null){
        	for(int i=0;i<this.m_objects.size();i++){
        		System.out.print(this.m_objects.get(i).getIndex()+": "+this.m_objects.get(i).getRect().x+
        					";"+this.m_objects.get(i).getRect().y+"||\t");
        	}
        	System.out.println();   		
    	}

        if (m_tl_child != null)
        {
        	m_tl_child.printTree();
            m_tr_child.printTree();
            m_bl_child.printTree();
            m_br_child.printTree();
        }   	
    	
    }
    
    
    
    
    /// <summary>
    /// Get the objects in this tree that intersect with the specified rectangle.
    /// </summary>
    /// <param name="rect">The rectangle to find objects in.</param>
    /// <param name="results">A reference to a list that will be populated with the results.</param>
    public void GetObjects(Rectangle2D.Float rect, List<XNode> results)
    {
        // We can't do anything if the results list doesn't exist
        if (results != null)
        {
            if (rect.contains(m_rect))
            {
                // If the search area completely contains this quad, just get every object this quad and all it's children have
            	getAllObjects(results);
            }
            else if (rect.intersects(m_rect))
            {
                // Otherwise, if the quad isn't fully contained, only add objects that intersect with the search rectangle
                if (m_objects != null)
                {
                    for (int i = 0; i < m_objects.size(); i++)
                    {
                        if (rect.intersects(((XNode)m_objects.get(i)).getRect()))
                        {
                            results.add(((XNode)m_objects.get(i)));
                        }
                    }
                }

                // Get the objects for the search rectangle from the children
                if (m_tl_child != null)
                {
                	m_tl_child.GetObjects(rect,results);
                    m_tr_child.GetObjects(rect,results);
                    m_bl_child.GetObjects(rect,results);
                    m_br_child.GetObjects(rect,results);
                }
            }
        }
    }

    /// <summary>
    /// Get all objects in this Quad, and it's children.
    /// </summary>
    /// <param name="results">A reference to a list in which to store the objects.</param>
    public List<XNode> getAllObjects(List<XNode> results)
    {    	
        // If this Quad has objects, add them
        if (m_objects != null){        	        	
        	results.addAll(m_objects);        	
        }
                       
        // If we have children, get their objects too
        if (this.m_tl_child != null)
        {
        	m_tl_child.getAllObjects(results);
        	m_tr_child.getAllObjects(results);
        	m_bl_child.getAllObjects(results);
        	m_br_child.getAllObjects(results);
        }
        return results;
    }
   
}    
    