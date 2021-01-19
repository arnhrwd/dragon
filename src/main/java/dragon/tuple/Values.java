package dragon.tuple;

import java.util.ArrayList;


/**
 * @author aaron
 *
 */
public class Values extends ArrayList<Object>  {
	private static final long serialVersionUID = 3560041625790748387L;

	/**
	 * 
	 */
	public Values() {
		
	}
	
	/**
	 * @param objects
	 */
	public Values(Object...objects) {
		super(objects.length);
		for(Object o: objects) {
			add(o);
		}
	}
	
}
