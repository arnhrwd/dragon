package dragon.network.comms;

import dragon.network.NodeDescriptor;

/**
 * Thrown when there is a communication error that results in a message not
 * being sent. 
 * @author aaron
 *
 */
public class DragonCommsException extends Exception {
	private static final long serialVersionUID = 152139701376688360L;
	
	/**
	 * 
	 */
	public NodeDescriptor desc;
	
	/**
	 * @param string
	 */
	public DragonCommsException(String string,NodeDescriptor desc) {
		super(string);
		this.desc=desc;
	}
	
	public DragonCommsException(String string) {
		super(string);
	}
}
