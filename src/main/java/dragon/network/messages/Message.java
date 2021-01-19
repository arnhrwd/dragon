package dragon.network.messages;

import java.io.Serializable;

/**
 * The base class for a Dragon message.
 * @author aaron
 *
 */
public class Message implements Serializable {
	private static final long serialVersionUID = -6123498202112069826L;
	
	/**
	 * Called by the receiver to process the message.
	 */
	public void process() {
		
	}
}
