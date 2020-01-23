package dragon.network.comms;

/**
 * Thrown when there is a communication error that results in a message not
 * being sent. 
 * @author aaron
 *
 */
public class DragonCommsException extends Exception {
	private static final long serialVersionUID = 152139701376688360L;
	
	public DragonCommsException(String string) {
		super(string);
	}
}
