package dragon.network.messages;

/**
 * Messages that are error messages must implement this interface.
 * @author aaron
 *
 */
public interface IErrorMessage {
	/**
	 * @return the error for the message
	 */
	public String getError();
}
