package dragon.network.messages.node;

import dragon.network.messages.IErrorMessage;

/**
 * Generic node error message.
 * @author aaron
 *
 */
public class NodeErrorMessage extends NodeMessage implements IErrorMessage {
	private static final long serialVersionUID = 1L;
	/**
	 * Error string to show the user.
	 */
	public final String error;
	
	/**
	 * 
	 * @param type
	 * @param error
	 */
	public NodeErrorMessage(NodeMessageType type,String error) {
		super(type);
		this.error=error;
	}

	@Override
	public String getError() {
		// TODO Auto-generated method stub
		return null;
	}
	
	
}
