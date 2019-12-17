package dragon;

import java.io.Serializable;

/**
 * A container class to keep track of exceptions thrown by components.
 * Such things are reported back to the client when e.g. listing the
 * topologies.
 * @author aaron
 *
 */
public class ComponentError implements Serializable {
	private static final long serialVersionUID = -7196582332156656626L;
	
	/**
	 * The error message.
	 */
	public final String message;
	
	/**
	 * The stringified stack trace.
	 */
	public final String stackTrace;
	
	/**
	 * Unpacks the stack trace into a single String.
	 * @param message
	 * @param stackTrace
	 */
	public ComponentError(String message, StackTraceElement[] stackTrace) {
		this.message=message;
		String msg="";
		for(int i=0;i<stackTrace.length;i++) {
			msg+=stackTrace[i].toString()+"\n";
		}
		this.stackTrace=msg;
	}

}
