package dragon;

import java.io.Serializable;

public class ComponentError implements Serializable {
	private static final long serialVersionUID = -7196582332156656626L;
	private final String message;
	private final String stackTrace;
	public ComponentError(String message, StackTraceElement[] stackTrace) {
		this.message=message;
		String msg="";
		for(int i=0;i<stackTrace.length;i++) {
			msg+=stackTrace[i].toString()+"\n";
		}
		this.stackTrace=msg;
	}

}
