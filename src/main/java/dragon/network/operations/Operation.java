package dragon.network.operations;

import java.io.Serializable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class Operation implements Serializable {
	private static final long serialVersionUID = -2761109390357720762L;
	@SuppressWarnings("unused")
	private static final Log log = LogFactory.getLog(Operation.class);
	private transient IOperationSuccess success;
	private transient IOperationFailure failure;
	public static enum State {
		READY,
		RUNNING,
		FAILED,
		COMPLETED
	}
	
	private State state;
	
	public void onSuccess(IOperationSuccess success) {
		this.success=success;
	}
	
	public void onFailure(IOperationFailure failure) {
		this.failure=failure;
	}
	
	public Operation(IOperationSuccess success,IOperationFailure failure) {
		state=State.READY;
	}
	
	public State getState() {
		return state;
	}
	
	public void start() {
		state=State.RUNNING;
	}
	
	public void success() {
		state=State.COMPLETED;
		if(success!=null) success.success();
	}
	
	public void fail(String error) {
		state=State.FAILED;
		if(failure!=null) failure.fail(error);
	}
}
