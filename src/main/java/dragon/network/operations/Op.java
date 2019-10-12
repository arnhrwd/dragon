package dragon.network.operations;

import java.io.Serializable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.network.NodeDescriptor;

/**
 * An Operation is a kind of future that requires a number of network
 * messages from other daemons to either succeed or fail.
 * @author aaron
 *
 */
public class Op implements Serializable {
	private static final long serialVersionUID = -2761109390357720762L;
	@SuppressWarnings("unused")
	private static final Log log = LogFactory.getLog(Op.class);
	private transient IOpStart start;
	private transient IOpSuccess success;
	private transient IOpFailure failure;
	private transient IOpRunning running;
	private long id;
	private NodeDescriptor sourceDesc;
	private String error;
	
	public static enum State {
		READY,
		RUNNING,
		FAILED,
		COMPLETED
	}
	
	private State state;
	
	public Op(IOpSuccess success,IOpFailure failure) {
		state=State.READY;
	}
	
	public Op(IOpStart start,IOpSuccess success,IOpFailure failure) {
		state=State.READY;
	}
	
	public Op() {
		this.state=State.READY;
	}

	public void onStart(IOpStart start) {
		this.start=start;
	}
	
	public void onRunning(IOpRunning running) {
		this.running=running;
		if(state==State.RUNNING) {
			running.running(this);
		}
	}
	
	public void onSuccess(IOpSuccess success) {
		this.success=success;
		if(state==State.COMPLETED) {
			success.success(this);
		}
	}
	
	public void onFailure(IOpFailure failure) {
		this.failure=failure;
		if(state==State.FAILED) {
			failure.fail(this,error);
		}
	}
	
	public void init(NodeDescriptor desc,long groupOperationCounter) {
		this.sourceDesc=desc;
		this.id=groupOperationCounter;
	}
	
	public Long getId() {
		return this.id;
	}
	
	public NodeDescriptor getSourceDesc() {
		return this.sourceDesc;
	}
	
	public State getState() {
		return state;
	}
	
	public void start() {
		state=State.RUNNING;
		if(start!=null) start.start(this);
		if(running!=null) running.running(this);
	}
	
	public void success() {
		state=State.COMPLETED;
		if(success!=null) success.success(this);
	}
	
	public void fail(String error) {
		state=State.FAILED;
		this.error=error;
		if(failure!=null) failure.fail(this,error);
	}
}
