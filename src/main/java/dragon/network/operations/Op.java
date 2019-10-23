package dragon.network.operations;

import java.io.Serializable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.network.NodeDescriptor;

/**
 * An Operation (Op) is a kind of future that is primarily the basis for handling
 * asynchronous protocols like group operations. At each state of the Operation, a callback
 * can bet set (usually to a lambda function) that can undertake tasks relevant to
 * the state. Every Op starts in the READY state and progresses to a final state of
 * either FAILED or COMPLETED.
 * @author aaron
 *
 */
public class Op implements Serializable {
	private static final long serialVersionUID = -2761109390357720762L;
	@SuppressWarnings("unused")
	private static final Log log = LogFactory.getLog(Op.class);
	
	/**
	 * Called when the operation starts, i.e. the first callback that will
	 * be triggered. After starting the operation enters the running state.
	 */
	private transient IOpStart start;
	
	/**
	 * Called if the operation succeeds, after which the state is final.
	 */
	private transient IOpSuccess success;
	
	/**
	 * Called if the operation fails, after which the state is final.
	 */
	private transient IOpFailure failure;
	
	/**
	 * Called after the start has finished, from the running state the
	 * operation can either proceed to success or failure.
	 */
	private transient IOpRunning running;
	
	/**
	 * Id used by the Ops processor to manage the Op.
	 */
	private long id;
	
	/**
	 * The node that initiated the operation.
	 */
	private NodeDescriptor sourceDesc;
	
	/**
	 * An error message in the case where the operation fails.
	 */
	private String error;
	
	/**
	 * Possible states of the operation.
	 * <li>
	 *
	 */
	public static enum State {
		READY,
		RUNNING,
		FAILED,
		COMPLETED
	}
	
	private State state;
	
	public Op(IOpSuccess success,IOpFailure failure) {
		state=State.READY;
		this.success=success;
		this.failure=failure;
	}
	
	public Op(IOpStart start,IOpSuccess success,IOpFailure failure) {
		state=State.READY;
		this.start=start;
		this.success=success;
		this.failure=failure;
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
	
	public void init(NodeDescriptor desc,long opCounter) {
		this.sourceDesc=desc;
		this.id=opCounter;
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
		if(start!=null) start.start(this);
		state=State.RUNNING;
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
