package dragon.network.operations;

import java.io.Serializable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
/**
 * @author aaron
 *
 */
public class Op implements Serializable {
	private static final long serialVersionUID = -2761109390357720762L;
	
	/**
	 * 
	 */
	@SuppressWarnings("unused")
	private static final Logger log = LogManager.getLogger(Op.class);
	
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
	 * <li>{@link #READY}</li>
	 * <li>{@link #RUNNING}</li>
	 * <li>{@link #FAILED}</li>
	 * <li>{@link #COMPLETED}</li>
	 * @author aaron
	 *
	 */
	public static enum State {
		/**
		 * The op has not yet started but is ready to start.
		 */
		READY,
		
		/**
		 * The op has started, and is waiting for an outcome to be determined.
		 */
		RUNNING,
		
		/**
		 * The op has failed.
		 */
		FAILED,
		
		/**
		 * The op has succeeded.
		 */
		COMPLETED
	}
	
	/**
	 * The current state of the op.
	 */
	private State state;
	
	/**
	 * @param success
	 * @param failure
	 */
	public Op(IOpSuccess success,IOpFailure failure) {
		state=State.READY;
		this.success=success;
		this.failure=failure;
	}
	
	/**
	 * @param start
	 * @param success
	 * @param failure
	 */
	public Op(IOpStart start,IOpSuccess success,IOpFailure failure) {
		state=State.READY;
		this.start=start;
		this.success=success;
		this.failure=failure;
	}
	
	/**
	 * 
	 */
	public Op() {
		this.state=State.READY;
	}

	/**
	 * @param start
	 */
	public void onStart(IOpStart start) {
		this.start=start;
	}
	
	/**
	 * @param running
	 */
	public void onRunning(IOpRunning running) {
		this.running=running;
		if(state==State.RUNNING) {
			running.running(this);
		}
	}
	
	/**
	 * @param success
	 */
	public void onSuccess(IOpSuccess success) {
		this.success=success;
		if(state==State.COMPLETED) {
			success.success(this);
		}
	}
	
	/**
	 * @param failure
	 */
	public void onFailure(IOpFailure failure) {
		this.failure=failure;
		if(state==State.FAILED) {
			failure.fail(this,error);
		}
	}
	
	/**
	 * @param desc
	 * @param opCounter
	 */
	public void init(NodeDescriptor desc,long opCounter) {
		this.sourceDesc=desc;
		this.id=opCounter;
	}
	
	/**
	 * @return
	 */
	public Long getId() {
		return this.id;
	}
	
	/**
	 * @return
	 */
	public NodeDescriptor getSourceDesc() {
		return this.sourceDesc;
	}
	
	/**
	 * @return
	 */
	public State getState() {
		return state;
	}
	
	/**
	 * 
	 */
	public void start() {
		if(start!=null) start.start(this);
		state=State.RUNNING;
		if(running!=null) {
			running.running(this);
		}
	}
	
	/**
	 * 
	 */
	public void success() {
		if(state==State.FAILED) {
			log.error("operation has already failed");
			return;
		}
		state=State.COMPLETED;
		if(success!=null) success.success(this);
	}
	
	/**
	 * @param error
	 */
	public void fail(String error) {
		if(state==State.COMPLETED) {
			log.error("operation has already completed");
			return;
		}
		state=State.FAILED;
		this.error=error;
		if(failure!=null) failure.fail(this,error);
	}
}
