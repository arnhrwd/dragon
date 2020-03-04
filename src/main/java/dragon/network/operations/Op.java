package dragon.network.operations;

import java.io.Serializable;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dragon.network.NodeDescriptor;

/**
 * An Operation (Op) is a kind of future that is primarily the basis for handling
 * asynchronous protocols like group operations. At each state of the Operation, a callback
 * can bet set (usually to a lambda function) that can undertake tasks relevant to
 * the state. Every Op starts in the READY state and progresses to a final state of
 * either FAILED, COMPLETED or CANCELED.
 * 
 * @author aaron
 *
 */
public class Op implements Serializable {
	private static final long serialVersionUID = -2761109390357720762L;
	
	/**
	 * 
	 */
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
	 * Called if a timeout is set, after the timeout period expires.
	 * Does not change the state of the operation.
	 */
	private transient IOpTimeout timeout;
	
	/**
	 * Called if the op was canceled.
	 */
	private transient IOpCancel cancel;
	
	/**
	 * Timer task
	 */
	private transient TimerTask timerTask;
	
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
	 * <li>{@link #CANCELED}</li>
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
		COMPLETED,
		
		/**
		 * The op has been canceled.
		 */
		CANCELED
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
	
	public Op(IOpStart start,IOpRunning running,
			IOpSuccess success,IOpFailure failure) {
		state=State.READY;
		this.start=start;
		this.running=running;
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
	public synchronized Op onStart(IOpStart start) {
		this.start=start;
		return this;
	}
	
	/**
	 * @param running
	 */
	public synchronized Op onRunning(IOpRunning running) {
		this.running=running;
		if(state==State.RUNNING) {
			running.running(this);
		}
		return this;
	}
	
	/**
	 * @param success
	 */
	public synchronized Op onSuccess(IOpSuccess success) {
		this.success=success;
		if(state==State.COMPLETED) {
			success.success(this);
		}
		return this;
	}
	
	/**
	 * @param failure
	 */
	public synchronized Op onFailure(IOpFailure failure) {
		this.failure=failure;
		if(state==State.FAILED) {
			failure.fail(this,error);
		}
		return this;
	}
	
	/**
	 * 
	 * @param timer
	 * @param duration
	 * @param unit
	 * @param timeout
	 * @return
	 */
	public synchronized Op onTimeout(Timer timer,long duration,TimeUnit unit,IOpTimeout timeout) {
		if(this.state==State.FAILED || this.state==State.COMPLETED || this.state==State.CANCELED) return this;
		this.timeout=timeout;
		final Op me=this;
		timerTask = new TimerTask() {
			@Override
			public void run() {
				me.timeout();
			}
		};
		timer.schedule(timerTask, unit.toMillis(duration));
		return this;
	}
	
	/**
	 * 
	 * @param cancel
	 * @return
	 */
	public synchronized Op onCancel(IOpCancel cancel) {
		this.cancel=cancel;
		if(state==State.CANCELED) {
			cancel.cancel(this);
		}
		return this;
	}
	
	/**
	 * @param desc
	 * @param opCounter
	 */
	public synchronized void init(NodeDescriptor desc,long opCounter) {
		this.sourceDesc=desc;
		this.id=opCounter;
	}
	
	/**
	 * @return
	 */
	public synchronized Long getId() {
		return this.id;
	}
	
	/**
	 * @return
	 */
	public synchronized NodeDescriptor getSourceDesc() {
		return this.sourceDesc;
	}
	
	/**
	 * @return
	 */
	public synchronized State getState() {
		return state;
	}
	
	/**
	 * Must only be called once.
	 */
	public synchronized void start() {
		if(start!=null) start.start(this);
		if(state!=State.READY) return;
		state=State.RUNNING;
		if(running!=null) {
			running.running(this);
		}
	}
	
	/**
	 * 
	 */
	public synchronized void success() {
		if(state==State.FAILED) {
			log.warn("operation has already failed");
			return;
		}
		if(state==State.CANCELED) {
			log.warn("operation has already been canceled");
			return;
		}
		if(state==State.COMPLETED) {
			log.warn("operation has already completed");
			return;
		}
		if(timerTask!=null) timerTask.cancel();
		state=State.COMPLETED;
		if(success!=null) success.success(this);
	}
	
	/**
	 * @param error
	 */
	public synchronized void fail(String error) {
		if(state==State.COMPLETED) {
			log.warn("operation has already completed");
			return;
		}
		if(state==State.CANCELED) {
			log.warn("operation has already been canceled");
			return;
		}
		if(state==State.FAILED) {
			log.warn("operation has already failed");
			return;
		}
		if(timerTask!=null) timerTask.cancel();
		state=State.FAILED;
		this.error=error;
		if(failure!=null) failure.fail(this,error);
	}
	
	/**
	 * Its up to the callee to decide if the op should
	 * fail or not as a result of the timeout.
	 */
	public synchronized void timeout() {
		if(timeout!=null) timeout.timeout(this);
	}
	
	/**
	 * 
	 */
	public synchronized void cancel() {
		if(state==State.FAILED || state==State.COMPLETED || state==State.CANCELED) {
			log.error("operation has already failed/completed/canceled");
			return;
		}
		if(timerTask!=null) timerTask.cancel();
		state=State.CANCELED;
		if(cancel!=null) cancel.cancel(this);
	}
}
