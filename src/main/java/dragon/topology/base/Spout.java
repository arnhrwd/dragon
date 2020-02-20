package dragon.topology.base;

import java.time.Instant;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dragon.LocalCluster;
import dragon.spout.SpoutOutputCollector;
import dragon.task.TopologyContext;
import dragon.topology.OutputFieldsDeclarer;

/**
 * @author aaron
 *
 */
public class Spout extends Component {
	private static final long serialVersionUID = -2734635234747476875L;

	/**
	 * 
	 */
	private static final Logger log = LogManager.getLogger(Spout.class);

	/* (non-Javadoc)
	 * @see dragon.topology.base.Component#run()
	 */
	@Override
	public final void run() {
		getOutputCollector().resetEmit();
		if(closed) {
			log.warn("already closed");
			return;
		}
		if(closing) {
			try {
				close();
			} catch (Exception e) {
				e.printStackTrace();
				log.error("exception thrown when closing: "+e.getMessage());
			}
			log.debug("closed");
			closed=true;
			return;
		}
		try {
			nextTuple();
		} catch (DragonEmitRuntimeException e) {
			/*
			 * Such exceptions generally catch user code that is not behaving according
			 * to the dragon requirements.
			 */
			e.printStackTrace();
			log.error(e.getMessage());
			if(getLocalCluster().getState()==LocalCluster.State.RUNNING) {
				/*
				 * If we are in the running state then we might signal to halt the 
				 * topology.
				 */
				getLocalCluster().componentException(this,e.getMessage(),e.getStackTrace());
			}
		} catch (Exception e) {
			/*
			 * Other exceptions are generally just bad user code.
			 */
			e.printStackTrace();
			log.error(e.getMessage());
			if(getLocalCluster().getState()==LocalCluster.State.RUNNING) {
				/*
				 * If we are in the running state then we might signal to halt the 
				 * topology.
				 */
				getLocalCluster().componentException(this,e.getMessage(),e.getStackTrace());
			}
		} 
		/*
		 * check tuple bundles for expiration
		 */
		long now = Instant.now().toEpochMilli();
		if(now >= getOutputCollector().getNextExpire()) {
			getOutputCollector().expireTupleBundles();
		} else if(!getOutputCollector().didEmit()) {
			/*
			 * user didn't emit anything so sleep until the next
			 * expiration time
			 */
			try {
				Thread.sleep(getOutputCollector().getNextExpire()-now);
			} catch (InterruptedException e) {
				log.info("interrupted");
				return;
			}
		}
	}

	/**
	 * @param conf
	 * @param context
	 * @param collector
	 */
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		
	}
	
	/**
	 * 
	 */
	public void nextTuple() {
		
	}
	
	/**
	 * 
	 */
	public void close() {
		
	}
	
	/**
	 * @param declarer
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}
