package dragon.topology.base;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

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
	private AtomicLong dataEmissionInterval = new AtomicLong(0); //in nanoseconds

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
			} catch (Throwable e) {
				e.printStackTrace();
				log.error("exception thrown when closing: "+e.getMessage());
			}
			log.debug("closed");
			closed=true;
			return;
		}
		try {
			nextTuple();
			System.out.println("next tuple called " + System.nanoTime()); 
			
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
		} catch (Throwable e) {
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
	
	public void updateDataEmissionInterval(long delta) {
		dataEmissionInterval.updateAndGet(value -> value + delta >= 0 ? value + delta : 0);
		
		/*if(dataEmissionInterval == null) {
			
			dataEmissionInterval = new AtomicLong(0);
		}
		if(dataEmissionInterval.longValue() + delta >= 0) {
			dataEmissionInterval.addAndGet(delta);
		}*/
	}
	
	public AtomicLong getDataEmissionInterval() {
		return dataEmissionInterval;
	}

}
