package dragon.topology.base;

import java.time.Instant;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dragon.LocalCluster;
import dragon.task.InputCollector;
import dragon.task.OutputCollector;
import dragon.task.TopologyContext;
import dragon.topology.OutputFieldsDeclarer;
import dragon.tuple.Tuple;

/**
 * @author aaron
 *
 */
public class Bolt extends Component {
	private static final long serialVersionUID = 6696004781292813419L;
	
	/**
	 * 
	 */
	private static final Logger log = LogManager.getLogger(Bolt.class);
	
	/**
	 * 
	 */
	private Tuple tickTuple=null;
	
	/**
	 * 
	 */
	private long processed=0;
	
	/**
	 * 
	 */
	private InputCollector inputCollector;
	
	/**
	 * 
	 */
	private HashSet<String> upstreamComponents;
	
	/**
	 * @param tuple
	 */
	public final void setTickTuple(Tuple tuple) {
		tickTuple=tuple;
	}
	
	/* (non-Javadoc)
	 * @see dragon.topology.base.Component#run()
	 */
	@Override
	public final void run() {
		Tuple[] tuples;
		long now=Instant.now().toEpochMilli();
		if(closed)return;
		if(tickTuple!=null) {
			tuples=new Tuple[] {tickTuple};
			tickTuple=null;
		} else {
			try {
				// poll, but timeout at a time when a bundle will expire
				tuples = getInputCollector().getQueue().poll(Math.max(getOutputCollector().getNextExpire()-now,1),TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				return;
			}
		}
		if(tuples!=null) {
			/*
			 * The entire array of tuples may not be used.
			 */
			for(int i=0;i<tuples.length&&tuples[i]!=null;i++) {
				Tuple tuple = tuples[i];
				switch(tuple.getType()) {
				case APPLICATION:{
					getOutputCollector().resetEmit();
					try {
						execute(tuple);
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
						 * Other exceptions/throwables are generally just bad user code.
						 */
						e.printStackTrace();
						log.error(e.getMessage());
						if(getLocalCluster().getState()==LocalCluster.State.RUNNING) {
							/*
							 * If we are in the running state then we might signal to halt the 
							 * topology.
							 */
							getLocalCluster().componentException(this,e.toString(),e.getStackTrace());
						}
					}
					processed++;
					break;
				}
				case CHECKPOINT:{
					break;
				}
				case FREEZE:
					break;
				case TERMINATE:{
					if(upstreamComponents.isEmpty()) {
					
						for(String componentId : getLocalCluster()
								.getTopology().getBoltMap()
								.get(getComponentId()).groupings.keySet()) {
							int numTasks=0;
							if(getLocalCluster().getTopology().getSpoutMap().containsKey(componentId)) {
								numTasks=getLocalCluster().getTopology().getSpoutMap().get(componentId).getNumTasks();
							} else {
								numTasks=getLocalCluster().getTopology().getBoltMap().get(componentId).getNumTasks();
							}
							
							for(Integer taskId = 0;taskId<numTasks;taskId++) {
								for(String streamId : getLocalCluster()
										.getTopology().getBoltMap()
										.get(getComponentId()).groupings.get(componentId).keySet()){
									
									upstreamComponents.add(componentId+","+taskId+","+streamId);
								}
							}
						}	
						//log.debug("waiting for "+upstreamComponents);
					}
				
					upstreamComponents.remove(tuple.getSourceComponent()+","+tuple.getSourceTaskId()+","+tuple.getSourceStreamId());
					if(upstreamComponents.isEmpty()) {
						try {
							close();
						} catch (Throwable e) {
							e.printStackTrace();
							log.error("exception thrown when closing: "+e.getMessage());
						}
						log.debug("closed");
						/*
						 * push the terminate tuple down the stream
						 */
						getOutputCollector().emitTerminateTuple(); //TODO: see how to call this safely _after_ calling setClosed()
						getOutputCollector().expireAllTupleBundles();
						closed=true;
					}
					break;
				}
				default:
					break;
					
				}
			} 
		} 
		/*
		 * check tuple bundles for expiration
		 */
		now=Instant.now().toEpochMilli();
		if(getOutputCollector().getNextExpire()<=now) {
			getOutputCollector().expireTupleBundles();
		}
	}
	
	/**
	 * @param conf
	 * @param context
	 * @param collector
	 */
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
			OutputCollector collector) {
		
	}
	
	/**
	 * @param tuple
	 */
	public void execute(Tuple tuple){
		
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

	/**
	 * @param inputCollector
	 */
	public final void setInputCollector(InputCollector inputCollector) {
		upstreamComponents=new HashSet<String>();
		processed=0;
		tickTuple=null;
		this.inputCollector = inputCollector;
	}
	
	/**
	 * @return
	 */
	public final InputCollector getInputCollector() {
		return inputCollector;
	}
	
	/**
	 * @return
	 */
	public final long getProcessed(){
		return processed;
	}
	
}
