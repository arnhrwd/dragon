package dragon.topology.base;

import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dragon.LocalCluster;
import dragon.task.InputCollector;
import dragon.task.OutputCollector;
import dragon.task.TopologyContext;
import dragon.topology.OutputFieldsDeclarer;
import dragon.tuple.RecycleStation;
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
		Tuple tuple=null;
		if(closed)return;
		if(tickTuple!=null) {
			tuple=tickTuple;
			tickTuple=null;
		} else {
			try {
				tuple = getInputCollector().getQueue().remove();
			} catch (NoSuchElementException e) {
				try {
					Thread.sleep(1);
				} catch (InterruptedException e1) {
					return;
				}
				return;
			}
		}
		if(tuple!=null){
			switch(tuple.getType()) {
			case APPLICATION:{
				getOutputCollector().resetEmit();
				try {
					execute(tuple);
				} catch (DragonEmitRuntimeException e) {
					log.warn("bolt ["+getComponentId()+"]: "+e.getMessage());
					if(getLocalCluster().getState()==LocalCluster.State.RUNNING) getLocalCluster().componentException(this,e.getMessage(),e.getStackTrace());
				} catch (Exception e) {
					log.warn("bolt ["+getComponentId()+"]: "+e.toString());
					if(getLocalCluster().getState()==LocalCluster.State.RUNNING) getLocalCluster().componentException(this,e.toString(),e.getStackTrace());
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
					log.debug(getComponentId()+":"+getTaskId()+" closed");
					close();
					getOutputCollector().emitTerminateTuple(); //TODO: see how to call this safely _after_ calling setClosed()
					closed=true;
					
				}
				break;
			}
			default:
				break;
				
			}
			RecycleStation.getInstance().getTupleRecycler(tuple.getFields().getFieldNamesAsString()).crushRecyclable(tuple, 1);
		} else {
			log.error("nothing on the queue!");
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
