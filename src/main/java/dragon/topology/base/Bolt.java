package dragon.topology.base;

import java.util.HashSet;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.task.InputCollector;
import dragon.task.OutputCollector;
import dragon.task.TopologyContext;
import dragon.topology.OutputFieldsDeclarer;
import dragon.tuple.RecycleStation;
import dragon.tuple.Tuple;

public class Bolt extends Component {
	private static final long serialVersionUID = 6696004781292813419L;
	private static final Log log = LogFactory.getLog(Bolt.class);
	private Tuple tickTuple=null;
	private long processed=0;
	private InputCollector inputCollector;
	private HashSet<String> upstreamComponents;
	
	public final void setTickTuple(Tuple tuple) {
		tickTuple=tuple;
	}
	
	@Override
	public final synchronized void run() {
		Tuple tuple;
		if(isClosing()) {
			close();
			setClosed();
			return;
		}
		if(tickTuple!=null) {
			tuple=tickTuple;
			tickTuple=null;
		} else {
			tuple = getInputCollector().getQueue().poll();
		}
		if(tuple!=null){
			switch(tuple.getType()) {
			case APPLICATION:{
				getOutputCollector().resetEmit();
				try {
					execute(tuple);
				} catch (DragonEmitRuntimeException e) {
					log.fatal("bolt ["+getComponentId()+"]: "+e.getMessage());
					getLocalCluster().componentException(this,e.getMessage(),e.getStackTrace());
				} catch (Exception e) {
					log.fatal("bolt ["+getComponentId()+"]: "+e.getMessage());
					getLocalCluster().componentException(this,e.getMessage(),e.getStackTrace());
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
					log.debug("waiting for "+upstreamComponents);
				}
			
				upstreamComponents.remove(tuple.getSourceComponent()+","+tuple.getSourceTaskId()+","+tuple.getSourceStreamId());
				if(upstreamComponents.isEmpty()) {
					//log.debug("closed");
					close();
					getOutputCollector().emitTerminateTuple(); //TODO: see how to call this safely _after_ calling setClosed()
					setClosed();
					
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
	
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
			OutputCollector collector) {
		
	}
	
	public void execute(Tuple tuple){
		
	}
	
	public void close() {
		
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

	public final void setInputCollector(InputCollector inputCollector) {
		upstreamComponents=new HashSet<String>();
		processed=0;
		tickTuple=null;
		this.inputCollector = inputCollector;
	}
	
	public final InputCollector getInputCollector() {
		return inputCollector;
	}
	
	public final long getProcessed(){
		return processed;
	}
	
}
