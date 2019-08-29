package dragon.task;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.Config;
import dragon.LocalCluster;
import dragon.NetworkTask;
import dragon.topology.base.IRichBolt;
import dragon.tuple.Tuple;
import dragon.tuple.Values;
import dragon.utils.DurableCircularBuffer;


public class OutputCollector {
	private Log log = LogFactory.getLog(OutputCollector.class);
	private DurableCircularBuffer<NetworkTask> outputQueue;
	private LocalCluster localCluster;
	private IRichBolt iRichBolt;
	
	public OutputCollector(LocalCluster localCluster,IRichBolt iRichBolt) {
		outputQueue=new DurableCircularBuffer<NetworkTask>(
				(Integer)localCluster.getConf().get(Config.DRAGON_OUTPUT_BUFFER_SIZE),
				localCluster.getPersistanceDir()+"/"+iRichBolt.getComponentId()+"_"+iRichBolt.getTaskId());
		this.localCluster=localCluster;
		this.iRichBolt=iRichBolt;
	}
	
	public synchronized List<Integer> emit(Tuple anchorTuple, Values values){
		return emit(values);
	}
	
	public synchronized List<Integer> emit(Values values){
		Tuple tuple = new Tuple(iRichBolt.getOutputFieldsDeclarer().fields,values);
		List<Integer> receivingTaskIds = new ArrayList<Integer>();
		
		
		
		try {
			outputQueue.put(new NetworkTask(tuple,new HashSet<Integer>(receivingTaskIds),""));
			localCluster.outputPending(this);
		} catch (InterruptedException e) {
			log.error("failed to emit tuple: "+e.toString());
		}
			
		
		return receivingTaskIds;
	}
	
	public void ack(Tuple tuple) {
		
	}
	
	public DurableCircularBuffer<NetworkTask> getQueue(){
		return outputQueue;
	}
}
