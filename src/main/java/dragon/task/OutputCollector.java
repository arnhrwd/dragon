package dragon.task;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.LocalCluster;
import dragon.NetworkTask;
import dragon.topology.base.IRichBolt;
import dragon.tuple.Tuple;
import dragon.tuple.Values;


public class OutputCollector {
	private Log log = LogFactory.getLog(OutputCollector.class);
	private LinkedBlockingQueue<NetworkTask> outputQueue;
	private LocalCluster localCluster;
	private IRichBolt iRichBolt;
	
	public OutputCollector(LocalCluster localCluster,IRichBolt iRichBolt) {
		outputQueue=new LinkedBlockingQueue<NetworkTask>();
		this.localCluster=localCluster;
		this.iRichBolt=iRichBolt;
	}
	
	public List<Integer> emit(Tuple anchorTuple, Values values){
		return emit(values);
	}
	
	public List<Integer> emit(Values values){
		Tuple tuple = new Tuple(iRichBolt.getOutputFieldsDeclarer().fields,values);
		List<Integer> receivingTaskIds = new ArrayList<Integer>();
		localCluster.outputPending(outputQueue);
		try {
			outputQueue.put(new NetworkTask(tuple,new HashSet<Integer>(receivingTaskIds),""));
		} catch (InterruptedException e) {
			log.error("interrupted while waiting to put values on queue");
		}
		
		return receivingTaskIds;
	}
	
	public void ack(Tuple tuple) {
		
	}
}
