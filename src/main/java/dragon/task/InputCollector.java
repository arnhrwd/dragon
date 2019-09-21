package dragon.task;

import dragon.LocalCluster;
import dragon.topology.base.Bolt;
import dragon.tuple.Tuple;
import dragon.utils.CircularBuffer;

public class InputCollector {
	private CircularBuffer<Tuple> inputQueue;
	private LocalCluster localCluster;
	private Bolt bolt;
	
	public InputCollector(LocalCluster localCluster,Bolt bolt){
		inputQueue=new CircularBuffer<Tuple>((Integer)localCluster.getConf().getDragonOutputBufferSize());
		this.localCluster = localCluster;
		this.bolt=bolt;
		
	}
	
	public CircularBuffer<Tuple> getQueue(){
		return inputQueue;
	}
}
