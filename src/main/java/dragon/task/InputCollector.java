package dragon.task;

import dragon.Config;
import dragon.LocalCluster;
import dragon.topology.base.Bolt;
import dragon.tuple.Tuple;
import dragon.utils.CircularBuffer;

public class InputCollector {
	private CircularBuffer<Tuple> inputQueue;
	private LocalCluster localCluster;
	private Bolt bolt;
	
	public InputCollector(LocalCluster localCluster,Bolt bolt){
		inputQueue=new CircularBuffer<Tuple>((Integer)localCluster.getConf().get(Config.DRAGON_OUTPUT_BUFFER_SIZE));
		this.localCluster = localCluster;
		this.bolt=bolt;
		
	}
	
	public CircularBuffer<Tuple> getQueue(){
		return inputQueue;
	}
}
