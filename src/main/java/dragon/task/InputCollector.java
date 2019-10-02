package dragon.task;

import dragon.LocalCluster;
import dragon.topology.base.Bolt;
import dragon.tuple.Tuple;
import dragon.utils.CircularBlockingQueue;

public class InputCollector {
	private CircularBlockingQueue<Tuple> inputQueue;
	private LocalCluster localCluster;
	private Bolt bolt;
	
	public InputCollector(LocalCluster localCluster,Bolt bolt){
		inputQueue=new CircularBlockingQueue<Tuple>((Integer)localCluster.getConf().getDragonOutputBufferSize());
		this.localCluster = localCluster;
		this.bolt=bolt;
		
	}
	
	public CircularBlockingQueue<Tuple> getQueue(){
		return inputQueue;
	}
}
