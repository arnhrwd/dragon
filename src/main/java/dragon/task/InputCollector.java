package dragon.task;

import dragon.Config;
import dragon.LocalCluster;
import dragon.topology.base.IRichBolt;
import dragon.tuple.Tuple;
import dragon.utils.CircularBuffer;

public class InputCollector {
	private CircularBuffer<Tuple> inputQueue;
	private LocalCluster localCluster;
	private IRichBolt iRichBolt;
	
	public InputCollector(LocalCluster localCluster,IRichBolt iRichBolt){
		inputQueue=new CircularBuffer<Tuple>((Integer)localCluster.getConf().get(Config.DRAGON_OUTPUT_BUFFER_SIZE));
		this.localCluster = localCluster;
		this.iRichBolt=iRichBolt;
		
	}
	
	public CircularBuffer<Tuple> getQueue(){
		return inputQueue;
	}
}
