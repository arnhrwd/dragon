package dragon.task;

import dragon.Config;
import dragon.LocalCluster;
import dragon.topology.base.IRichBolt;
import dragon.tuple.Tuple;
import dragon.utils.DurableCircularBuffer;

public class InputCollector {
	private DurableCircularBuffer<Tuple> inputQueue;
	private LocalCluster localCluster;
	private IRichBolt iRichBolt;
	
	public InputCollector(LocalCluster localCluster,IRichBolt iRichBolt){
		inputQueue=new DurableCircularBuffer<Tuple>(
				(Integer)localCluster.getConf().get(Config.DRAGON_OUTPUT_BUFFER_SIZE),
				localCluster.getPersistanceDir()+"/"+iRichBolt.getComponentId()+"_"+iRichBolt.getTaskId());
		this.localCluster = localCluster;
		this.iRichBolt=iRichBolt;
		
	}
	
	public DurableCircularBuffer<Tuple> getQueue(){
		return inputQueue;
	}
}
