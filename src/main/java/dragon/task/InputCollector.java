package dragon.task;

import java.util.concurrent.LinkedBlockingQueue;

import dragon.LocalCluster;
import dragon.tuple.Tuple;

public class InputCollector {
	private LinkedBlockingQueue<Tuple> inputQueue;
	private LocalCluster localCluster;
	
	public InputCollector(LocalCluster localCluster){
		inputQueue=new LinkedBlockingQueue<Tuple>();
		this.localCluster = localCluster;
		
	}
	
	public void receive(Tuple tuple) throws InterruptedException {
		inputQueue.put(tuple);
	}
}
