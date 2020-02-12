package dragon.task;

import org.jctools.queues.MpscArrayQueue;

import dragon.LocalCluster;
import dragon.topology.base.Bolt;
import dragon.tuple.Tuple;
import dragon.utils.CircularBlockingQueue;

/**
 * @author aaron
 *
 */
public class InputCollector {
	/**
	 * 
	 */
	private final MpscArrayQueue<Tuple> inputQueue;
	
	/**
	 * 
	 */
	@SuppressWarnings("unused")
	private final LocalCluster localCluster;
	
	/**
	 * 
	 */
	@SuppressWarnings("unused")
	private final Bolt bolt;
	
	/**
	 * @param localCluster
	 * @param bolt
	 */
	public InputCollector(LocalCluster localCluster,Bolt bolt){
		//inputQueue=new CircularBlockingQueue<Tuple>(localCluster.getConf().getDragonInputBufferSize());
		inputQueue=new MpscArrayQueue<Tuple>(localCluster.getConf().getDragonInputBufferSize());
		this.localCluster = localCluster;
		this.bolt=bolt;
		
	}
	
	/**
	 * @return
	 */
	public MpscArrayQueue<Tuple> getQueue(){
		return inputQueue;
	}
}
