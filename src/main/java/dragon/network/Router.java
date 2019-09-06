package dragon.network;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.Config;
import dragon.NetworkTask;
import dragon.utils.CircularBuffer;

public class Router {
	private static Log log = LogFactory.getLog(Router.class);
	private Node node;
	private ExecutorService outgoingExecutorService;
	private ExecutorService incommingExecutorService;
	private TopologyQueueMap inputQueues;
	private TopologyQueueMap outputQueues;
	private boolean shouldTerminate=false;
	private Config conf;
	private LinkedBlockingQueue<CircularBuffer<NetworkTask>> outputsPending;
	public Router(Node node, Config conf) {
		this.node=node;
		this.conf=conf;
		inputQueues = new TopologyQueueMap((Integer)conf.get(Config.DRAGON_ROUTER_INPUT_BUFFER_SIZE));
		outputQueues = new TopologyQueueMap((Integer)conf.get(Config.DRAGON_ROUTER_OUTPUT_BUFFER_SIZE));
		outgoingExecutorService = Executors.newFixedThreadPool((Integer)conf.get(Config.DRAGON_ROUTER_OUTPUT_THREADS));
		incommingExecutorService = Executors.newFixedThreadPool((Integer)conf.get(Config.DRAGON_ROUTER_INPUT_THREADS));
		outputsPending=new LinkedBlockingQueue<CircularBuffer<NetworkTask>>();
		runExecutors();
	}
	
	private void runExecutors() {
		for(int i=0;i<(Integer)conf.get(Config.DRAGON_ROUTER_OUTPUT_THREADS);i++) {
			outgoingExecutorService.execute(new Runnable() {
				public void run() {
					while(!shouldTerminate) {
						try {
							CircularBuffer<NetworkTask> buffer = outputsPending.take();
							synchronized(buffer.lock){
								NetworkTask task = buffer.peek();
								if(task!=null){
									//node.getComms().sendNetworkTask(desc, task);
								}
							}
						} catch (InterruptedException e) {
							log.debug("interrupted while taking from queue");
						}
					}
				}
			});
		}
		for(int i=0;i<(Integer)conf.get(Config.DRAGON_ROUTER_INPUT_THREADS);i++) {
			incommingExecutorService.execute(new Runnable() {
				public void run() {
					while(!shouldTerminate) {
						NetworkTask task = node.getComms().receiveNetworkTask();
						try {
							inputQueues.put(task);
							node.getLocalClusters().get(task.getTopologyId()).outputPending(inputQueues.getBuffer(task));
						} catch (InterruptedException e) {
							log.error("interrupted");
							break;
						}
					}
				}
			});
		}
	}
	
	public boolean offer(NetworkTask task) {
		boolean ret = outputQueues.getBuffer(task).offer(task);
		if(ret){
			try {
				outputsPending.put(outputQueues.getBuffer(task));
			} catch (InterruptedException e) {
				log.error("interrupted while waiting to put on outputs pending");
			}
		}
		return ret;
	}
	
	public void put(NetworkTask task) throws InterruptedException {
		outputQueues.getBuffer(task).put(task);
		outputsPending.put(outputQueues.getBuffer(task));
	}
	
}
