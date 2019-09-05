package dragon.network;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.Config;
import dragon.NetworkTask;

public class Router {
	private static Log log = LogFactory.getLog(Router.class);
	private Node node;
	private ExecutorService outgoingExecutorService;
	private ExecutorService incommingExecutorService;
	private StreamQueueMap streamInputQueues;
	private boolean shouldTerminate=false;
	private Config conf;
	public Router(Node node, Config conf) {
		this.node=node;
		this.conf=conf;
		streamInputQueues = new StreamQueueMap();
		outgoingExecutorService = Executors.newFixedThreadPool((Integer)conf.get(Config.DRAGON_ROUTER_THREADS));
		incommingExecutorService = Executors.newFixedThreadPool((Integer)conf.get(Config.DRAGON_ROUTER_THREADS));
		runExecutors();
	}
	
	private void runExecutors() {
		for(int i=0;i<(Integer)conf.get(Config.DRAGON_ROUTER_THREADS);i++) {
			outgoingExecutorService.execute(new Runnable() {
				public void run() {
					while(!shouldTerminate) {
						
					}
				}
			});
		}
		for(int i=0;i<(Integer)conf.get(Config.DRAGON_ROUTER_THREADS);i++) {
			incommingExecutorService.execute(new Runnable() {
				public void run() {
					while(!shouldTerminate) {
						NetworkTask task = node.getComms().receiveNetworkTask();
						try {
							streamInputQueues.put(task);
							node.getLocalClusters().get(task.getTopologyId())
							.outputPending(streamInputQueues.get(task.getTuple()
									.getSourceStreamId()));
						} catch (InterruptedException e) {
							log.error("interrupted");
							break;
						}
					}
				}
			});
		}
	}
	
	
}
