package dragon.network;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import dragon.Config;
import dragon.NetworkTask;

public class Router {
	private IComms comms;
	private ExecutorService outgoingExecutorService;
	private ExecutorService incommingExecutorService;
	private boolean shouldTerminate=false;
	private Config conf;
	public Router(IComms comms, Config conf) {
		this.comms=comms;
		this.conf=conf;
		outgoingExecutorService = Executors.newFixedThreadPool((Integer)conf.get(Config.DRAGON_ROUTER_THREADS));
		incommingExecutorService = Executors.newFixedThreadPool((Integer)conf.get(Config.DRAGON_ROUTER_THREADS));
		
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
						NetworkTask task = comms.receiveNetworkTask();
					}
				}
			});
		}
	}
	
	
}
