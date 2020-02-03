package dragon.process;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import dragon.Config;


/**
 * Manage processes, that may be time bounded (like running a command
 * to copy a file) or time unbounded (like running a server).
 * 
 * @author aaron
 *
 */
public class ProcessManager extends Thread {
	private final static Logger log = LogManager.getLogger(ProcessManager.class);
	
	/**
	 * 
	 */
	HashMap<Long,ProcessContainer> unbounded;
	
	/**
	 * 
	 */
	ArrayList<ProcessContainer> waitingToStart;
	
	/**
	 * 
	 */
	HashSet<ProcessContainer> running;
	
	/**
	 * 
	 */
	private Config conf;
	
	
	/**
	 * @author aaron
	 *
	 */
	private class ProcessContainer {
		public IProcessOnStart pos;
		public IProcessOnFail pof;
		public IProcessOnExit poe;
		public Process p;
		public ProcessBuilder pb;
		public ProcessContainer(Process p,
				ProcessBuilder pb,
				IProcessOnStart pos,
				IProcessOnFail pof,
				IProcessOnExit poe) {
			this.pos=pos;
			this.pof=pof;
			this.poe=poe;
			this.p=p;
			this.pb=pb;
		}
	}
	
	/**
	 * 
	 * @param conf
	 */
	public ProcessManager(Config conf) {
		unbounded = new HashMap<Long,ProcessContainer>();
		waitingToStart = new ArrayList<ProcessContainer>();
		running = new HashSet<ProcessContainer>();
		this.conf=conf;
		setName("process manager");
		start();
	}
	
	/**
	 * @param pb
	 * @param isUnbounded
	 * @param pos
	 * @param pof
	 * @param poe
	 */
	public void startProcess(ProcessBuilder pb,boolean isUnbounded,
			IProcessOnStart pos,
			IProcessOnFail pof,
			IProcessOnExit poe) {
		Process p;
		if(isUnbounded) {
			try {
				p = pb.start();
				if(pos!=null) {
					pos.process(p);
				}
				unbounded.put(p.pid(),new ProcessContainer(p,pb,pos,pof,poe));
			} catch (IOException e) {
				if(pof!=null) {
					pof.fail(pb);
				} else {
					log.error("could not start process: "+pb.toString());
				}
			}
		} else {
			synchronized(waitingToStart) {
				waitingToStart.add(new ProcessContainer(null,pb,pos,pof,poe));
			}
		}
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Thread#run()
	 */
	@Override
	public void run() {
		log.info("starting up");
		while(!isInterrupted()) {
			while(waitingToStart.size()>0) {
				/**
				 * Startup as many waiting processes as we can within limits.
				 */
				while(waitingToStart.size()>0 && running.size() < conf.getDragonProcessesMax()) {
					ProcessContainer pc;
					synchronized(waitingToStart) {
						pc = waitingToStart.remove(0);
					}
					try {
						pc.p = pc.pb.start();
						if(pc.pos!=null) {
							pc.pos.process(pc.p);
						}
						running.add(pc);
					} catch (IOException e) {
						if(pc.pof!=null) {
							pc.pof.fail(pc.pb);
						} else {
							log.error("could not start process: "+pc.pb.toString());
						}
					}
				}
				
				/**
				 * Wait for processes while there are max outstanding
				 */
				while(running.size()>0 && !(waitingToStart.size()>0 && 
						running.size()<conf.getDragonProcessesMax())) {
					ArrayList<ProcessContainer> done = new ArrayList<ProcessContainer>();
					for(ProcessContainer pc : running) {
						if(!pc.p.isAlive()) {
							if(pc.poe!=null) {
								pc.poe.process(pc.p);
							}
							done.add(pc);
						}
					}
					for(ProcessContainer pc : done) {
						running.remove(pc);
					}
					try {
						Thread.sleep(500);
					} catch (InterruptedException e) {
						interrupt();
						break;
					}
				}
			}
			
			/**
			 * Sleep when there are no bounded processes to wait for.
			 */
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				break;
			}
		}
		log.info("shutting down");
	}
}
