package dragon.process;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
	HashMap<Long,ProcessContainer> unboundedRunning;
	
	/**
	 * 
	 */
	ArrayList<ProcessContainer> boundedWaiting;
	
	/**
	 * 
	 */
	HashSet<ProcessContainer> boundedRunning;
	
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
		unboundedRunning = new HashMap<Long,ProcessContainer>();
		boundedWaiting = new ArrayList<ProcessContainer>();
		boundedRunning = new HashSet<ProcessContainer>();
		this.conf=conf;
		setName("process manager");
		start();
	}
	
	/**
	 * Start unbounded and bounded processes, keeping a limit 
	 * (DRAGON_PROCESSES_MAX) on how many 
	 * bounded processes can be running
	 * at any one time.
	 * @param pb the process builder object
	 * @param isUnbounded true if the process will run for an unbounded amount of time (like a server), 
	 * false if the process is expected to be short lived (like a command to do something)
	 * @param pos callback when process successfully starts
	 * @param pof callback if process fails to start
	 * @param poe callback when process exits
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
				unboundedRunning.put(p.pid(),new ProcessContainer(p,pb,pos,pof,poe));
				p.onExit().thenRunAsync(new Runnable() {

					@Override
					public void run() {
						// TODO Auto-generated method stub
						if(poe!=null) {
							poe.process(p);
						}
					}
					
				});
			} catch (IOException e) {
				if(pof!=null) {
					pof.fail(pb);
				} else {
					log.error("could not start process: "+pb.toString());
				}
			}
		} else {
			synchronized(boundedWaiting) {
				boundedWaiting.add(new ProcessContainer(null,pb,pos,pof,poe));
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
			while(boundedWaiting.size()>0) {
				/**
				 * Startup as many waiting processes as we can within limits.
				 */
				while(boundedWaiting.size()>0 && boundedRunning.size() < conf.getDragonProcessesMax()) {
					ProcessContainer pc;
					synchronized(boundedWaiting) {
						pc = boundedWaiting.remove(0);
					}
					try {
						pc.p = pc.pb.start();
						if(pc.pos!=null) {
							pc.pos.process(pc.p);
						}
						boundedRunning.add(pc);
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
				while(boundedRunning.size()>0 && !(boundedWaiting.size()>0 && 
						boundedRunning.size()<conf.getDragonProcessesMax())) {
					ArrayList<ProcessContainer> done = new ArrayList<ProcessContainer>();
					for(ProcessContainer pc : boundedRunning) {
						if(!pc.p.isAlive()) {
							if(pc.poe!=null) {
								pc.poe.process(pc.p);
							}
							done.add(pc);
						}
					}
					for(ProcessContainer pc : done) {
						boundedRunning.remove(pc);
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
		if(!boundedRunning.isEmpty() || !boundedWaiting.isEmpty()) {
			log.error("still some bounded process that were not complete");
		}
	}
	
	public static ProcessBuilder createDaemon(Config conf) {
		String home=conf.getDragonHomeDir();
		File stdout = new File(home+"/log/dragon-"+conf.getDragonNetworkLocalDataPort()+".stdout");
		File stderr = new File(home+"/log/dragon-"+conf.getDragonNetworkLocalDataPort()+".stderr");
		ProcessBuilder pb = new ProcessBuilder(home+"/bin/dragon.sh",
				"-d","-C",home+"/conf/dragon-"+conf.getDragonNetworkLocalDataPort()+".yaml")
				.redirectOutput(stdout)
				.redirectError(stderr);
		return pb;
	}
	
	public static ProcessBuilder killDaemon(Config conf) throws IOException {
		String pidfile=conf.getDragonDataDir()+"/dragon-"+conf.getDragonNetworkLocalDataPort()+".pid";
		File file = new File(pidfile);
		FileInputStream fos = new FileInputStream(file);
		BufferedReader bw = new BufferedReader(new InputStreamReader(fos));
		String pid=bw.readLine();
		bw.close();
		ProcessBuilder pb = new ProcessBuilder("kill","-KILL",pid);
		return pb;
	}
}
