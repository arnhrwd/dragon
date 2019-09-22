package dragon;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.yaml.snakeyaml.Yaml;

import dragon.network.NodeDescriptor;

public class Config extends HashMap<String, Object>{
	/**
	 * 
	 */
	private static final long serialVersionUID = -5933157870455074368L;
	private static Log log = LogFactory.getLog(Config.class);

	public static final String TOPOLOGY_WORKERS="topology.workers";
	public static final String TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS="topology.trident.batch.emit.interval.millis";
	public static final String TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS="topology.enable.message.timeouts";
	public static final String TOPOLOGY_MAX_SPOUT_PENDING="topology.max.spout.pending";
	public static final String TOPOLOGY_MESSAGE_TIMEOUT_SECS="topology.message.timeout.secs";
	public static final String TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE="topology.executor.receive.buffer.size";
	public static final String TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE="topology.executor.send.buffer.size";
	public static final String TOPOLOGY_TICK_TUPLE_FREQ_SECS="topology.tick.tuple.freq.secs";
	
	public static final String DRAGON_OUTPUT_BUFFER_SIZE="dragon.output.buffer.size";
	public static final String DRAGON_INPUT_BUFFER_SIZE="dragon.input.buffer.size";
	public static final String DRAGON_BASE_DIR="dragon.base.dir";
	public static final String DRAGON_JAR_DIR="dragon.jar.dir";
	public static final String DRAGON_PERSISTANCE_DIR="dragon.persistance.dir";
	public static final String DRAGON_LOCALCLUSTER_THREADS="dragon.localcluster.threads";
	public static final String DRAGON_ROUTER_INPUT_THREADS="dragon.router.input.threads";
	public static final String DRAGON_ROUTER_OUTPUT_THREADS="dragon.router.output.threads";
	public static final String DRAGON_ROUTER_INPUT_BUFFER_SIZE="dragon.router.input.buffer.size";
	public static final String DRAGON_ROUTER_OUTPUT_BUFFER_SIZE="dragon.router.output.buffer.size";

	public static final String DRAGON_NETWORK_LOCAL_HOST="dragon.network.local.host";
	public static final String DRAGON_NETWORK_LOCAL_SERVICE_PORT="dragon.network.local.service.port";
	public static final String DRAGON_NETWORK_LOCAL_DATA_PORT="dragon.network.local.data.port";
	
	public static final String DRAGON_NETWORK_DEFAULT_SERVICE_PORT="dragon.network.default.service.port";
	public static final String DRAGON_NETWORK_DEFAULT_DATA_PORT="dragon.network.default.node.port";
	
	public static final String DRAGON_NETWORK_HOSTS="dragon.network.hosts";
	
	public static final String DRAGON_METRICS_SAMPLE_PERIOD_MS="dragon.metrics.sample.period.ms";
	public static final String DRAGON_METRICS_ENABLED="dragon.metrics.enabled";
	public static final String DRAGON_METRICS_SAMPLE_HISTORY="dragon.metrics.sample.history";
	
	public static final String DRAGON_EMBEDDING_ALGORITHM="dragon.embedding.algorithm";
	public static final String DRAGON_EMBEDDING_CUSTOM_FILE="dragon.embedding.custom.file";
	
	int numWorkers=1;
	int maxTaskParallelism=1000;
	
	public Config() {
		super();
		defaults();
	}
	
	public Config(String file) throws IOException {
		super();
		defaults();
		
		Yaml config = new Yaml();
		//Properties props = new Properties();
		InputStream inputStream=null;
		try {
			log.debug("looking for "+file+" in working directory");
			inputStream = loadByFileName(file);
		} catch (FileNotFoundException e) {
			
		}
		if(inputStream==null) {
			try {
				log.debug("looking for "+file+" in ../conf");
				inputStream =loadByFileName("../conf/"+file);
			} catch (FileNotFoundException e) {
				
			}
		}
		if(inputStream==null) {
			try {
				log.debug("looking for "+file+" in /etc/dragon");
				inputStream = loadByFileName("/etc/dragon/"+file);
			} catch (FileNotFoundException e) {
				
			}
		}
		if(inputStream==null) {
			try {
				String home = System.getenv("HOME");
				log.debug("looking for "+file+" in "+home+"/.dragon");
				inputStream = loadByFileName(home+"/.dragon/"+file);
			} catch (FileNotFoundException e) {
				log.warn("cannot find "+file+" - using defaults");
				return;
			}
		}
		Map<String,Object> map = config.load(inputStream);
		log.debug(map);
		putAll(map);
	}
	
	private InputStream loadByFileName(String name) throws FileNotFoundException {
        File f = new File(name);
        if (f.isFile()) {
            return new FileInputStream(f);
        } else {
            return this.getClass().getClassLoader().getResourceAsStream(name);
        }
    }
	
	public void defaults() {
		put(DRAGON_OUTPUT_BUFFER_SIZE,1024);
		put(DRAGON_INPUT_BUFFER_SIZE,1024);
		put(DRAGON_BASE_DIR,"/tmp/dragon");
		put(DRAGON_PERSISTANCE_DIR,"persistance");
		put(DRAGON_JAR_DIR,"jars");
		put(DRAGON_LOCALCLUSTER_THREADS,10);
		put(DRAGON_ROUTER_INPUT_THREADS,10);
		put(DRAGON_ROUTER_OUTPUT_THREADS,10);
		put(DRAGON_ROUTER_INPUT_BUFFER_SIZE,1024);
		put(DRAGON_ROUTER_OUTPUT_BUFFER_SIZE,1024);
		put(DRAGON_NETWORK_LOCAL_HOST,"localhost");
		put(DRAGON_NETWORK_DEFAULT_SERVICE_PORT,4000);
		//put(DRAGON_NETWORK_LOCAL_SERVICE_PORT,4000);
		put(DRAGON_NETWORK_DEFAULT_DATA_PORT,4001);
		//put(DRAGON_NETWORK_LOCAL_DATA_PORT,4001);
		put(DRAGON_METRICS_ENABLED,true);
		put(DRAGON_METRICS_SAMPLE_PERIOD_MS,60*1000);
		put(DRAGON_METRICS_SAMPLE_HISTORY,1);
		put(DRAGON_NETWORK_HOSTS,new ArrayList<HashMap<String,ArrayList<Integer>>>());
		put(DRAGON_EMBEDDING_ALGORITHM, "dragon.topology.RoundRobinEmbedding");
		put(DRAGON_EMBEDDING_CUSTOM_FILE, "embedding.yaml");
	}

	public void setNumWorkers(int numWorkers) {
		this.numWorkers=numWorkers;
	}
	
	public int getNumberWorkers() {
		return this.numWorkers;
	}
	
	public void setMaxTaskParallelism(int maxTaskParallelism) {
		this.maxTaskParallelism=maxTaskParallelism;
	}
	
	public int getMaxTaskParallelism() {
		return this.maxTaskParallelism;
	}
	
	//
	// Simple Getters
	//
	
	public int getDragonOutputBufferSize() {
		return (Integer)get(DRAGON_OUTPUT_BUFFER_SIZE);
	}
	
	public int getDragonInputBufferSize() {
		return (Integer)get(DRAGON_INPUT_BUFFER_SIZE);
	}
	
	public String getDragonBaseDir() {
		return (String)get(DRAGON_BASE_DIR);
	}
	
	public String getDragonPersistanceDir() {
		return (String)get(DRAGON_PERSISTANCE_DIR);
	}
	
	public String getDragonJarDir() {
		return (String)get(DRAGON_JAR_DIR);
	}
	
	public int getDragonLocalclusterThreads() {
		return (Integer)get(DRAGON_LOCALCLUSTER_THREADS);
	}
	
	public int getDragonRouterInputThreads() {
		return (Integer)get(DRAGON_ROUTER_INPUT_THREADS);
	}
	
	public int getDragonRouterOutputThreads() {
		return (Integer)get(DRAGON_ROUTER_OUTPUT_THREADS);
	}
	
	public int getDragonRouterInputBufferSize() {
		return (Integer)get(DRAGON_ROUTER_INPUT_BUFFER_SIZE);
	}
	
	public int getDragonRouterOutputBufferSize() {
		return (Integer)get(DRAGON_ROUTER_OUTPUT_BUFFER_SIZE);
	}
	
	public String getDragonNetworkLocalHost() {
		return (String)get(DRAGON_NETWORK_LOCAL_HOST);
	}
	
	public int getDragonNetworkDefaultServicePort() {
		return (Integer)get(DRAGON_NETWORK_DEFAULT_SERVICE_PORT);
	}
	
	public int getDragonNetworkLocalServicePort() {
		if(containsKey(DRAGON_NETWORK_LOCAL_SERVICE_PORT))
		return (Integer)get(DRAGON_NETWORK_LOCAL_SERVICE_PORT);
		return getDragonNetworkDefaultServicePort();
	}
	
	public int getDragonNetworkDefaultDataPort() {
		return (Integer)get(DRAGON_NETWORK_DEFAULT_DATA_PORT);
	}
	
	public int getDragonNetworkLocalDataPort() {
		if(containsKey(DRAGON_NETWORK_LOCAL_DATA_PORT))
		return (Integer)get(DRAGON_NETWORK_LOCAL_DATA_PORT);
		return getDragonNetworkDefaultDataPort();
	}
	
	public boolean getDragonMetricsEnabled() {
		return (Boolean)get(DRAGON_METRICS_ENABLED);
	}
	
	public int getDragonMetricsSamplePeriodMs() {
		return (int)get(DRAGON_METRICS_SAMPLE_PERIOD_MS);
	}
	
	public int getDragonMetricsSampleHistory() {
		return (int)get(DRAGON_METRICS_SAMPLE_HISTORY);
	}
	
	@SuppressWarnings("unchecked")
	public ArrayList<HashMap<String,ArrayList<Integer>>> getDragonNetworkHosts(){
		return (ArrayList<HashMap<String,ArrayList<Integer>>>)get(DRAGON_NETWORK_HOSTS);
	}
	
	public String getDragonEmbeddingAlgorithm() {
		return (String)get(DRAGON_EMBEDDING_ALGORITHM);
	}
	
	public String getDragonEmbeddingCustomFile() {
		return (String)get(DRAGON_EMBEDDING_CUSTOM_FILE);
	}
	
	//
	// Advanced Getters
	//
	
	private String onlyKey(HashMap<String,?> map) {
		return (new ArrayList<String>(map.keySet())).get(0);
	}
	
	public String getJarPath() {
		return getDragonBaseDir()+"/"+getDragonJarDir();
	}
	
	public NodeDescriptor getLocalHost() throws UnknownHostException {
		return new NodeDescriptor(getDragonNetworkLocalHost(),
				getDragonNetworkLocalDataPort(),
				getDragonNetworkLocalServicePort());
	}
	
	public ArrayList<NodeDescriptor> getHosts(){
		ArrayList<NodeDescriptor> nodes = new ArrayList<NodeDescriptor>();
		ArrayList<HashMap<String,ArrayList<Integer>>> hosts = getDragonNetworkHosts();
		for(int i=0;i<hosts.size();i++) {
			String hostname = onlyKey(hosts.get(i));
			try {
				if(hosts.get(i).get(hostname)==null || hosts.get(i).get(hostname).size()==0) {
					nodes.add(new NodeDescriptor(hostname,
							getDragonNetworkDefaultDataPort(),
							getDragonNetworkDefaultServicePort()));
				} else if(hosts.get(i).get(hostname).size()==1) {
					nodes.add(new NodeDescriptor(hostname,
							getDragonNetworkDefaultDataPort(),
							hosts.get(i).get(hostname).get(0)));
				} else {
					nodes.add(new NodeDescriptor(hostname,
							hosts.get(i).get(hostname).get(1),
							hosts.get(i).get(hostname).get(0)));
				}
			} catch (UnknownHostException e) {
				log.error(hostname + " is not found");
			}
			
		}
		return nodes;
	}
}
