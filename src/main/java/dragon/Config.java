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
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;


import dragon.network.NodeDescriptor;

/**
 * Parameters for Dragon. See the README.md file for documentation.
 * @author aaron
 *
 */
public class Config extends HashMap<String, Object> {
	private static final long serialVersionUID = -5933157870455074368L;
	private static final Log log = LogFactory.getLog(Config.class);
	
	/**
	 * legacy parameter from storm to indicate when tick tuples should be sent
	 */
	public static final String TOPOLOGY_TICK_TUPLE_FREQ_SECS="topology.tic.tuple.freq.secs";
	
	/**
	 * the size of the buffers on Spout and Bolt outputs
	 */
	public static final String DRAGON_OUTPUT_BUFFER_SIZE="dragon.output.buffer.size";
	
	/**
	 * the size of the buffers on Spout and Bolt inputs
	 */
	public static final String DRAGON_INPUT_BUFFER_SIZE="dragon.input.buffer.size";
	
	/**
	 * the base directory where Dragon can store files such as submitted jar files and check point data
	 */
	public static final String DRAGON_BASE_DIR="dragon.base.dir";
	
	/**
	 * the sub-directory to store jars files within
	 */
	public static final String DRAGON_JAR_DIR="dragon.jar.dir";
	
	/**
	 * the sub-directory to store persistence information within
	 */
	public static final String DRAGON_PERSISTENCE_DIR="dragon.persistance.dir";
	
	/**
	 * the size of the thread pool that transfers tuples within a local cluster
	 */
	public static final String DRAGON_LOCALCLUSTER_THREADS="dragon.localcluster.threads";
	
	/**
	 * the size of the thread pool that transfers tuples into the local cluster
	 * from the network (note that values larger than 1 result in tuple reordering on streams)
	 */
	public static final String DRAGON_ROUTER_INPUT_THREADS="dragon.router.input.threads";
	
	/**
	 *  the size of the thread pool that transfers tuples out of the local cluster to the 
	 *  network (note that values large than 1 result in tuple reordering on streams)
	 */
	public static final String DRAGON_ROUTER_OUTPUT_THREADS="dragon.router.output.threads";
	
	/**
	 * the size of the buffers for tuples transferring into the local cluster from the network
	 */
	public static final String DRAGON_ROUTER_INPUT_BUFFER_SIZE="dragon.router.input.buffer.size";
	
	/**
	 * the size of the buffers for tuples transferring out of the local cluster to the network
	 */
	public static final String DRAGON_ROUTER_OUTPUT_BUFFER_SIZE="dragon.router.output.buffer.size";

	/**
	 * the number of milliseconds to wait between retries when attempting to make a connection
	 */
	public static final String DRAGON_COMMS_RETRY_MS="dragon.comms.retry.ms";
	
	/**
	 * the number of retries to make before suspending retry attempts
	 */
	public static final String DRAGON_COMMS_RETRY_ATTEMPTS="dragon.comms.retry.attempts";
	
	/**
	 * the default advertised host name for the Dragon daemon
	 */
	public static final String DRAGON_NETWORK_LOCAL_HOST="dragon.network.local.host";
	
	/**
	 *  the service port for the Dragon daemon, if not set then the default service port is used
	 */
	public static final String DRAGON_NETWORK_LOCAL_SERVICE_PORT="dragon.network.local.service.port";
	
	/**
	 * the data port for the Dragon daemon, if not set then the default data port is used
	 */
	public static final String DRAGON_NETWORK_LOCAL_DATA_PORT="dragon.network.local.data.port";
	
	/**
	 * the default service port
	 */
	public static final String DRAGON_NETWORK_DEFAULT_SERVICE_PORT="dragon.network.default.service.port";
	
	/**
	 * the default data port
	 */
	public static final String DRAGON_NETWORK_DEFAULT_DATA_PORT="dragon.network.default.data.port";
	
	/**
	 * strictly an array of dictionaries which is used to bootstrap a daemon, in the format:
	 * [{hostname:localhost,dport:4001,sport:4000,primary:true,partition:primary},...]
	 */
	public static final String DRAGON_NETWORK_HOSTS="dragon.network.hosts";
	
	/**
	 * only one Dragon daemon per machine should be designated as the primary
	 */
	public static final String DRAGON_NETWORK_PRIMARY="dragon.network.primary";
	
	/**
	 * the partition name for this daemon
	 */
	public static final String DRAGON_NETWORK_PARTITION="dragon.network.partition";
	
	/**
	 * the sample period in milliseconds
	 */
	public static final String DRAGON_METRICS_SAMPLE_PERIOD_MS="dragon.metrics.sample.period.ms";
	
	/**
	 * whether the Dragon daemon should record metrics
	 */
	public static final String DRAGON_METRICS_ENABLED="dragon.metrics.enabled";
	
	/**
	 * how much sample history to record
	 */
	public static final String DRAGON_METRICS_SAMPLE_HISTORY="dragon.metrics.sample.history";
	
	/**
	 * the embedding algorithm that maps a task in the topology to a host node
	 */
	public static final String DRAGON_EMBEDDING_ALGORITHM="dragon.embedding.algorithm";
	
	/**
	 * file to use for custom embedding
	 */
	public static final String DRAGON_EMBEDDING_CUSTOM_FILE="dragon.embedding.custom.file";
	
	/**
	 * number of tuple objects to allocate in advance
	 */
	public static final String DRAGON_RECYCLER_TUPLE_CAPACITY="dragon.recycler.tuple.capacity";
	
	/**
	 * number of tuple objects to increase the tuple pool by 
	 * when/if the tuple pool capacity is reached
	 */
	public static final String DRAGON_RECYCLER_TUPLE_EXPANSION="dragon.recycler.tuple.expansion";
	
	/**
	 * fraction of capacity the tuple pool size must reach to trigger compaction of the pool
	 */
	public static final String DRAGON_RECYCLER_TUPLE_COMPACT="dragon.recycler.tuple.compact";
	
	/**
	 * number of network task objects to allocate in advance
	 */
	public static final String DRAGON_RECYCLER_TASK_CAPACITY="dragon.recycler.task.capacity";
	
	/**
	 * number of network task objects to increase the network task pool by 
	 * when/if the tuple pool capacity is reached
	 */
	public static final String DRAGON_RECYCLER_TASK_EXPANSION="dragon.recycler.task.expansion";
	
	/**
	 * fraction of capacity the network task pool size must reach to trigger compaction of the pool
	 */
	public static final String DRAGON_RECYCLER_TASK_COMPACT="dragon.recycler.task.compact";
	
	/**
	 * (advanced) the number of network tasks transmitted over object stream before reseting
	 *  the object stream handle table
	 */
	public static final String DRAGON_COMMS_RESET_COUNT="dragon.comms.reset.count";
	
	/**
	 * the size of the buffer for incoming network tasks, shared over all sockets
	 */
	public static final String DRAGON_COMMS_INCOMING_BUFFER_SIZE="dragon.comms.incoming.buffer.size";
	
	/**
	 * number of faults (exceptions caught) for any component after which the topology is halted
	 */
	public static final String DRAGON_FAULTS_COMPONENT_TOLERANCE="dragon.faults.component.tolerance";
	

	/**
	 * the path of the Java binary
	 */
	public static final String DRAGON_JAVA_BIN="dragon.java.bin";

	/**
	 * the URL to use for the InfluxDB, if available
	 * If this parameter is not given then InfluxDB will not be used.
	 */
	public static final String INFLUXDB_URL="influxdb.url";
	
	/**
	 * the authorization token used to access the InfluxDB
	 */
	public static final String INFLUXDB_TOKEN="influxdb.token";
	
	/**
	 * the InfluxDB bucket to use for storing data samples
	 */
	public static final String INFLUXDB_BUCKET="influxdb.bucket";
	
	/**
	 * the organization name for storing data samples
	 */
	public static final String INFLUXDB_ORGANIZATION="influxdb.organization";

	
	/**
	 * maximum bounded processes running at any one time
	 */
	public static final String DRAGON_PROCESSES_MAX="dragon.processes.max";
	
	/**
	 * Use default config and drop parameters that are relevant to the daemon.
	 */
	public Config() {
		super();
		defaults();
		drop();
	}
	
	/**
	 * Use default config and overwrite with supplied conf.
	 * @param conf
	 */
	public Config(Map<String,Object> conf) {
		super();
		defaults();
		log.debug("using configuration supplied on command line");
		log.debug(conf);
		putAll(conf);
	}
	
	/**
	 * Use default config and overwrite with conf from supplied file.
	 * @param file the filename to read conf from
	 * @throws IOException if there was a problem reading the file
	 */
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
		if(inputStream==null){
			log.warn("cannot find "+file+"- using default");
			return;
		}
		Map<String,Object> map = config.load(inputStream);
		if(map!=null) {
			putAll(map);
			log.debug(toYamlString());
		} else {
			log.warn("empty conf file");
		}
	}
	
	/**
	 * Construct an input stream from a supplied file name. Look in the class
	 * loader if the file is not found on the file system.
	 * @param name
	 * @return the input stream for the supplied file name
	 * @throws FileNotFoundException if the file cannot be found
	 */
	private InputStream loadByFileName(String name) throws FileNotFoundException {
        File f = new File(name);
        if (f.isFile()) {
            return new FileInputStream(f);
        } else {
            return this.getClass().getClassLoader().getResourceAsStream(name);
        }
    }
	
	/**
	 * Return the conf as a YAML string, without any new line characters. Useful
	 * for including on a command line as a parameter.
	 * @return the conf as a YAML string 
	 */
	public String toYamlString() {
		DumperOptions options = new DumperOptions();
		options.setPrettyFlow(false);
		options.setSplitLines(false);
		options.setDefaultFlowStyle(DumperOptions.FlowStyle.FLOW);
		Yaml config = new Yaml(options);
		String ret = config.dump(this).stripTrailing();
		return ret;
	}
	
	/**
	 * Return the conf as a YAML string, Useful
	 * for writing to a file that will be human readable.
	 * @return the conf as a YAML string 
	 */
	public String toYamlStringNice() {
		DumperOptions options = new DumperOptions();
		options.setPrettyFlow(false);
		options.setSplitLines(false);
		Yaml config = new Yaml(options);
		String ret = config.dump(this);
		return ret;
	}
	
	/**
	 * Setup default values
	 */
	public void defaults() {
		put(DRAGON_OUTPUT_BUFFER_SIZE,16);
		put(DRAGON_INPUT_BUFFER_SIZE,16);
		put(DRAGON_BASE_DIR,"/tmp/dragon");
		put(DRAGON_PERSISTENCE_DIR,"persistance");
		put(DRAGON_JAR_DIR,"jars");
		put(DRAGON_LOCALCLUSTER_THREADS,5);
		put(DRAGON_ROUTER_INPUT_THREADS,1);
		put(DRAGON_ROUTER_OUTPUT_THREADS,1);
		put(DRAGON_ROUTER_INPUT_BUFFER_SIZE,16);
		put(DRAGON_ROUTER_OUTPUT_BUFFER_SIZE,16);
		put(DRAGON_COMMS_RETRY_MS,10*1000);
		put(DRAGON_COMMS_RETRY_ATTEMPTS,30);
		put(DRAGON_NETWORK_LOCAL_HOST,"localhost");
		put(DRAGON_NETWORK_DEFAULT_SERVICE_PORT,4000);
		put(DRAGON_NETWORK_DEFAULT_DATA_PORT,4001);
		put(DRAGON_NETWORK_PRIMARY,true);
		put(DRAGON_NETWORK_PARTITION,Constants.DRAGON_PRIMARY_PARTITION);
		put(DRAGON_METRICS_ENABLED,true);
		put(DRAGON_METRICS_SAMPLE_PERIOD_MS,60*1000);
		put(DRAGON_METRICS_SAMPLE_HISTORY,1);
		put(DRAGON_NETWORK_HOSTS,new ArrayList<HashMap<String,?>>());
		put(DRAGON_EMBEDDING_ALGORITHM, "dragon.topology.RoundRobinEmbedding");
		put(DRAGON_EMBEDDING_CUSTOM_FILE, "embedding.yaml");
		put(DRAGON_RECYCLER_TUPLE_CAPACITY,1024);
		put(DRAGON_RECYCLER_TUPLE_EXPANSION,1024);
		put(DRAGON_RECYCLER_TUPLE_COMPACT,0.20);
		put(DRAGON_RECYCLER_TASK_CAPACITY,1024);
		put(DRAGON_RECYCLER_TASK_EXPANSION,1024);
		put(DRAGON_RECYCLER_TASK_COMPACT,0.20);
		put(DRAGON_COMMS_RESET_COUNT,128);
		put(DRAGON_COMMS_INCOMING_BUFFER_SIZE,1024);
		put(DRAGON_FAULTS_COMPONENT_TOLERANCE,3);
		put(DRAGON_JAVA_BIN,"java");
		put(DRAGON_PROCESSES_MAX,10);
	}
	
	/**
	 * Drop values that are relevant to daemon. This is so the user or application
	 * programmer can specifically override these values at run time when submitting
	 * the topology.
	 */
	public void drop() {
		remove(DRAGON_OUTPUT_BUFFER_SIZE);
		remove(DRAGON_INPUT_BUFFER_SIZE);
		remove(DRAGON_BASE_DIR);
		remove(DRAGON_PERSISTENCE_DIR);
		remove(DRAGON_JAR_DIR);
		remove(DRAGON_LOCALCLUSTER_THREADS);
		remove(DRAGON_ROUTER_INPUT_THREADS);
		remove(DRAGON_ROUTER_OUTPUT_THREADS);
		remove(DRAGON_ROUTER_INPUT_BUFFER_SIZE);
		remove(DRAGON_ROUTER_OUTPUT_BUFFER_SIZE);
		remove(DRAGON_METRICS_ENABLED);
		remove(DRAGON_METRICS_SAMPLE_PERIOD_MS);
		remove(DRAGON_RECYCLER_TUPLE_CAPACITY);
		remove(DRAGON_RECYCLER_TUPLE_EXPANSION);
		remove(DRAGON_RECYCLER_TUPLE_COMPACT);
		remove(DRAGON_RECYCLER_TASK_CAPACITY);
		remove(DRAGON_RECYCLER_TASK_EXPANSION);
		remove(DRAGON_RECYCLER_TASK_COMPACT);
	}
	
	//
	// Simple Getters
	//
	
	/**
	 * 
	 * @return the output buffer size.
	 */
	public int getDragonOutputBufferSize() {
		return (Integer)get(DRAGON_OUTPUT_BUFFER_SIZE);
	}
	
	/**
	 *  
	 * @return the input buffer size.
	 */
	public int getDragonInputBufferSize() {
		return (Integer)get(DRAGON_INPUT_BUFFER_SIZE);
	}
	
	/**
	 *  
	 * @return the base directory.
	 */
	public String getDragonBaseDir() {
		return (String)get(DRAGON_BASE_DIR);
	}
	
	/**
	 * 
	 * @return the persistence directory.
	 */
	public String getDragonPersistanceDir() {
		return (String)get(DRAGON_PERSISTENCE_DIR);
	}
	
	/**
	 *  
	 * @return the JAR directory.
	 */
	public String getDragonJarDir() {
		return (String)get(DRAGON_JAR_DIR);
	}
	
	/**
	 *  
	 * @return the local cluster threads.
	 */
	public int getDragonLocalclusterThreads() {
		return (Integer)get(DRAGON_LOCALCLUSTER_THREADS);
	}
	
	/**
	 * 
	 * @return the router input threads.
	 */
	public int getDragonRouterInputThreads() {
		return (Integer)get(DRAGON_ROUTER_INPUT_THREADS);
	}
	
	/**
	 * 
	 * @return the router output threads.
	 */
	public int getDragonRouterOutputThreads() {
		return (Integer)get(DRAGON_ROUTER_OUTPUT_THREADS);
	}
	
	/**
	 * 
	 * @return the router input buffer size.
	 */
	public int getDragonRouterInputBufferSize() {
		return (Integer)get(DRAGON_ROUTER_INPUT_BUFFER_SIZE);
	}
	
	/**
	 * 
	 * @return the router output buffer size.
	 */
	public int getDragonRouterOutputBufferSize() {
		return (Integer)get(DRAGON_ROUTER_OUTPUT_BUFFER_SIZE);
	}
	
	/**
	 * 
	 * @return the comms retry in milliseconds.
	 */
	public int getDragonCommsRetryMs() {
		return (Integer)get(DRAGON_COMMS_RETRY_MS);
	}
	
	/**
	 * 
	 * @return the comms retry attempts.
	 */
	public int getDragonCommsRetryAttempts() {
		return (Integer)get(DRAGON_COMMS_RETRY_ATTEMPTS);
	}
	
	/**
	 * 
	 * @return the network local host.
	 */
	public String getDragonNetworkLocalHost() {
		return (String)get(DRAGON_NETWORK_LOCAL_HOST);
	}
	
	/**
	 * 
	 * @return the network default service port.
	 */
	public int getDragonNetworkDefaultServicePort() {
		return (Integer)get(DRAGON_NETWORK_DEFAULT_SERVICE_PORT);
	}
	
	/**
	 * If the network local service port has been set in the configuration
	 * then it will be returned, otherwise the default setting will be returned.
	 * @return the network local service port.
	 */
	public int getDragonNetworkLocalServicePort() {
		if(containsKey(DRAGON_NETWORK_LOCAL_SERVICE_PORT))
		return (Integer)get(DRAGON_NETWORK_LOCAL_SERVICE_PORT);
		return getDragonNetworkDefaultServicePort();
	}
	
	/**
	 *  
	 * @return the network default data port.
	 */
	public int getDragonNetworkDefaultDataPort() {
		return (Integer)get(DRAGON_NETWORK_DEFAULT_DATA_PORT);
	}
	
	/**
	 * If the network local data port has been set then it will be
	 * returned, otherwise the default network local data port will
	 * be returned.
	 * @return the network local data port.
	 */
	public int getDragonNetworkLocalDataPort() {
		if(containsKey(DRAGON_NETWORK_LOCAL_DATA_PORT))
		return (Integer)get(DRAGON_NETWORK_LOCAL_DATA_PORT);
		return getDragonNetworkDefaultDataPort();
	}
	
	/**
	 * 
	 * @return true if the node is a primary node, false otherwise.
	 */
	public boolean getDragonNetworkPrimary() {
		return (Boolean)get(DRAGON_NETWORK_PRIMARY);
	}
	
	/**
	 * 
	 * @return the network partition id.
	 */
	public String getDragonNetworkPartition() {
		return (String)get(DRAGON_NETWORK_PARTITION);
	}
	
	/**
	 * 
	 * @return true if metrics are enabled, false otherwise.
	 */
	public boolean getDragonMetricsEnabled() {
		return (Boolean)get(DRAGON_METRICS_ENABLED);
	}
	
	/**
	 * 
	 * @return the metrics sample period in milliseconds.
	 */
	public int getDragonMetricsSamplePeriodMs() {
		return (int)get(DRAGON_METRICS_SAMPLE_PERIOD_MS);
	}
	
	/**
	 * 
	 * @return the metrics sample history.
	 */
	public int getDragonMetricsSampleHistory() {
		return (int)get(DRAGON_METRICS_SAMPLE_HISTORY);
	}
	
	/**
	 * 
	 * @return the network hosts.
	 */
	@SuppressWarnings("unchecked")
	public ArrayList<HashMap<String,?>> getDragonNetworkHosts(){
		return (ArrayList<HashMap<String,?>>)get(DRAGON_NETWORK_HOSTS);
	}
	
	/**
	 * 
	 * @return the embedding algorithm.
	 */
	public String getDragonEmbeddingAlgorithm() {
		return (String)get(DRAGON_EMBEDDING_ALGORITHM);
	}
	
	/**
	 * 
	 * @return the embedding custom file.
	 */
	public String getDragonEmbeddingCustomFile() {
		return (String)get(DRAGON_EMBEDDING_CUSTOM_FILE);
	}
	
	/**
	 * 
	 * @return the recycler tuple capacity.
	 */
	public int getDragonRecyclerTupleCapacity() {
		return (Integer)get(DRAGON_RECYCLER_TUPLE_CAPACITY);
	}
	
	/**
	 * 
	 * @return the recycler tuple expansion size.
	 */
	public int getDragonRecyclerTupleExpansion() {
		return (Integer)get(DRAGON_RECYCLER_TUPLE_EXPANSION);
	}
	
	/**
	 * 
	 * @return the recycler tuple compact fraction.
	 */
	public double getDragonRecyclerTupleCompact() {
		return (Double)get(DRAGON_RECYCLER_TUPLE_COMPACT);
	}
	
	/**
	 * 
	 * @return the recycler task capacity.
	 */
	public int getDragonRecyclerTaskCapacity() {
		return (Integer)get(DRAGON_RECYCLER_TASK_CAPACITY);
	}
	
	/**
	 * 
	 * @return the recycler task expansion size.
	 */
	public int getDragonRecyclerTaskExpansion() {
		return (Integer)get(DRAGON_RECYCLER_TASK_EXPANSION);
	}
	
	/**
	 * 
	 * @return the recycler task compact fraction.
	 */
	public double getDragonRecyclerTaskCompact() {
		return (Double)get(DRAGON_RECYCLER_TASK_COMPACT);
	}
	
	/**
	 * 
	 * @return the comms reset count.
	 */
	public int getDragonCommsResetCount() {
		return (Integer)get(DRAGON_COMMS_RESET_COUNT);
	}
	
	/**
	 * 
	 * @return the comms incoming buffer size.
	 */
	public int getDragonCommsIncomingBufferSize() {
		return (Integer)get(DRAGON_COMMS_INCOMING_BUFFER_SIZE);
	}
	
	/**
	 * 
	 * @return the faults component tolerance.
	 */
	public int getDragonFaultsComponentTolerance() {
		return (Integer)get(DRAGON_FAULTS_COMPONENT_TOLERANCE);
	}
	

	/**
	 * Returns the java binary path if provided in the configuration file,
	 * else returns what is found in the system property java.home with "/java"
	 * postfixed.
	 * @return the java bin
	 */
	public String getDragonJavaBin() {
		if(containsKey(DRAGON_JAVA_BIN)) {
			return (String)get(DRAGON_JAVA_BIN);
		} else {
			return (String)System.getProperty("java.home")+System.getProperty("file.separator")+"java";
		}
	}
		
	/**
	 * 
	 * @return the InfluxDB url
	 */
	public String getInfluxDBUrl() {
		return (String)get(INFLUXDB_URL);
	}
	
	/**
	 * 
	 * @return the InfluxDB authorization token
	 */
	public String getInfluxDBToken() {
		return (String)get(INFLUXDB_TOKEN);
	}
	
	/**
	 * 
	 * @return the InfluxDB bucket for data samples
	 */
	public String getInfluxDBBucket() {
		return (String)get(INFLUXDB_BUCKET);
	}
	
	/**
	 * 
	 * @return the InfluxDB organization for data samples
	 */
	public String getInfluxDBOrganization() {
		return (String)get(INFLUXDB_ORGANIZATION);
	}
	
	/**
	 * 
	 * @return the maximum number of bounded processes running at once
	 */
	public Integer getDragonProcessesMax() {
		return (Integer)get(DRAGON_PROCESSES_MAX);
	}
	
 	//
	// Advanced Getters
	//
	
	/**
	 * The jar path is the concatenation of base dir and jar dir.
	 * @return the jar path
	 */
	public String getJarPath() {
		return getDragonBaseDir()+"/"+getDragonJarDir();
	}
	
	/**
	 * The node descriptor for this node is a combination of other
	 * parameters in the configuration: network local host, network local data port,
	 * network local service port, network primary, network paritition.
	 * @return the node descriptor for this node
	 * @throws UnknownHostException if the node descriptor hostname cannot be looked up.
	 */
	public NodeDescriptor getLocalHost() throws UnknownHostException {
		return new NodeDescriptor(getDragonNetworkLocalHost(),
				getDragonNetworkLocalDataPort(),
				getDragonNetworkLocalServicePort(),
				getDragonNetworkPrimary(),
				getDragonNetworkPartition());
	}
	
	/**
	 * The conf network hosts list is translated into an array list of descriptors.
	 * @return an array list of descriptors for the hosts in the conf.
	 */
	public ArrayList<NodeDescriptor> getHosts(){
		ArrayList<NodeDescriptor> nodes = new ArrayList<NodeDescriptor>();
		ArrayList<HashMap<String,?>> hosts = getDragonNetworkHosts();
		for(int i=0;i<hosts.size();i++) {
			if(!hosts.get(i).containsKey("hostname")) {
				log.error("skipping host entry without hostname");
				continue;
			}
			String hostname = (String) hosts.get(i).get("hostname");
			int dport = getDragonNetworkDefaultDataPort();
			int sport = getDragonNetworkDefaultServicePort();
			boolean primary = true;
			String partition = Constants.DRAGON_PRIMARY_PARTITION;
			if(hosts.get(i).containsKey("dport")) dport = (Integer) hosts.get(i).get("dport");
			if(hosts.get(i).containsKey("sport")) sport = (Integer) hosts.get(i).get("sport");
			if(hosts.get(i).containsKey("primary")) primary = (Boolean) hosts.get(i).get("primary");
			if(hosts.get(i).containsKey("partition")) partition = (String) hosts.get(i).get("partition");
			try {
				nodes.add(new NodeDescriptor(hostname,
						dport,
						sport,
						primary,
						partition));
			} catch (UnknownHostException e) {
				log.error(hostname + " is not found");
			}
			
		}
		return nodes;
	}
}
