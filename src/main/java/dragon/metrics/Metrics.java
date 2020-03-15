package dragon.metrics;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.time.Instant;
import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApi;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;

import dragon.Config;
import dragon.LocalCluster;
import dragon.network.NodeDescriptor;

/**
 * Log metrics at a regular interval. Store metrics in memory and also
 * send to InfluxDB if that is configured.
 * @author aaron
 *
 */
public class Metrics extends Thread {
	private static Logger log = LogManager.getLogger(Metrics.class);
	
	/**
	 * 
	 */
	private final Config conf;
	
	/**
	 * 
	 */
	private final NodeDescriptor desc;
	
	/**
	 * 
	 */
	private final HashMap<String, LocalCluster> localClusters;
	
	/**
	 * The topology metric map for storing samples on topologies.
	 */
	private final TopologyMetricMap samples;
	
	/**
	 * The InfluxDBClient, null if none given in conf. 
	 */
	private InfluxDBClient influxDBClient;
	
	/**
	 * For writing to the InfluxDBClient.
	 */
	private WriteApi writeApi;
	
	/**
	 * When initialized it will open a DB connection to InfluxDB if
	 * available. The connection will remain open until metrics shuts down.
	 * @param conf
	 * @param localClusters
	 * @param desc
	 */
	public Metrics(Config conf,HashMap<String, LocalCluster> localClusters,NodeDescriptor desc){
		this.conf=conf;
		this.localClusters=localClusters;
		this.desc=desc;
		samples=new TopologyMetricMap((int)conf.getDragonMetricsSampleHistory());
		setName("metrics");
		start();
	}
	
	/**
	 * Return the samples for a given topology.
	 * @param topologyId
	 * @return
	 */
	public ComponentMetricMap getMetrics(String topologyId){
		log.debug("gettings samples for ["+topologyId+"]");
		synchronized(samples){
			return samples.get(topologyId);
		}
	}
	
	/**
	 * Compute the total accumulated time that the garbage collector
	 * has been working for this JVM.
	 * @return
	 */
	private long gcTime() {
		long gcTime = 0;
	    for (GarbageCollectorMXBean garbageCollectorMXBean : ManagementFactory.getGarbageCollectorMXBeans()) {
	        gcTime += garbageCollectorMXBean.getCollectionTime();
	    }
	    return gcTime;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Thread#run()
	 */
	@Override
	public void run(){
		log.info("starting up");
		if(conf.getInfluxDBUrl()!=null) {
			log.info("using InfluxDB ["+conf.getInfluxDBUrl()+"]");
			influxDBClient = InfluxDBClientFactory.create(conf.getInfluxDBUrl(),conf.getInfluxDBToken().toCharArray());
			writeApi = influxDBClient.getWriteApi();
		}
		Point point;
		while(!isInterrupted()){
			try {
				sleep((int)conf.getDragonMetricsSamplePeriodMs());
			} catch (InterruptedException e) {
				log.info("shutting down");
			}
			synchronized(samples){
				
				if(writeApi!=null) {
					point = Point.measurement("gcTime").addTag("node", desc.toString())
							.addField("value", gcTime()).time(Instant.now().toEpochMilli(), WritePrecision.MS);
					writeApi.writePoint(conf.getInfluxDBBucket(), conf.getInfluxDBOrganization(), point);
				}
				
				for(String topologyId : localClusters.keySet()){
					log.info("sampling topology ["+topologyId+"]");
					LocalCluster localCluster = localClusters.get(topologyId);
					for(String componentId : localCluster.getSpouts().keySet()){
						for(Integer taskIndex : localCluster.getSpouts().get(componentId).keySet()){
							Sample sample = new Sample(localCluster.getSpouts().get(componentId).get(taskIndex));
							if(influxDBClient!=null) {
								writeToInfluxDB(topologyId,componentId,taskIndex,sample);
							}
							samples.put(topologyId, componentId, taskIndex, sample);
						}
					}
					for(String componentId : localCluster.getBolts().keySet()){
						for(Integer taskIndex : localCluster.getBolts().get(componentId).keySet()){
							Sample sample = new Sample(localCluster.getBolts().get(componentId).get(taskIndex));
							if(influxDBClient!=null) {
								writeToInfluxDB(topologyId,componentId,taskIndex,sample);
							}
							samples.put(topologyId, componentId, taskIndex, sample);
						}
					}
				}
			}
		}
		if(influxDBClient!=null) {
			log.info("closing connection to InfluxDB");
			influxDBClient.close();
		}
		log.info("shutting down");
	}
	
	/**
	 * Send a sample to InfluxDB
	 * @param topologyId
	 * @param componentId
	 * @param taskIndex
	 * @param sample
	 */
	private void writeToInfluxDB(String topologyId, String componentId, Integer taskIndex, Sample sample) {
		Point point;
		point = Point.measurement("outputQueueSize").addTag("node", desc.toString())
				.addTag("topology", topologyId).addTag("component", componentId).addTag("instance", taskIndex.toString())
				.addField("value", sample.outputQueueSize).time(Instant.now().toEpochMilli(), WritePrecision.MS);
		writeApi.writePoint(conf.getInfluxDBBucket(), conf.getInfluxDBOrganization(), point);
		point = Point.measurement("inputQueueSize").addTag("node", desc.toString())
				.addTag("topology", topologyId).addTag("component", componentId).addTag("instance", taskIndex.toString())
				.addField("value", sample.inputQueueSize).time(Instant.now().toEpochMilli(), WritePrecision.MS);
		writeApi.writePoint(conf.getInfluxDBBucket(), conf.getInfluxDBOrganization(), point);
		point = Point.measurement("processed").addTag("node", desc.toString())
				.addTag("topology", topologyId).addTag("component", componentId).addTag("instance", taskIndex.toString())
				.addField("value", sample.processed).time(Instant.now().toEpochMilli(), WritePrecision.MS);
		writeApi.writePoint(conf.getInfluxDBBucket(), conf.getInfluxDBOrganization(), point);
		point = Point.measurement("emitted").addTag("node", desc.toString())
				.addTag("topology", topologyId).addTag("component", componentId).addTag("instance", taskIndex.toString())
				.addField("value", sample.emitted).time(Instant.now().toEpochMilli(), WritePrecision.MS);
		writeApi.writePoint(conf.getInfluxDBBucket(), conf.getInfluxDBOrganization(), point);
		point = Point.measurement("transferred").addTag("node", desc.toString())
				.addTag("topology", topologyId).addTag("component", componentId).addTag("instance", taskIndex.toString())
				.addField("value", sample.transferred).time(Instant.now().toEpochMilli(), WritePrecision.MS);
		writeApi.writePoint(conf.getInfluxDBBucket(), conf.getInfluxDBOrganization(), point);
	}

}
