package dragon.metrics;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.time.Instant;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApi;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;

import dragon.LocalCluster;
import dragon.network.Node;

/**
 * Log metrics at a regular interval. Store metrics in memory and also
 * send to InfluxDB if that is configured.
 * @author aaron
 *
 */
public class Metrics extends Thread {
	private static Logger log = LogManager.getLogger(Metrics.class);
	
	/**
	 * The node that this metrics class belongs to.
	 */
	private Node node;
	
	/**
	 * The topology metric map for storing samples on topologies.
	 */
	private TopologyMetricMap samples;
	
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
	 * @param node
	 */
	public Metrics(Node node){
		samples=new TopologyMetricMap((int)node.getConf().getDragonMetricsSampleHistory());
		this.node = node;
		
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
		setName("metrics");
		log.info("starting up");
		if(node.getConf().getInfluxDBUrl()!=null) {
			log.info("using InfluxDB ["+node.getConf().getInfluxDBUrl()+"]");
			influxDBClient = InfluxDBClientFactory.create(node.getConf().getInfluxDBUrl(), node.getConf().getInfluxDBToken().toCharArray());
			writeApi = influxDBClient.getWriteApi();
		}
		Point point;
		while(!isInterrupted()){
			try {
				sleep((int)node.getConf().getDragonMetricsSamplePeriodMs());
			} catch (InterruptedException e) {
				log.info("shutting down");
			}
			synchronized(samples){
				
				if(writeApi!=null) {
					point = Point.measurement("gcTime").addTag("node", node.getComms().getMyNodeDesc().toString())
							.addField("value", gcTime()).time(Instant.now().toEpochMilli(), WritePrecision.MS);
					writeApi.writePoint(node.getConf().getInfluxDBBucket(), node.getConf().getInfluxDBOrganization(), point);
				}
				
				for(String topologyId : node.getLocalClusters().keySet()){
					log.info("sampling topology ["+topologyId+"]");
					LocalCluster localCluster = node.getLocalClusters().get(topologyId);
					for(String componentId : localCluster.getSpouts().keySet()){
						for(Integer taskId : localCluster.getSpouts().get(componentId).keySet()){
							Sample sample = new Sample(localCluster.getSpouts().get(componentId).get(taskId));
							if(influxDBClient!=null) {
								writeToInfluxDB(topologyId,componentId,taskId,sample);
							}
							samples.put(topologyId, componentId, taskId, sample);
						}
					}
					for(String componentId : localCluster.getBolts().keySet()){
						for(Integer taskId : localCluster.getBolts().get(componentId).keySet()){
							Sample sample = new Sample(localCluster.getBolts().get(componentId).get(taskId));
							if(influxDBClient!=null) {
								writeToInfluxDB(topologyId,componentId,taskId,sample);
							}
							samples.put(topologyId, componentId, taskId, sample);
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
	 * @param taskId
	 * @param sample
	 */
	private void writeToInfluxDB(String topologyId, String componentId, Integer taskId, Sample sample) {
		Point point;
		point = Point.measurement("outputQueueSize").addTag("node", node.getComms().getMyNodeDesc().toString())
				.addTag("topology", topologyId).addTag("component", componentId).addTag("instance", taskId.toString())
				.addField("value", sample.outputQueueSize).time(Instant.now().toEpochMilli(), WritePrecision.MS);
		writeApi.writePoint(node.getConf().getInfluxDBBucket(), node.getConf().getInfluxDBOrganization(), point);
		point = Point.measurement("inputQueueSize").addTag("node", node.getComms().getMyNodeDesc().toString())
				.addTag("topology", topologyId).addTag("component", componentId).addTag("instance", taskId.toString())
				.addField("value", sample.inputQueueSize).time(Instant.now().toEpochMilli(), WritePrecision.MS);
		writeApi.writePoint(node.getConf().getInfluxDBBucket(), node.getConf().getInfluxDBOrganization(), point);
		point = Point.measurement("processed").addTag("node", node.getComms().getMyNodeDesc().toString())
				.addTag("topology", topologyId).addTag("component", componentId).addTag("instance", taskId.toString())
				.addField("value", sample.processed).time(Instant.now().toEpochMilli(), WritePrecision.MS);
		writeApi.writePoint(node.getConf().getInfluxDBBucket(), node.getConf().getInfluxDBOrganization(), point);
		point = Point.measurement("emitted").addTag("node", node.getComms().getMyNodeDesc().toString())
				.addTag("topology", topologyId).addTag("component", componentId).addTag("instance", taskId.toString())
				.addField("value", sample.emitted).time(Instant.now().toEpochMilli(), WritePrecision.MS);
		writeApi.writePoint(node.getConf().getInfluxDBBucket(), node.getConf().getInfluxDBOrganization(), point);
		point = Point.measurement("transferred").addTag("node", node.getComms().getMyNodeDesc().toString())
				.addTag("topology", topologyId).addTag("component", componentId).addTag("instance", taskId.toString())
				.addField("value", sample.transferred).time(Instant.now().toEpochMilli(), WritePrecision.MS);
		writeApi.writePoint(node.getConf().getInfluxDBBucket(), node.getConf().getInfluxDBOrganization(), point);
	}

}
