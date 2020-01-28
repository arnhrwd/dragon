package dragon.metrics;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.time.Instant;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
	private static Log log = LogFactory.getLog(Metrics.class);
	private Node node;
	
	private TopologyMetricMap samples;
	private InfluxDBClient influxDBClient;
	private WriteApi writeApi;
	
	public Metrics(Node node){
		log.debug("metrics initialized");
		samples=new TopologyMetricMap((int)node.getConf().getDragonMetricsSampleHistory());
		this.node = node;
		if(node.getConf().getInfluxDBUrl()!=null) {
			log.info("using InfluxDB ["+node.getConf().getInfluxDBUrl()+"]");
			influxDBClient = InfluxDBClientFactory.create(node.getConf().getInfluxDBUrl(), node.getConf().getInfluxDBToken().toCharArray());
			writeApi = influxDBClient.getWriteApi();
		}
	}
	
	public ComponentMetricMap getMetrics(String topologyId){
		log.debug("gettings samples for ["+topologyId+"]");
		synchronized(samples){
			return samples.get(topologyId);
		}
	}
	
	private long gcTime() {
		long gcTime = 0;
	    for (GarbageCollectorMXBean garbageCollectorMXBean : ManagementFactory.getGarbageCollectorMXBeans()) {
	        gcTime += garbageCollectorMXBean.getCollectionTime();
	    }
	    return gcTime;
	}
	
	@Override
	public void run(){
		Point point;
		while(!isInterrupted()){
			try {
				sleep((int)node.getConf().getDragonMetricsSamplePeriodMs());
			} catch (InterruptedException e) {
				log.info("shutting down");
			}
			synchronized(samples){
				
				point = Point.measurement("gcTime").addTag("node", node.getComms().getMyNodeDesc().toString())
						.addField("value", gcTime()).time(Instant.now().toEpochMilli(), WritePrecision.MS);
				writeApi.writePoint(node.getConf().getInfluxDBBucket(), node.getConf().getInfluxDBOrganization(), point);
				
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
		if(influxDBClient!=null) influxDBClient.close();
	}
	
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
