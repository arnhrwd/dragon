package dragon;

import java.io.IOException;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.network.NodeContext;
import dragon.network.NodeDescriptor;
import dragon.network.comms.IComms;
import dragon.network.comms.TcpComms;
import dragon.network.messages.service.GetMetricsMessage;
import dragon.network.messages.service.GetNodeContextMessage;
import dragon.network.messages.service.MetricsErrorMessage;
import dragon.network.messages.service.MetricsMessage;
import dragon.network.messages.service.NodeContextMessage;
import dragon.network.messages.service.RunTopologyMessage;
import dragon.network.messages.service.ServiceDoneMessage;
import dragon.network.messages.service.ServiceMessage;
import dragon.topology.DragonTopology;
import dragon.topology.RoundRobinEmbedding;

public class DragonSubmitter {
	private static Log log = LogFactory.getLog(DragonSubmitter.class);
	public static NodeDescriptor node;
	public static byte[] topologyJar;
	private static IComms comms;
	
	private static void initComms(Config conf){
		comms=null;
		try {
			comms = new TcpComms(conf);
			comms.open(node);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(-1);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(-1);
		}
	}
	public static void submitTopology(String string, Config conf, DragonTopology topology){
		initComms(conf);
		log.debug("requesting context from ["+node+"]");
		comms.sendServiceMessage(new GetNodeContextMessage());
		ServiceMessage message = comms.receiveServiceMessage();
		NodeContext context;
		switch(message.getType()) {
		case NODE_CONTEXT:
			NodeContextMessage nc = (NodeContextMessage) message;
			context=nc.context;
			break;
		default:
			log.error("unexpected response: "+message.getType().name());
			throw new RuntimeException("could not obtain node context");
		}
		
		log.debug("received context  ["+context+"]");
		topology.embedTopology(new RoundRobinEmbedding(), context);
		
		
		log.debug("submitting topology to ["+node+"]");
		comms.sendServiceMessage(new RunTopologyMessage(string,conf,topology,topologyJar));
		message = comms.receiveServiceMessage();
		switch(message.getType()){
		case TOPOLOGY_EXISTS:
			log.error("topology ["+string+"] already exists");
			break;
		case TOPOLOGY_SUBMITTED:
			log.info("topology ["+string+"] submitted");
			break;
		default:
			log.error("unexpected response: "+message.getType().name());
		}
		comms.sendServiceMessage(new ServiceDoneMessage());
		comms.close();
	}
	
	public static void getMetrics(Config conf,String topologyId){
		initComms(conf);
		comms.sendServiceMessage(new GetMetricsMessage(topologyId));
		ServiceMessage message = comms.receiveServiceMessage();
		switch(message.getType()){
		case METRICS:
			MetricsMessage m = (MetricsMessage) message;
			log.info(m.samples.toString());
			break;
		case METRICS_ERROR:
			MetricsErrorMessage e = (MetricsErrorMessage) message;
			log.error(e.error);
			break;
		}
		comms.sendServiceMessage(new ServiceDoneMessage());
		comms.close();
	}

}
