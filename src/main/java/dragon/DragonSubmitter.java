package dragon;

import java.io.IOException;
import java.net.UnknownHostException;

import dragon.topology.IEmbeddingAlgo;
import dragon.utils.ReflectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.network.NodeContext;
import dragon.network.NodeDescriptor;
import dragon.network.comms.IComms;
import dragon.network.comms.TcpComms;
import dragon.network.messages.service.GetMetricsMessage;
import dragon.network.messages.service.GetNodeContextMessage;
import dragon.network.messages.service.UploadJarMessage;
import dragon.network.messages.service.GetMetricsErrorMessage;
import dragon.network.messages.service.MetricsMessage;
import dragon.network.messages.service.NodeContextMessage;
import dragon.network.messages.service.RunTopologyErrorMessage;
import dragon.network.messages.service.RunTopologyMessage;
import dragon.network.messages.service.ServiceDoneMessage;
import dragon.network.messages.service.ServiceMessage;
import dragon.network.messages.service.TerminateTopologyErrorMessage;
import dragon.network.messages.service.TerminateTopologyMessage;
import dragon.network.messages.service.UploadJarFailedMessage;
import dragon.topology.DragonTopology;

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
			comms.close();
			throw new RuntimeException("could not obtain node context");
		}
		
		log.debug("received context  ["+context+"]");

		IEmbeddingAlgo embedding = ReflectionUtils.newInstance((String)conf.get(Config.DRAGON_EMBEDDING_ALGORITHM));
		topology.embedTopology(embedding, context, conf);
		
		
		log.debug("uploading jar file to ["+node+"]");
		comms.sendServiceMessage(new UploadJarMessage(string,topologyJar));
		message = comms.receiveServiceMessage();
		UploadJarFailedMessage te;
		switch(message.getType()) {
		case UPLOAD_JAR_FAILED:
			te = (UploadJarFailedMessage) message;
			comms.sendServiceMessage(new ServiceDoneMessage());
			comms.close();
			log.error("uploading jar failed for ["+string+"]: "+te.error);
			System.exit(-1);
		case UPLOAD_JAR_SUCCESS:
			break;
		default:
			log.error("unexpected response: "+message.getType().name());
			comms.close();
			throw new RuntimeException("could not upload jar file");
		}
		
		log.debug("running topology on ["+node+"]");
		comms.sendServiceMessage(new RunTopologyMessage(string,conf,topology));
		message = comms.receiveServiceMessage();
		RunTopologyErrorMessage rtem;
		switch(message.getType()){
		case RUN_TOPOLOGY_ERROR:
			rtem = (RunTopologyErrorMessage) message;
			log.error("run topology error for ["+string+"]: "+rtem.error);
			break;
		case TOPOLOGY_RUNNING:
			log.info("topology ["+string+"] running");
			break;
		default:
			log.error("unexpected response: "+message.getType().name());
			comms.close();
			throw new RuntimeException("could not run the topology");
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
		case GET_METRICS_ERROR:
			GetMetricsErrorMessage e = (GetMetricsErrorMessage) message;
			log.error(e.error);
			break;
		default:
			log.error("unexpected response: "+message.getType().name());
			comms.close();
			throw new RuntimeException("could not get metrics");
		}
		comms.sendServiceMessage(new ServiceDoneMessage());
		comms.close();
	}
	
	public static void terminateTopology(Config conf, String topologyId) {
		initComms(conf);
		comms.sendServiceMessage(new TerminateTopologyMessage(topologyId));
		ServiceMessage message = comms.receiveServiceMessage();
		TerminateTopologyErrorMessage tte;
		switch(message.getType()) {
		case TERMINATE_TOPOLOGY_ERROR:
			tte = (TerminateTopologyErrorMessage) message;
			log.error("terminate topology error ["+topologyId+"] "+tte.error);
		case TOPOLOGY_TERMINATED:
			log.info("topology terminated ["+topologyId+"]");
			break;
		default:
			log.error("unexpected response: "+message.getType().name());
			comms.close();
			throw new RuntimeException("could not terminate topology");
		}
		comms.sendServiceMessage(new ServiceDoneMessage());
		comms.close();
	}

}
