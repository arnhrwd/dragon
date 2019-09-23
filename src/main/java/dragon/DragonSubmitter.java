package dragon;

import java.io.IOException;
import java.net.UnknownHostException;

import dragon.topology.IEmbeddingAlgo;
import dragon.utils.ReflectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.network.NodeContext;
import dragon.network.NodeDescriptor;
import dragon.network.comms.DragonCommsException;
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
			log.error("unknown host ["+node+"]");
			System.exit(1);
		} catch (IOException e) {
			log.error("ioexception: "+e.toString());
			System.exit(1);
		}
	}
	public static void submitTopology(String string, Config conf, DragonTopology topology) {
		initComms(conf);
		log.info("requesting context from ["+node+"]");
		try {
			comms.sendServiceMessage(new GetNodeContextMessage());
		} catch (DragonCommsException e1) {
			log.error("could not send get node context message");
			System.exit(-1);
		}
		ServiceMessage message=null;
		try {
			message = comms.receiveServiceMessage();
		} catch (InterruptedException e) {
			log.info("interrupted waiting for context");
			comms.close();
			System.exit(-1);
		}
		NodeContext context=null;
		switch(message.getType()) {
		case NODE_CONTEXT:
			NodeContextMessage nc = (NodeContextMessage) message;
			context=nc.context;
			break;
		default:
			log.error("unexpected response: "+message.getType().name());
			comms.close();
			System.exit(-1);
		}
		
		log.info("received context  ["+context+"]");

		IEmbeddingAlgo embedding = ReflectionUtils.newInstance(conf.getDragonEmbeddingAlgorithm());
		topology.embedTopology(embedding, context, conf);
		
		
		log.info("uploading jar file to ["+node+"]");
		try {
			comms.sendServiceMessage(new UploadJarMessage(string,topologyJar));
		} catch (DragonCommsException e1) {
			log.error("could not send upload jar message");
			System.exit(-1);
		}
		try {
			message = comms.receiveServiceMessage();
		} catch (InterruptedException e) {
			log.info("interrupted waiting for upload jar confirmation");
			comms.close();
			System.exit(-1);
		}
		UploadJarFailedMessage te;
		switch(message.getType()) {
		case UPLOAD_JAR_FAILED:
			te = (UploadJarFailedMessage) message;
			try {
				comms.sendServiceMessage(new ServiceDoneMessage());
			} catch (DragonCommsException e1) {
				log.error("could not send service done message");
				System.exit(-1);
			}
			comms.close();
			log.error("uploading jar failed for ["+string+"]: "+te.error);
			System.exit(-1);
		case UPLOAD_JAR_SUCCESS:
			break;
		default:
			log.error("unexpected response: "+message.getType().name());
			comms.close();
			System.exit(-1);
		}
		
		log.debug("running topology on ["+node+"]");
		try {
			comms.sendServiceMessage(new RunTopologyMessage(string,conf,topology));
		} catch (DragonCommsException e1) {
			log.error("could not send run topoology message");
			System.exit(-1);
		}
		try {
			message = comms.receiveServiceMessage();
		} catch (InterruptedException e) {
			log.info("interrupted waiting for run confirmation");
			comms.close();
			System.exit(-1);
		}
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
			System.exit(-1);
		}
		try {
			comms.sendServiceMessage(new ServiceDoneMessage());
		} catch (DragonCommsException e) {
			log.error("could not send service done message");
			System.exit(-1);
		}
		comms.close();
	}
	
	public static void getMetrics(Config conf,String topologyId) throws InterruptedException, DragonCommsException{
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
			System.exit(-1);
		}
		comms.sendServiceMessage(new ServiceDoneMessage());
		comms.close();
	}
	
	public static void terminateTopology(Config conf, String topologyId) throws InterruptedException, DragonCommsException {
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
			System.exit(-1);
		}
		comms.sendServiceMessage(new ServiceDoneMessage());
		comms.close();
	}

}
