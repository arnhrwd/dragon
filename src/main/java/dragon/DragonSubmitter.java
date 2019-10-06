package dragon;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashSet;

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
import dragon.network.messages.service.ListTopologiesMessage;
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
import dragon.network.messages.service.TopologyListMessage;
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
			System.out.println("unknown host ["+node+"]");
			System.exit(1);
		} catch (IOException e) {
			System.out.println("ioexception: "+e.toString());
			System.exit(1);
		}
	}
	public static void submitTopology(String string, Config conf, DragonTopology topology) {
		initComms(conf);
		log.info("requesting context from ["+node+"]");
		try {
			comms.sendServiceMessage(new GetNodeContextMessage());
		} catch (DragonCommsException e1) {
			System.out.println("could not send get node context message");
			System.exit(-1);
		}
		ServiceMessage message=null;
		try {
			message = comms.receiveServiceMessage();
		} catch (InterruptedException e) {
			System.out.println("interrupted waiting for context");
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
			System.out.println("unexpected response: "+message.getType().name());
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
			System.out.println("could not send upload jar message");
			System.exit(-1);
		}
		try {
			message = comms.receiveServiceMessage();
		} catch (InterruptedException e) {
			System.out.println("interrupted waiting for upload jar confirmation");
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
				System.out.println("could not send service done message");
				System.exit(-1);
			}
			comms.close();
			System.out.println("uploading jar failed for ["+string+"]: "+te.error);
			System.exit(-1);
		case UPLOAD_JAR_SUCCESS:
			break;
		default:
			System.out.println("unexpected response: "+message.getType().name());
			comms.close();
			System.exit(-1);
		}
		
		log.debug("running topology on ["+node+"]");
		try {
			comms.sendServiceMessage(new RunTopologyMessage(string,conf,topology));
		} catch (DragonCommsException e1) {
			System.out.println("could not send run topoology message");
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
			System.out.println("run topology error for ["+string+"]: "+rtem.error);
			break;
		case TOPOLOGY_RUNNING:
			System.out.println("topology ["+string+"] running");
			break;
		default:
			System.out.println("unexpected response: "+message.getType().name());
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
			System.out.println(m.samples.toString());
			break;
		case GET_METRICS_ERROR:
			GetMetricsErrorMessage e = (GetMetricsErrorMessage) message;
			System.out.println(e.error);
			break;
		default:
			System.out.println("unexpected response: "+message.getType().name());
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
			System.out.println("terminate topology error ["+topologyId+"] "+tte.error);
		case TOPOLOGY_TERMINATED:
			System.out.println("topology terminated ["+topologyId+"]");
			break;
		default:
			System.out.println("unexpected response: "+message.getType().name());
			comms.close();
			System.exit(-1);
		}
		comms.sendServiceMessage(new ServiceDoneMessage());
		comms.close();
	}
	
	public static void listTopologies(Config conf) throws DragonCommsException, InterruptedException {
		initComms(conf);
		comms.sendServiceMessage(new ListTopologiesMessage());
		TopologyListMessage message = (TopologyListMessage) comms.receiveServiceMessage();
		comms.sendServiceMessage(new ServiceDoneMessage());
		comms.close();
		HashSet<String> topologies = new HashSet<String>();
		for(String descid : message.descState.keySet()) {
			topologies.addAll(message.descState.get(descid).keySet());
		}
		for(String topologyId: topologies) {
			System.out.println("\n# "+topologyId+"\n");
			boolean hasErrors=false;
			for(String descid : message.descState.keySet()) {
				System.out.println("- "+descid+" "+message.descState.get(descid).get(topologyId));
				if(message.descErrors.get(descid).containsKey(topologyId)) {
					for(String cid : message.descErrors.get(descid).get(topologyId).keySet()) {
						hasErrors=true;
						for(ComponentError ce : message.descErrors.get(descid).get(topologyId).get(cid)) {
							System.out.println("    - "+ce.message);
						}
					}
				}
			}
			if(hasErrors) {
				System.out.println("\n## Stack traces\n");
				for(String descid : message.descState.keySet()) {
					if(message.descErrors.get(descid).containsKey(topologyId)) {
						System.out.println("### "+descid+"\n");
						for(String cid : message.descErrors.get(descid).get(topologyId).keySet()) {
							for(ComponentError ce : message.descErrors.get(descid).get(topologyId).get(cid)) {
								System.out.println(ce.message);
								System.out.println(ce.stackTrace+"\n");
							}
						}
					}
				}
			}
			
		}
	}

}
