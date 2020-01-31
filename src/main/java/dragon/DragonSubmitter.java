package dragon;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashSet;

import dragon.topology.IEmbeddingAlgo;
import dragon.utils.ReflectionUtils;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import dragon.network.NodeContext;
import dragon.network.NodeDescriptor;
import dragon.network.comms.DragonCommsException;
import dragon.network.comms.IComms;
import dragon.network.comms.TcpComms;
import dragon.network.messages.service.GetMetricsSMsg;
import dragon.network.messages.service.GetNodeContextSMsg;
import dragon.network.messages.service.HaltTopoErrorSMsg;
import dragon.network.messages.service.HaltTopoSMsg;
import dragon.network.messages.service.ListToposSMsg;
import dragon.network.messages.service.UploadJarSMsg;
import dragon.network.messages.service.GetMetricsErrorSMsg;
import dragon.network.messages.service.MetricsSMsg;
import dragon.network.messages.service.NodeContextSMsg;
import dragon.network.messages.service.ResumeTopoErrorSMsg;
import dragon.network.messages.service.ResumeTopoSMsg;
import dragon.network.messages.service.RunTopoErrorSMsg;
import dragon.network.messages.service.RunTopoSMsg;
import dragon.network.messages.service.ServiceDoneSMsg;
import dragon.network.messages.service.ServiceMessage;
import dragon.network.messages.service.TermTopoErrorSMsg;
import dragon.network.messages.service.TermTopoSMsg;
import dragon.network.messages.service.TopoListSMsg;
import dragon.network.messages.service.UploadJarFailedSMsg;
import dragon.topology.DragonTopology;

/**
 * Static class for providing stand-alone client commands.
 * @author aaron
 *
 */
public class DragonSubmitter {
	private static Logger log = LogManager.getLogger(DragonSubmitter.class);
	
	/**
	 * The daemon node to contact.
	 */
	public static NodeDescriptor node;
	
	/**
	 * The byte array of the topology JAR to submit.
	 */
	public static byte[] topologyJar;
	
	/**
	 * Comms layer for this client.
	 */
	private static IComms comms;
	
	/**
	 * Open the comms to the node, using the supplied conf.
	 * @param conf the configuration to use for the client
	 */
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
	
	/**
	 * Submit a topology to a given Dragon daemon.
	 * @param string the user defined name of the topology
	 * @param conf parameters that override the daemon specific settings for the topology
	 * @param topology the topology to submit
	 */
	public static void submitTopology(String string, Config conf, DragonTopology topology) {
		initComms(conf);
		log.info("requesting context from ["+node+"]");
		try {
			comms.sendServiceMsg(new GetNodeContextSMsg());
		} catch (DragonCommsException e1) {
			System.out.println("could not send get node context message");
			System.exit(-1);
		}
		ServiceMessage message=null;
		try {
			message = comms.receiveServiceMsg();
		} catch (InterruptedException e) {
			System.out.println("interrupted waiting for context");
			comms.close();
			System.exit(-1);
		}
		NodeContext context=null;
		switch(message.getType()) {
		case NODE_CONTEXT:
			NodeContextSMsg nc = (NodeContextSMsg) message;
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
			comms.sendServiceMsg(new UploadJarSMsg(string,topologyJar));
		} catch (DragonCommsException e1) {
			System.out.println("could not send upload jar message");
			System.exit(-1);
		}
		try {
			message = comms.receiveServiceMsg();
		} catch (InterruptedException e) {
			System.out.println("interrupted waiting for upload jar confirmation");
			comms.close();
			System.exit(-1);
		}
		UploadJarFailedSMsg te;
		switch(message.getType()) {
		case UPLOAD_JAR_FAILED:
			te = (UploadJarFailedSMsg) message;
			try {
				comms.sendServiceMsg(new ServiceDoneSMsg());
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
			comms.sendServiceMsg(new RunTopoSMsg(string,conf,topology));
		} catch (DragonCommsException e1) {
			System.out.println("could not send run topoology message");
			System.exit(-1);
		}
		try {
			message = comms.receiveServiceMsg();
		} catch (InterruptedException e) {
			log.info("interrupted waiting for run confirmation");
			comms.close();
			System.exit(-1);
		}
		RunTopoErrorSMsg rtem;
		switch(message.getType()){
		case RUN_TOPOLOGY_ERROR:
			rtem = (RunTopoErrorSMsg) message;
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
			comms.sendServiceMsg(new ServiceDoneSMsg());
		} catch (DragonCommsException e) {
			log.error("could not send service done message");
			System.exit(-1);
		}
		comms.close();
	}
	
	/**
	 * Extract metrics for the given topology on the supplied daemon node.
	 * @param conf
	 * @param topologyId
	 * @throws InterruptedException
	 * @throws DragonCommsException
	 */
	public static void getMetrics(Config conf,String topologyId) throws InterruptedException, DragonCommsException{
		initComms(conf);
		comms.sendServiceMsg(new GetMetricsSMsg(topologyId));
		ServiceMessage message = comms.receiveServiceMsg();
		switch(message.getType()){
		case METRICS:
			MetricsSMsg m = (MetricsSMsg) message;
			System.out.println(m.samples.toString());
			break;
		case GET_METRICS_ERROR:
			GetMetricsErrorSMsg e = (GetMetricsErrorSMsg) message;
			System.out.println(e.error);
			break;
		default:
			System.out.println("unexpected response: "+message.getType().name());
			comms.close();
			System.exit(-1);
		}
		comms.sendServiceMsg(new ServiceDoneSMsg());
		comms.close();
	}
	
	/**
	 * Terminate the topology, which removes it from daemon completely. Will wait for
	 * all existing data to be completely processed before terminating.
	 * @param conf
	 * @param topologyId
	 * @throws InterruptedException
	 * @throws DragonCommsException
	 */
	public static void terminateTopology(Config conf, String topologyId) throws InterruptedException, DragonCommsException {
		initComms(conf);
		comms.sendServiceMsg(new TermTopoSMsg(topologyId));
		ServiceMessage message = comms.receiveServiceMsg();
		TermTopoErrorSMsg tte;
		switch(message.getType()) {
		case TERMINATE_TOPOLOGY_ERROR:
			tte = (TermTopoErrorSMsg) message;
			System.out.println("terminate topology error ["+topologyId+"] "+tte.error);
		case TOPOLOGY_TERMINATED:
			System.out.println("topology terminated ["+topologyId+"]");
			break;
		default:
			System.out.println("unexpected response: "+message.getType().name());
			comms.close();
			System.exit(-1);
		}
		comms.sendServiceMsg(new ServiceDoneSMsg());
		comms.close();
	}
	
	/**
	 * Resume a halted topology.
	 * @param conf
	 * @param topologyId
	 * @throws InterruptedException
	 * @throws DragonCommsException
	 */
	public static void resumeTopology(Config conf, String topologyId) throws InterruptedException, DragonCommsException {
		initComms(conf);
		comms.sendServiceMsg(new ResumeTopoSMsg(topologyId));
		ServiceMessage message = comms.receiveServiceMsg();
		ResumeTopoErrorSMsg tte;
		switch(message.getType()) {
		case RESUME_TOPOLOGY_ERROR:
			tte = (ResumeTopoErrorSMsg) message;
			System.out.println("resume topology error ["+topologyId+"] "+tte.error);
		case TOPOLOGY_RESUMED:
			System.out.println("topology resumed ["+topologyId+"]");
			break;
		default:
			System.out.println("unexpected response: "+message.getType().name());
			comms.close();
			System.exit(-1);
		}
		comms.sendServiceMsg(new ServiceDoneSMsg());
		comms.close();
	}
	
	/**
	 * Halt a topology, which simply stops all processing with data still in place. Can be resumed.
	 * @param conf
	 * @param topologyId
	 * @throws InterruptedException
	 * @throws DragonCommsException
	 */
	public static void haltTopology(Config conf, String topologyId) throws InterruptedException, DragonCommsException {
		initComms(conf);
		comms.sendServiceMsg(new HaltTopoSMsg(topologyId));
		ServiceMessage message = comms.receiveServiceMsg();
		HaltTopoErrorSMsg tte;
		switch(message.getType()) {
		case HALT_TOPOLOGY_ERROR:
			tte = (HaltTopoErrorSMsg) message;
			System.out.println("halt topology error ["+topologyId+"] "+tte.error);
		case TOPOLOGY_HALTED:
			System.out.println("topology halted ["+topologyId+"]");
			break;
		default:
			System.out.println("unexpected response: "+message.getType().name());
			comms.close();
			System.exit(-1);
		}
		comms.sendServiceMsg(new ServiceDoneSMsg());
		comms.close();
	}
	
	/**
	 * List all of the topologies.
	 * @param conf
	 * @throws DragonCommsException
	 * @throws InterruptedException
	 */
	public static void listTopologies(Config conf) throws DragonCommsException, InterruptedException {
		initComms(conf);
		comms.sendServiceMsg(new ListToposSMsg());
		TopoListSMsg message = (TopoListSMsg) comms.receiveServiceMsg();
		comms.sendServiceMsg(new ServiceDoneSMsg());
		comms.close();
		HashSet<String> topologies = new HashSet<String>();
		for(String descid : message.descState.keySet()) {
			topologies.addAll(message.descState.get(descid).keySet());
		}
		if(topologies.isEmpty()) {
			System.out.println("there are no topologies running");
			return;
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
