package dragon;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.parser.ParseException;

import dragon.metrics.Sample;
import dragon.network.NodeContext;
import dragon.network.NodeDescriptor;
import dragon.network.NodeStatus;
import dragon.network.comms.DragonCommsException;
import dragon.network.comms.IComms;
import dragon.network.comms.TcpComms;
import dragon.network.messages.service.ServiceDoneSMsg;
import dragon.network.messages.service.ServiceMessage;
import dragon.network.messages.service.allocpart.AllocPartErrorSMsg;
import dragon.network.messages.service.allocpart.AllocPartSMsg;
import dragon.network.messages.service.dealloc.DeallocPartErrorSMsg;
import dragon.network.messages.service.dealloc.DeallocPartSMsg;
import dragon.network.messages.service.getmetrics.GetMetricsErrorSMsg;
import dragon.network.messages.service.getmetrics.GetMetricsSMsg;
import dragon.network.messages.service.getmetrics.MetricsSMsg;
import dragon.network.messages.service.getnodecontext.GetNodeContextSMsg;
import dragon.network.messages.service.getnodecontext.NodeContextSMsg;
import dragon.network.messages.service.getstatus.GetStatusSMsg;
import dragon.network.messages.service.getstatus.StatusSMsg;
import dragon.network.messages.service.halttopo.HaltTopoErrorSMsg;
import dragon.network.messages.service.halttopo.HaltTopoSMsg;
import dragon.network.messages.service.listtopo.ListToposSMsg;
import dragon.network.messages.service.listtopo.TopoListSMsg;
import dragon.network.messages.service.resumetopo.ResumeTopoErrorSMsg;
import dragon.network.messages.service.resumetopo.ResumeTopoSMsg;
import dragon.network.messages.service.runtopo.RunTopoErrorSMsg;
import dragon.network.messages.service.runtopo.RunTopoSMsg;
import dragon.network.messages.service.termtopo.TermTopoErrorSMsg;
import dragon.network.messages.service.termtopo.TermTopoSMsg;
import dragon.network.messages.service.uploadjar.UploadJarFailedSMsg;
import dragon.network.messages.service.uploadjar.UploadJarSMsg;
import dragon.network.messages.service.progress.ProgressSMsg;
import dragon.topology.DragonTopology;
import dragon.topology.IEmbeddingAlgo;
import dragon.utils.ReflectionUtils;
import io.bretty.console.tree.PrintableTreeNode;
import io.bretty.console.tree.TreePrinter;

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
	 * For displaying information on the console
	 */
	private static class TreeNode implements PrintableTreeNode {

		private String name;
		private List<TreeNode> children;

		public TreeNode(String name) {
			this.name = name;
			this.children = new ArrayList<>();
		}

		public void addChild(TreeNode child){
			this.children.add(child);
		}
		
		public String name() {
			// return the name of the node that you wish to print later
			return this.name;
		}

		public List<TreeNode> children() {
			// return the list of children of this node
			return this.children;
		}
	}
	
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
	 * Receive service message responses and print progress messages
	 * if they arrive.
	 * @return the first message that is not a progress message
	 */
	private static ServiceMessage skipProgress() {
		ServiceMessage message=null;
		while(message==null || message.getType()==ServiceMessage.ServiceMessageType.PROGRESS) {
			try {
				message = comms.receiveServiceMsg();
			} catch (InterruptedException e) {
				System.out.println("interrupted waiting for context");
				comms.close();
				System.exit(-1);
			}
			if(message.getType()==ServiceMessage.ServiceMessageType.PROGRESS) {
				System.out.println(((ProgressSMsg)message).msg);
			}
		}
		return message;
	}
	
	/**
	 * Submit a topology to a given Dragon daemon.
	 * @param string the user defined name of the topology
	 * @param conf parameters that override the daemon specific settings for the topology
	 * @param topology the topology to submit
	 */
	public static void submitTopology(String string, Config conf, DragonTopology topology) {
		System.out.println("connecting to dragon daemon: ["+node+"]");
		initComms(conf);
		System.out.println("requesting context from ["+node+"]");
		try {
			comms.sendServiceMsg(new GetNodeContextSMsg());
		} catch (DragonCommsException e1) {
			System.out.println("could not send get node context message");
			System.exit(-1);
		}
		ServiceMessage message=skipProgress();
		NodeContext context = ((NodeContextSMsg)message).context;
		log.debug("received context  ["+context+"]");
		IEmbeddingAlgo embedding = ReflectionUtils.newInstance(conf.getDragonEmbeddingAlgorithm());
		topology.embedTopology(embedding, context, conf);
		System.out.println("uploading jar file to ["+node+"]");
		try {
			comms.sendServiceMsg(new UploadJarSMsg(string,topologyJar));
		} catch (DragonCommsException e1) {
			System.out.println("could not send upload jar message");
			System.exit(-1);
		}
		message = skipProgress();
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
		System.out.println("running topology on ["+node+"]");
		try {
			comms.sendServiceMsg(new RunTopoSMsg(string,conf,topology));
		} catch (DragonCommsException e1) {
			System.out.println("could not send run topoology message");
			System.exit(-1);
		}
		message = skipProgress();
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
		ServiceMessage message = skipProgress();
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
		System.out.println("terminating topology ["+topologyId+"]");
		comms.sendServiceMsg(new TermTopoSMsg(topologyId,false));
		ServiceMessage message = skipProgress();
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
		ServiceMessage message = skipProgress();
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
		ServiceMessage message = skipProgress();
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
		TopoListSMsg message = (TopoListSMsg) skipProgress();
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
		TreeNode dragon = new TreeNode("<dragon>");
		for(String topologyId: topologies) {
			TreeNode topo = new TreeNode("["+topologyId+"]");
			dragon.addChild(topo);
			for(String descid : message.descState.keySet()) {
				TreeNode machine= new TreeNode("["+descid+"] "+message.descState.get(descid).get(topologyId));
				topo.addChild(machine);
				// list components
				if(message.descComponents.get(descid).containsKey(topologyId)) {
					for(String cid : message.descComponents.get(descid).get(topologyId)) {
						Sample metrics = message.descMetrics.get(topologyId).get(cid);
						TreeNode component = new TreeNode("["+cid+"] "+"emt:"+metrics.emitted+",exe:"+metrics.processed+",trf:"+metrics.transferred);
						machine.addChild(component);
						// list errors
						if(message.descErrors.get(descid).containsKey(topologyId)) {
							if(message.descErrors.get(descid).get(topologyId).containsKey(cid)) {
								for(ComponentError ce : message.descErrors.get(descid).get(topologyId).get(cid)) {
									TreeNode error=new TreeNode(ce.message);
									component.addChild(error);
									for(String line : ce.stackTrace.split("\n")) {
										TreeNode errline=new TreeNode(line);
										error.addChild(errline);
									}
								}
							}
						}
					}
				}
			}
		}
		String output = TreePrinter.toString(dragon);
		System.out.println(output);
	}
	
	public static void getStatus(Config conf) throws DragonCommsException, InterruptedException {
		initComms(conf);
		comms.sendServiceMsg(new GetStatusSMsg());
		StatusSMsg message = (StatusSMsg) skipProgress();
		comms.sendServiceMsg(new ServiceDoneSMsg());
		comms.close();
		ArrayList<NodeStatus> dragonStatus = message.dragonStatus;
		if(dragonStatus.isEmpty()) {
			System.out.println("there are no topologies running");
			return;
		}
		TreeNode dragon = new TreeNode("<dragon>");
		for(NodeStatus nodeStatus : dragonStatus) {
			TreeNode node = new TreeNode("["+nodeStatus.desc.toString()+"] "+(nodeStatus.primary?"(primary) ":"")+nodeStatus.state.name()+" at "+ (new Date(nodeStatus.timestamp)).toString());
			dragon.addChild(node);
			TreeNode partition = new TreeNode("partition: "+nodeStatus.partitionId);
			node.addChild(partition);
			if(nodeStatus.context.size()>0) {
				TreeNode context = new TreeNode("<context>");
				node.addChild(context);
				for(String desc : nodeStatus.context.keySet()) {
					TreeNode contextdesc = new TreeNode(desc);
					context.addChild(contextdesc);
				}
			}
			if(!nodeStatus.localClusterStates.isEmpty()) {
				TreeNode topos = new TreeNode("<topologies>");
				node.addChild(topos);
				for(String topo : nodeStatus.localClusterStates.keySet()) {
					TreeNode toponode = new TreeNode("["+topo+"] "+nodeStatus.localClusterStates.get(topo).name());
					topos.addChild(toponode);
				}
			}
		}
		String output = TreePrinter.toString(dragon);
		System.out.println(output);
		
	}
	
	public static void allocatePartition(Config conf,List<String> argList) throws ParseException, 
			DragonCommsException, InterruptedException {
		initComms(conf);
		if(argList.size()!=3) {
			throw new ParseException("required arguments: PARTITIONID NUMBER STRATEGY\n"+
					"where strategy is: each|uniform|balanced");
		}
		String partitionId = argList.get(0);
		Integer number = Integer.parseInt(argList.get(1));
		AllocPartSMsg.Strategy strat;
		switch(argList.get(2)) {
		case "each":
			strat=AllocPartSMsg.Strategy.EACH;
			break;
		case "uniform":
			strat=AllocPartSMsg.Strategy.UNIFORM;
			break;
		case "balanced":
			strat=AllocPartSMsg.Strategy.BALANCED;
			break;
		default:
			throw new ParseException("strategy must be: each|uniform|balanced");
		}
		comms.sendServiceMsg(new AllocPartSMsg(partitionId,number,strat));
		ServiceMessage message = skipProgress();
		AllocPartErrorSMsg apem;
		switch(message.getType()) {
		case ALLOCATE_PARTITION_ERROR:
			apem = (AllocPartErrorSMsg) message;
			System.out.println("error: "+apem.getError());
			break;
		case PARTITION_ALLOCATED:
			System.out.println("partition allocated");
			break;
		default:
			System.out.println("unexpected response: "+message.getType().name());
			comms.close();
			System.exit(-1);
		}
		comms.sendServiceMsg(new ServiceDoneSMsg());
		comms.close();
	}
	
	public static void deallocatePartition(Config conf,List<String> argList) throws ParseException, 
		DragonCommsException, InterruptedException {
		initComms(conf);
		if(argList.size()!=3) {
			throw new ParseException("required arguments: PARTITIONID NUMBER STRATEGY\n"+
					"where strategy is: each|uniform|balanced");
		}
		String partitionId = argList.get(0);
		Integer number = Integer.parseInt(argList.get(1));
		DeallocPartSMsg.Strategy strat;
		switch(argList.get(2)) {
		case "each":
			strat=DeallocPartSMsg.Strategy.EACH;
			break;
		case "uniform":
			strat=DeallocPartSMsg.Strategy.UNIFORM;
			break;
		case "balanced":
			strat=DeallocPartSMsg.Strategy.BALANCED;
			break;
		default:
			throw new ParseException("strategy must be: each|uniform|balanced");
		}
		comms.sendServiceMsg(new DeallocPartSMsg(partitionId,number,strat));
		ServiceMessage message = skipProgress();
		DeallocPartErrorSMsg apem;
		switch(message.getType()) {
		case DEALLOCATE_PARTITION_ERROR:
			apem = (DeallocPartErrorSMsg) message;
			System.out.println("error: "+apem.getError());
			break;
		case PARTITION_DEALLOCATED:
			System.out.println("partition deallocated");
			break;
		default:
			System.out.println("unexpected response: "+message.getType().name());
			comms.close();
			System.exit(-1);
		}
		comms.sendServiceMsg(new ServiceDoneSMsg());
		comms.close();
	}

}
