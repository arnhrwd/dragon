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
import dragon.network.messages.service.ServiceErrorMessage;
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
		System.out.println("connecting to dragon daemon: ["+node+"]");
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
	 * if they arrive. Bail out on error.
	 * @return the first message that is not a progress message or
	 * error message
	 */
	private static ServiceMessage fromServer() {
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
			} else if(message instanceof ServiceErrorMessage) {
				System.out.println(((ServiceErrorMessage)message).error);
				try {
					comms.sendServiceMsg(new ServiceDoneSMsg());
				} catch (DragonCommsException e1) {
					System.out.println("could not send service done message");
					System.exit(-1);
				}
				comms.close();
				System.exit(-1);
			}
		}
		System.out.println("received ["+message.getType().name()+"]");
		return message;
	}
	
	/**
	 * Send a service message.
	 * @param msg
	 */
	private static void toServer(ServiceMessage msg) {
		System.out.println("sending ["+msg.getType().name()+"]");
		try {
			comms.sendServiceMsg(msg);
		} catch (DragonCommsException e1) {
			System.out.println("error: "+e1);
			System.exit(-1);
		}
	}
	
	/**
	 * Submit a topology to a given Dragon daemon.
	 * @param string the user defined name of the topology
	 * @param conf parameters that override the daemon specific settings for the topology
	 * @param topology the topology to submit
	 */
	public static void submitTopology(String string, Config conf, DragonTopology topology) {
		topology.setTopologyId(string);
		initComms(conf);
		System.out.println("requesting context...");
		toServer(new GetNodeContextSMsg());
		ServiceMessage message=fromServer();
		NodeContext context = ((NodeContextSMsg)message).context;
		log.debug("received context  ["+context+"]");
		IEmbeddingAlgo embedding = ReflectionUtils.newInstance(conf.getDragonEmbeddingAlgorithm());
		topology.embedTopology(embedding, context, conf);
		System.out.println("uploading jar file...");
		toServer(new UploadJarSMsg(string,topologyJar));
		message = fromServer();
		System.out.println("starting topology...");
		toServer(new RunTopoSMsg(string,conf,topology));
		message = fromServer();
		toServer(new ServiceDoneSMsg());
		comms.close();
	}
	
	/**
	 * Extract metrics for the given topology on the supplied daemon node.
	 * @param conf
	 * @param topologyId
	 * @throws InterruptedException
	 * @throws DragonCommsException
	 */
	public static void getMetrics(Config conf,String topologyId) {
		initComms(conf);
		System.out.println("getting metrics...");
		toServer(new GetMetricsSMsg(topologyId));
		ServiceMessage message = fromServer();
		MetricsSMsg m = (MetricsSMsg) message;
		System.out.println(m.samples.toString());
		toServer(new ServiceDoneSMsg());
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
	public static void terminateTopology(Config conf, String topologyId) {
		initComms(conf);
		System.out.println("terminating topology ["+topologyId+"]...");
		toServer(new TermTopoSMsg(topologyId,false));
		@SuppressWarnings("unused")
		ServiceMessage message = fromServer();
		System.out.println("topology terminated ["+topologyId+"]");
		toServer(new ServiceDoneSMsg());
		comms.close();
	}
	
	/**
	 * Resume a halted topology.
	 * @param conf
	 * @param topologyId
	 * @throws InterruptedException
	 * @throws DragonCommsException
	 */
	public static void resumeTopology(Config conf, String topologyId) {
		initComms(conf);
		System.out.println("resuming topology ["+topologyId+"]...");
		toServer(new ResumeTopoSMsg(topologyId));
		@SuppressWarnings("unused")
		ServiceMessage message = fromServer();
		System.out.println("topology resumed ["+topologyId+"]");
		toServer(new ServiceDoneSMsg());
		comms.close();
	}
	
	/**
	 * Halt a topology, which simply stops all processing with data still in place. Can be resumed.
	 * @param conf
	 * @param topologyId
	 * @throws InterruptedException
	 * @throws DragonCommsException
	 */
	public static void haltTopology(Config conf, String topologyId)  {
		initComms(conf);
		System.out.println("halting topology ["+topologyId+"]...");
		toServer(new HaltTopoSMsg(topologyId));
		@SuppressWarnings("unused")
		ServiceMessage message = fromServer();
		System.out.println("topology halted ["+topologyId+"]");
		toServer(new ServiceDoneSMsg());
		comms.close();
	}
	
	/**
	 * List all of the topologies.
	 * @param conf
	 * @throws DragonCommsException
	 * @throws InterruptedException
	 */
	public static void listTopologies(Config conf) {
		initComms(conf);
		System.out.println("requesting list of topologies...");
		toServer(new ListToposSMsg());
		TopoListSMsg message = (TopoListSMsg) fromServer();
		toServer(new ServiceDoneSMsg());
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
				if(message.descState.get(descid).get(topologyId)==null) continue;
				TreeNode machine= new TreeNode("["+descid+"] "+message.descState.get(descid).get(topologyId));
				topo.addChild(machine);
				// list components
				if(message.descComponents.get(descid).containsKey(topologyId)) {
					for(String cid : message.descComponents.get(descid).get(topologyId)) {
						String name="["+cid+"]";
						Sample metrics = message.descMetrics.get(descid).get(topologyId).get(cid);
						name+=" emt:"+metrics.emitted+",exe:"+metrics.processed+",trf:"+metrics.transferred;
						TreeNode component = new TreeNode(name);
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
	
	public static void getStatus(Config conf) {
		initComms(conf);
		System.out.println("requesting status...");
		toServer(new GetStatusSMsg());
		StatusSMsg message = (StatusSMsg) fromServer();
		toServer(new ServiceDoneSMsg());
		comms.close();
		ArrayList<NodeStatus> dragonStatus = message.dragonStatus;
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
	
	public static void allocatePartition(Config conf,List<String> argList) throws ParseException {
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
		System.out.println("allocating partition ["+partitionId+"]...");
		toServer(new AllocPartSMsg(partitionId,number,strat));
		@SuppressWarnings("unused")
		ServiceMessage message = fromServer();
		System.out.println("partition allocated");
		toServer(new ServiceDoneSMsg());
		comms.close();
	}
	
	public static void deallocatePartition(Config conf,List<String> argList) throws ParseException {
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
		System.out.println("deallocating partition ["+partitionId+"]...");
		toServer(new DeallocPartSMsg(partitionId,number,strat));
		@SuppressWarnings("unused")
		ServiceMessage message = fromServer();
		System.out.println("partition deallocated");
		toServer(new ServiceDoneSMsg());
		comms.close();
	}

	public static void purgeTopology(Config conf, String topologyId) {
		initComms(conf);
		System.out.println("purging topology ["+topologyId+"]...");
		toServer(new TermTopoSMsg(topologyId,true));
		@SuppressWarnings("unused")
		ServiceMessage message = fromServer();
		System.out.println("topology purged ["+topologyId+"]");
		toServer(new ServiceDoneSMsg());
		comms.close();
	}

}
