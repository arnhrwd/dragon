package dragon.network.operations;

import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.network.Node;
import dragon.network.NodeDescriptor;
import dragon.network.messages.service.HaltTopoSMsg;
import dragon.network.messages.service.ResumeTopoSMsg;
import dragon.network.messages.service.RunTopoSMsg;
import dragon.network.messages.service.TermTopoSMsg;
import dragon.topology.DragonTopology;

/**
 * Provides a standard interface to construct new group operations and
 * make them asynchronous, so as not to block the calling thread while
 * waiting to transmit group messages.
 * @author aaron
 *
 */
public class Operations extends Thread {
	private static final Log log = LogFactory.getLog(Operations.class);
	private long operationCounter=0;
	private final HashMap<Long,Op> groupOperations;
	private final Node node;
	private static Operations me;
	private final LinkedBlockingQueue<Op> readyQueue;
	
	public static Operations getInstance() {
		return me;
	}
	
	public Operations(Node node) {
		Operations.me=this;
		this.node=node;
		groupOperations=new HashMap<Long,Op>();
		readyQueue=new LinkedBlockingQueue<Op>();
		log.info("starting operations thread");
		start();
	}
	
	public RunTopoGroupOp newRunTopoGroupOp(RunTopoSMsg rtm,
			byte[] jarFile,
			DragonTopology topology,
			IOpSuccess success,
			IOpFailure failure) {
		RunTopoGroupOp rtgo = new RunTopoGroupOp(rtm,jarFile,success,failure);
		return (RunTopoGroupOp) newGroupOperation(rtgo,topology);
	}
	
	public PrepareTopoGroupOp newPrepareTopoGroupOp(RunTopoSMsg rtm,
			DragonTopology topology,
			IOpSuccess success,
			IOpFailure failure) {
		PrepareTopoGroupOp ptgo = new PrepareTopoGroupOp(rtm,success,failure);
		return (PrepareTopoGroupOp) newGroupOperation(ptgo,topology);
	}
	
	public StartTopoGroupOp newStartTopologyGroupOperation(RunTopoSMsg orig,
			IOpSuccess success,
			IOpFailure failure) {
		StartTopoGroupOp stgo = new StartTopoGroupOp(orig,success,failure);
		return (StartTopoGroupOp) newGroupOperation(stgo,orig.topologyId);
	}
	
	public TermTopoGroupOp newTermTopoGroupOp(TermTopoSMsg ttm,
			IOpSuccess success,
			IOpFailure failure) {
		TermTopoGroupOp ttgo = new TermTopoGroupOp(ttm,success,failure);
		return (TermTopoGroupOp) newGroupOperation(ttgo,ttm.topologyId);
	}
	
	public TermRouterGroupOp newTermRouterGroupOp(TermTopoSMsg ttm,
			DragonTopology topology,
			IOpSuccess success,
			IOpFailure failure) {
		TermRouterGroupOp trgo = new TermRouterGroupOp(ttm.topologyId,success,failure);
		return (TermRouterGroupOp) newGroupOperation(trgo,topology);
	}
	
	public ListToposGroupOp newListToposGroupOp(
			IOpSuccess success,
			IOpFailure failure) {
		ListToposGroupOp ltgo = new ListToposGroupOp(success,failure);
		for(NodeDescriptor desc : node.getNodeProcessor().getContext().values()) {
			ltgo.add(desc);
		}
		register(ltgo);
		return ltgo;
	}
			
	
	public HaltTopoGroupOp newHaltTopoGroupOp(HaltTopoSMsg orig,
			IOpSuccess success,
			IOpFailure failure) {
		HaltTopoGroupOp htgo = new HaltTopoGroupOp(orig,success,failure);
		return (HaltTopoGroupOp) newGroupOperation(htgo,orig.topologyId);
	}
	
	public ResumeTopoGroupOp newResumeTopoGroupOp(ResumeTopoSMsg orig,
			IOpSuccess success,
			IOpFailure failure) {
		ResumeTopoGroupOp htgo = new ResumeTopoGroupOp(orig,success,failure);
		return (ResumeTopoGroupOp) newGroupOperation(htgo,orig.topologyId);
	}
	
	private GroupOp newGroupOperation(GroupOp go,String topologyId){
		return newGroupOperation(go,node.getLocalClusters().get(topologyId).getTopology());
	}
	
	private GroupOp newGroupOperation(GroupOp go,DragonTopology topology){
		for(NodeDescriptor desc : topology.getReverseEmbedding().keySet()) {
			go.add(desc);
		}
		register(go);
		return go;
	}

	private void register(Op groupOperation) {
		synchronized(groupOperations) {
			groupOperation.init(node.getComms().getMyNodeDesc(),operationCounter);
			groupOperations.put(operationCounter, groupOperation);
			operationCounter++;
			try {
				readyQueue.put(groupOperation);
			} catch (InterruptedException e) {
				log.error("interrupted while putting onto the ready queue");
			}
		}
	}
	
	public GroupOp getGroupOperation(Long id) {
		synchronized(groupOperations) {
			return (GroupOp) groupOperations.get(id);
		}
	}
	
	public void removeGroupOperation(Long id) {
		synchronized(groupOperations) {
			groupOperations.remove(id);
		}
	}
	
	@Override
	public void run() {
		while(!isInterrupted()) {
			try {
				Op op = readyQueue.take();
				if(op instanceof GroupOp) {
					GroupOp go = (GroupOp) op;
					go.initiate(node.getComms());
				} else {
					op.start();
				}
			} catch (InterruptedException e) {
				log.info("interrupted");
			}
			
		}
	}
}
