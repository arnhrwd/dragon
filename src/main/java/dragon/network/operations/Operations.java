package dragon.network.operations;

import java.util.HashMap;

import dragon.network.Node;
import dragon.network.NodeDescriptor;
import dragon.network.messages.service.HaltTopoSMsg;
import dragon.network.messages.service.ListToposSMsg;
import dragon.network.messages.service.ResumeTopoSMsg;
import dragon.network.messages.service.RunTopoSMsg;
import dragon.network.messages.service.TermTopoSMsg;
import dragon.topology.DragonTopology;

public class Operations extends Thread {
	private long operationCounter=0;
	private final HashMap<Long,Op> groupOperations;
	private final Node node;
	private static Operations me;
	
	public static Operations getInstance() {
		return me;
	}
	
	public Operations(Node node) {
		Operations.me=this;
		this.node=node;
		groupOperations=new HashMap<Long,Op>();
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
		return (StartTopoGroupOp) newGroupOperation(stgo,orig.topologyName);
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
		TermRouterGroupOp trgo = new TermRouterGroupOp(ttm,ttm.topologyId,success,failure);
		return (TermRouterGroupOp) newGroupOperation(trgo,topology);
	}
	
	public ListToposGroupOp newListToposGroupOp(ListToposSMsg ltm,
			IOpSuccess success,
			IOpFailure failure) {
		ListToposGroupOp ltgo = new ListToposGroupOp(ltm);
		ltgo.onSuccess(success);
		ltgo.onFailure(failure);
		for(NodeDescriptor desc : node.getNodeProcessor().getContext().values()) {
			ltgo.add(desc);
		}
		register(ltgo);
		ltgo.initiate(node.getComms());
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
		for(NodeDescriptor desc : node.getLocalClusters().get(topologyId).getTopology().getReverseEmbedding().keySet()) {
			go.add(desc);
		}
		register(go);
		go.initiate(node.getComms());
		return go;
	}
	
	private GroupOp newGroupOperation(GroupOp go,DragonTopology topology){
		for(NodeDescriptor desc : topology.getReverseEmbedding().keySet()) {
			go.add(desc);
		}
		register(go);
		go.initiate(node.getComms());
		return go;
	}

	private void register(Op groupOperation) {
		synchronized(groupOperations) {
			groupOperation.init(node.getComms().getMyNodeDescriptor(),operationCounter);
			groupOperations.put(operationCounter, groupOperation);
			operationCounter++;
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
}
