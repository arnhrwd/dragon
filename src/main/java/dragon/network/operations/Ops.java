package dragon.network.operations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import dragon.network.Node;
import dragon.network.NodeContext;
import dragon.network.NodeDescriptor;
import dragon.network.messages.service.RunTopoSMsg;
import dragon.network.messages.service.TermTopoSMsg;
import dragon.topology.DragonTopology;

/**
 * Provides a standard interface to construct new group operations and make them
 * asynchronous, so as not to block the calling thread while waiting to transmit
 * group messages. Also handles other kinds of ops such as conditional ops, etc.
 * 
 * @author aaron
 *
 */
public class Ops extends Thread {
	private static final Logger log = LogManager.getLogger(Ops.class);
	private long opCounter = 0;
	private final HashMap<Long, Op> groupOps;
	private final Node node;
	private static Ops me;
	private final LinkedBlockingQueue<Op> readyQueue;
	private final ArrayList<ConditionalOp> conditionalOps;
	public static Ops inst() {
		return me;
	}

	public Ops(Node node) {
		Ops.me = this;
		this.node = node;
		groupOps = new HashMap<Long, Op>();
		readyQueue = new LinkedBlockingQueue<Op>();
		conditionalOps = new ArrayList<ConditionalOp>();
		setName("ops processor");
		log.info("starting operations thread");
		start();
	}

	public ConditionalOp newConditionOp(IOpCondition condition,
			IOpSuccess success,
			IOpFailure failure) {
		ConditionalOp cop = new ConditionalOp(condition,success,failure);
		try {
			readyQueue.put(cop);
		} catch (InterruptedException e) {
			log.error("interrupted while putting conditional op on queue");
		}
		return cop;
	}
	
	public RunTopoGroupOp newRunTopoGroupOp(String topologyId, byte[] jarFile, DragonTopology topology,
			IOpSuccess success, IOpFailure failure) {
		RunTopoGroupOp rtgo = new RunTopoGroupOp(topologyId, jarFile, success, failure);
		return (RunTopoGroupOp) newGroupOp(rtgo, topology);
	}

	public PrepareTopoGroupOp newPrepareTopoGroupOp(RunTopoSMsg rtm, DragonTopology topology, IOpSuccess success,
			IOpFailure failure) {
		PrepareTopoGroupOp ptgo = new PrepareTopoGroupOp(rtm, success, failure);
		return (PrepareTopoGroupOp) newGroupOp(ptgo, topology);
	}

	public StartTopoGroupOp newStartTopologyGroupOp(String topologyId, IOpSuccess success, IOpFailure failure) {
		StartTopoGroupOp stgo = new StartTopoGroupOp(topologyId, success, failure);
		return (StartTopoGroupOp) newGroupOp(stgo, topologyId);
	}

	public TermTopoGroupOp newTermTopoGroupOp(String topologyId, IOpSuccess success, IOpFailure failure) {
		TermTopoGroupOp ttgo = new TermTopoGroupOp(topologyId, success, failure);
		return (TermTopoGroupOp) newGroupOp(ttgo, topologyId);
	}

	public RemoveTopoGroupOp newRemoveTopoGroupOp(TermTopoSMsg ttm, DragonTopology topology, IOpSuccess success,
			IOpFailure failure) {
		RemoveTopoGroupOp trgo = new RemoveTopoGroupOp(ttm.topologyId, success, failure);
		return (RemoveTopoGroupOp) newGroupOp(trgo, topology);
	}

	public ListToposGroupOp newListToposGroupOp(IOpSuccess success, IOpFailure failure) {
		ListToposGroupOp ltgo = new ListToposGroupOp(success, failure);
		// this group operation goes to EVERY dragon daemon
		NodeContext nc = new NodeContext();
		nc.putAll(node.getNodeProcessor().getContext());
		for (NodeDescriptor desc : nc.values()) {
			// we never send messages to ourselves
			if(!desc.equals(node.getComms().getMyNodeDesc())) ltgo.add(desc);
		}
		register(ltgo);
		return ltgo;
	}

	public HaltTopoGroupOp newHaltTopoGroupOp(String topologyId, IOpSuccess success, IOpFailure failure) {
		HaltTopoGroupOp htgo = new HaltTopoGroupOp(topologyId, success, failure);
		return (HaltTopoGroupOp) newGroupOp(htgo, topologyId);
	}

	public ResumeTopoGroupOp newResumeTopoGroupOp(String topologyId, IOpSuccess success, IOpFailure failure) {
		ResumeTopoGroupOp htgo = new ResumeTopoGroupOp(topologyId, success, failure);
		return (ResumeTopoGroupOp) newGroupOp(htgo, topologyId);
	}
	
	public JoinGroupOp newJoinGroupOp(NodeDescriptor desc,IOpSuccess success, IOpFailure failure) {
		JoinGroupOp jgo = new JoinGroupOp(success,failure);
		jgo.add(desc);
		register(jgo);
		return jgo;
	}
	
	public AllocPartGroupOp newAllocPartGroupOp(String partitionId,HashMap<NodeDescriptor,Integer> allocation,IOpSuccess success, IOpFailure failure) {
		AllocPartGroupOp apgo = new AllocPartGroupOp(partitionId,allocation,success,failure);
		for(NodeDescriptor desc : allocation.keySet()) {
			apgo.add(desc);
		}
		register(apgo);
		return apgo;
	}

	private GroupOp newGroupOp(GroupOp go, String topologyId) {
		return newGroupOp(go, node.getLocalClusters().get(topologyId).getTopology());
	}

	private GroupOp newGroupOp(GroupOp go, DragonTopology topology) {
		for (NodeDescriptor desc : topology.getReverseEmbedding().keySet()) {
			go.add(desc);
		}
		register(go);
		return go;
	}

	private void register(Op groupOp) {
		synchronized (groupOps) {
			groupOp.init(node.getComms().getMyNodeDesc(), opCounter);
			groupOps.put(opCounter, groupOp);
			opCounter++;
			try {
				readyQueue.put(groupOp);
			} catch (InterruptedException e) {
				groupOp.fail("interrupted while putting onto the ready queue");
			}
		}
	}

	public GroupOp getGroupOp(Long id) {
		synchronized (groupOps) {
			return (GroupOp) groupOps.get(id);
		}
	}

	public void removeGroupOp(Long id) {
		synchronized (groupOps) {
			groupOps.remove(id);
		}
	}

	@Override
	public void run() {
		final ArrayList<ConditionalOp> removed = new ArrayList<ConditionalOp>();
		boolean hit;
		while (!isInterrupted()) {
			hit=false;
			
			Op op = readyQueue.poll();
			if(op!=null) {
				hit=true;
				if (op instanceof GroupOp) {
					GroupOp go = (GroupOp) op;
					go.initiate(node.getComms());
				} else if(op instanceof ConditionalOp){
					op.start();
					conditionalOps.add((ConditionalOp)op);
				} else {
					op.start();
				}
			}
			
			removed.clear();
			for(ConditionalOp cop : conditionalOps) {
				if(cop.check()) {
					hit=true;
					removed.add(cop);
				}
			}
			conditionalOps.removeAll(removed);
			
			if(!hit) {
				try {
					Thread.sleep(50);
				} catch (InterruptedException e) {
					log.info("interrupted");
				}
			}

		}
	}
}
