package dragon.network.operations;

import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.network.Node;
import dragon.network.NodeDescriptor;
import dragon.network.messages.service.RunTopoSMsg;
import dragon.network.messages.service.TermTopoSMsg;
import dragon.topology.DragonTopology;

/**
 * Provides a standard interface to construct new group operations and make them
 * asynchronous, so as not to block the calling thread while waiting to transmit
 * group messages.
 * 
 * @author aaron
 *
 */
public class Ops extends Thread {
	private static final Log log = LogFactory.getLog(Ops.class);
	private long opCounter = 0;
	private final HashMap<Long, Op> groupOps;
	private final Node node;
	private static Ops me;
	private final LinkedBlockingQueue<Op> readyQueue;

	public static Ops inst() {
		return me;
	}

	public Ops(Node node) {
		Ops.me = this;
		this.node = node;
		groupOps = new HashMap<Long, Op>();
		readyQueue = new LinkedBlockingQueue<Op>();
		log.info("starting operations thread");
		start();
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

	public StartTopoGroupOp newStartTopologyGroupOperation(String topologyId, IOpSuccess success, IOpFailure failure) {
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
		for (NodeDescriptor desc : node.getNodeProcessor().getContext().values()) {
			ltgo.add(desc);
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
		while (!isInterrupted()) {
			try {
				Op op = readyQueue.take();
				if (op instanceof GroupOp) {
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
