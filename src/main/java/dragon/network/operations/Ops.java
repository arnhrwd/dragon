package dragon.network.operations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dragon.network.Node;
import dragon.network.NodeContext;
import dragon.network.NodeDescriptor;
import dragon.network.messages.service.runtopo.RunTopoSMsg;
import dragon.network.messages.service.termtopo.TermTopoSMsg;
import dragon.topology.DragonTopology;

/**
 * Provides a standard interface to construct new group operations and make them
 * asynchronous, so as not to block the calling thread while waiting to transmit
 * group messages. Also handles other kinds of ops such as conditional ops, etc.
 * 
 * @author aaron
 *
 */
/**
 * @author aaron
 *
 */
public class Ops extends Thread {
	private static final Logger log = LogManager.getLogger(Ops.class);
	
	/**
	 * 
	 */
	private long opCounter = 0;
	
	/**
	 * 
	 */
	private final HashMap<Long, Op> groupOps;
	
	/**
	 * 
	 */
	private final Node node;
	
	/**
	 * 
	 */
	private static Ops me;
	
	/**
	 * 
	 */
	private final LinkedBlockingQueue<Op> readyQueue;
	
	/**
	 * 
	 */
	private final ArrayList<ConditionalOp> conditionalOps;
	
	/**
	 * @return
	 */
	public static Ops inst() {
		return me;
	}

	/**
	 * @param node
	 */
	public Ops(Node node) {
		Ops.me = this;
		this.node = node;
		groupOps = new HashMap<Long, Op>();
		readyQueue = new LinkedBlockingQueue<Op>();
		conditionalOps = new ArrayList<ConditionalOp>();
		setName("ops processor");
		start();
	}

	/**
	 * @param condition
	 * @param success
	 * @param failure
	 * @return
	 */
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
	
	/**
	 * @param topologyId
	 * @param jarFile
	 * @param topology
	 * @param success
	 * @param failure
	 * @return
	 */
	public RunTopoGroupOp newRunTopoGroupOp(String topologyId, byte[] jarFile, DragonTopology topology,
			IOpSuccess success, IOpFailure failure) {
		RunTopoGroupOp rtgo = new RunTopoGroupOp(topologyId, jarFile, success, failure);
		return (RunTopoGroupOp) newGroupOp(rtgo, topology);
	}

	/**
	 * @param rtm
	 * @param topology
	 * @param success
	 * @param failure
	 * @return
	 */
	public PrepareTopoGroupOp newPrepareTopoGroupOp(RunTopoSMsg rtm, DragonTopology topology, IOpSuccess success,
			IOpFailure failure) {
		PrepareTopoGroupOp ptgo = new PrepareTopoGroupOp(rtm, success, failure);
		return (PrepareTopoGroupOp) newGroupOp(ptgo, topology);
	}

	/**
	 * @param topologyId
	 * @param success
	 * @param failure
	 * @return
	 */
	public StartTopoGroupOp newStartTopologyGroupOp(String topologyId, IOpSuccess success, IOpFailure failure) {
		StartTopoGroupOp stgo = new StartTopoGroupOp(topologyId, success, failure);
		return (StartTopoGroupOp) newGroupOp(stgo, topologyId);
	}

	/**
	 * @param topologyId
	 * @param success
	 * @param failure
	 * @return
	 */
	public TermTopoGroupOp newTermTopoGroupOp(String topologyId, IOpSuccess success, IOpFailure failure) {
		TermTopoGroupOp ttgo = new TermTopoGroupOp(topologyId, success, failure);
		return (TermTopoGroupOp) newGroupOp(ttgo, topologyId);
	}

	/**
	 * @param ttm
	 * @param topology
	 * @param success
	 * @param failure
	 * @return
	 */
	public RemoveTopoGroupOp newRemoveTopoGroupOp(TermTopoSMsg ttm, DragonTopology topology, IOpSuccess success,
			IOpFailure failure) {
		RemoveTopoGroupOp trgo = new RemoveTopoGroupOp(ttm.topologyId, success, failure);
		return (RemoveTopoGroupOp) newGroupOp(trgo, topology);
	}

	/**
	 * @param success
	 * @param failure
	 * @return
	 */
	public ListToposGroupOp newListToposGroupOp(IOpSuccess success, IOpFailure failure) {
		ListToposGroupOp ltgo = new ListToposGroupOp(success, failure);
		// this group operation goes to EVERY dragon daemon
		NodeContext nc = new NodeContext();
		nc.putAll(node.getNodeProcessor().getContext());
		for (NodeDescriptor desc : nc.values()) {
			ltgo.add(desc);
		}
		register(ltgo);
		return ltgo;
	}

	/**
	 * @param topologyId
	 * @param success
	 * @param failure
	 * @return
	 */
	public HaltTopoGroupOp newHaltTopoGroupOp(String topologyId, IOpSuccess success, IOpFailure failure) {
		HaltTopoGroupOp htgo = new HaltTopoGroupOp(topologyId, success, failure);
		return (HaltTopoGroupOp) newGroupOp(htgo, topologyId);
	}

	/**
	 * @param topologyId
	 * @param success
	 * @param failure
	 * @return
	 */
	public ResumeTopoGroupOp newResumeTopoGroupOp(String topologyId, IOpSuccess success, IOpFailure failure) {
		ResumeTopoGroupOp htgo = new ResumeTopoGroupOp(topologyId, success, failure);
		return (ResumeTopoGroupOp) newGroupOp(htgo, topologyId);
	}
	
	/**
	 * @param desc
	 * @param success
	 * @param failure
	 * @return
	 */
	public JoinGroupOp newJoinGroupOp(NodeDescriptor desc,IOpSuccess success, IOpFailure failure) {
		JoinGroupOp jgo = new JoinGroupOp(success,failure);
		jgo.add(desc);
		register(jgo);
		return jgo;
	}
	
	/**
	 * @param partitionId
	 * @param allocation
	 * @param success
	 * @param failure
	 * @return
	 */
	public AllocPartGroupOp newAllocPartGroupOp(String partitionId,HashMap<NodeDescriptor,Integer> allocation,IOpSuccess success, IOpFailure failure) {
		AllocPartGroupOp apgo = new AllocPartGroupOp(partitionId,allocation,success,failure);
		for(NodeDescriptor desc : allocation.keySet()) {
			apgo.add(desc);
		}
		register(apgo);
		return apgo;
	}
	
	/**
	 * 
	 * @param partitionId
	 * @param allocation
	 * @param success
	 * @param failure
	 * @return
	 */
	public DeallocPartGroupOp newDeallocPartGroupOp(String partitionId,HashMap<NodeDescriptor,Integer> allocation,IOpSuccess success, IOpFailure failure) {
		DeallocPartGroupOp apgo = new DeallocPartGroupOp(partitionId,allocation,success,failure);
		for(NodeDescriptor desc : allocation.keySet()) {
			apgo.add(desc);
		}
		register(apgo);
		return apgo;
	}
	
	/**
	 * 
	 * @param success
	 * @param failure
	 * @return
	 */
	public GetStatusGroupOp newGetStatusGroupOp(IOpSuccess success, IOpFailure failure) {
		GetStatusGroupOp apgo = new GetStatusGroupOp(success,failure);
		// this group operation goes to EVERY dragon daemon
		NodeContext nc = new NodeContext();
		nc.putAll(node.getNodeProcessor().getContext());
		for (NodeDescriptor desc : nc.values()) {
			apgo.add(desc);
		}
		register(apgo);
		return apgo;
	}

	/**
	 * @param go
	 * @param topologyId
	 * @return
	 */
	private GroupOp newGroupOp(GroupOp go, String topologyId) {
		return newGroupOp(go, node.getLocalClusters().get(topologyId).getTopology());
	}

	/**
	 * @param go
	 * @param topology
	 * @return
	 */
	private GroupOp newGroupOp(GroupOp go, DragonTopology topology) {
		for (NodeDescriptor desc : topology.getReverseEmbedding().keySet()) {
			go.add(desc);
		}
		register(go);
		return go;
	}

	/**
	 * @param groupOp
	 */
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

	/**
	 * @param id
	 * @return
	 */
	public GroupOp getGroupOp(Long id) {
		synchronized (groupOps) {
			return (GroupOp) groupOps.get(id);
		}
	}

	/**
	 * @param id
	 */
	public void removeGroupOp(Long id) {
		synchronized (groupOps) {
			groupOps.remove(id);
		}
	}

	/* (non-Javadoc)
	 * @see java.lang.Thread#run()
	 */
	@Override
	public void run() {
		final ArrayList<ConditionalOp> removed = new ArrayList<ConditionalOp>();
		boolean hit;
		log.info("starting up");
		while (!isInterrupted()) {
			hit=false;
			
			Op op = readyQueue.poll();
			if(op!=null) {
				hit=true;
				if (op instanceof GroupOp) {
					GroupOp go = (GroupOp) op;
					try {
						node.getOperationsLock().lockInterruptibly();
						go.initiate(node.getComms());
					} catch(InterruptedException e) {
						log.error("interrupted while waiting for node operations lock");
						break;
					} finally {
						node.getOperationsLock().unlock();
					}
					
				} else if(op instanceof ConditionalOp){
					try {
						node.getOperationsLock().lockInterruptibly();
						op.start();
					} catch(InterruptedException e) {
						log.error("interrupted while waiting for node operations lock");
						break;
					} finally {
						node.getOperationsLock().unlock();
					}
					
					conditionalOps.add((ConditionalOp)op);
				} else {
					try {
						node.getOperationsLock().lockInterruptibly();
						op.start();
					} catch(InterruptedException e) {
						log.error("interrupted while waiting for node operations lock");
						break;
					} finally {
						node.getOperationsLock().unlock();
					}
					
				}
			}
			
			removed.clear();
			for(ConditionalOp cop : conditionalOps) {
				boolean check = false;
				try  {
					node.getOperationsLock().lockInterruptibly();
					check = cop.check();
				} catch(InterruptedException e) {
					log.error("interrupted while waiting for node operations lock");
					break;
				} finally {
					node.getOperationsLock().unlock();
				}
				if(check) {
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
					break;
				}
			}

		}
		log.info("shutting down");
		if(!readyQueue.isEmpty()) log.error("some ops are still on the ready queue");
		if(!conditionalOps.isEmpty()) log.error("some conditional ops did not complete");
	}
}
