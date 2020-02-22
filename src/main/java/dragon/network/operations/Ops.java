package dragon.network.operations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dragon.network.Node;
import dragon.network.NodeContext;
import dragon.network.NodeDescriptor;
import dragon.network.messages.service.runtopo.RunTopoErrorSMsg;
import dragon.network.messages.service.runtopo.RunTopoSMsg;
import dragon.network.messages.service.termtopo.TermTopoSMsg;
import dragon.topology.DragonTopology;

/**
 * Provides asynchronous events, largely to support operations that require message
 * passing, so as not to block the calling thread while waiting to transmit
 * group messages and receive responses. Also handles other kinds of ops such
 * as conditional ops, etc., that take place when given conditions arise.
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
	public Ops() {
		Ops.me = this;
		this.node = Node.inst();
		groupOps = new HashMap<Long, Op>();
		readyQueue = new LinkedBlockingQueue<Op>();
		conditionalOps = new ArrayList<ConditionalOp>();
		setName("ops processor");
		start();
	}

	public Op newOp(IOpStart start,IOpRunning running,
			IOpSuccess success,IOpFailure failure) {
		Op op = new Op(start,running,success,failure);
		try {
			readyQueue.put(op);
		} catch (InterruptedException e) {
			log.error("interrupted while putting op on queue");
		}
		return op;
		
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
	 * @throws DragonInvalidContext 
	 */
	public PrepJarGroupOp newPrepJarGroupOp(String topologyId, byte[] jarFile, DragonTopology topology,
			IOpSuccess success, IOpFailure failure) throws DragonInvalidContext {
		PrepJarGroupOp rtgo = new PrepJarGroupOp(node.getComms(),topologyId, jarFile, success, failure);
		return (PrepJarGroupOp) newGroupOp(rtgo, topology);
	}

	/**
	 * @param rtm
	 * @param topology
	 * @param success
	 * @param failure
	 * @return
	 * @throws DragonInvalidContext 
	 */
	public PrepTopoGroupOp newPreTopoGroupOp(RunTopoSMsg rtm, DragonTopology topology, IOpSuccess success,
			IOpFailure failure) throws DragonInvalidContext {
		PrepTopoGroupOp ptgo = new PrepTopoGroupOp(node.getComms(),rtm, success, failure);
		return (PrepTopoGroupOp) newGroupOp(ptgo, topology);
	}

	/**
	 * @param topologyId
	 * @param success
	 * @param failure
	 * @return
	 * @throws DragonInvalidContext 
	 */
	public StartTopoGroupOp newStartTopologyGroupOp(String topologyId, IOpSuccess success, IOpFailure failure) throws DragonInvalidContext {
		StartTopoGroupOp stgo = new StartTopoGroupOp(node.getComms(),topologyId, success, failure);
		return (StartTopoGroupOp) newGroupOp(stgo, topologyId);
	}

	/**
	 * @param topologyId
	 * @param success
	 * @param failure
	 * @return
	 * @throws DragonInvalidContext 
	 */
	public TermTopoGroupOp newTermTopoGroupOp(String topologyId, IOpSuccess success, IOpFailure failure) throws DragonInvalidContext {
		TermTopoGroupOp ttgo = new TermTopoGroupOp(node.getComms(),topologyId, success, failure);
		return (TermTopoGroupOp) newGroupOp(ttgo, topologyId);
	}

	/**
	 * @param ttm
	 * @param topology
	 * @param success
	 * @param failure
	 * @return
	 * @throws DragonInvalidContext 
	 */
	public RemoveTopoGroupOp newRemoveTopoGroupOp(TermTopoSMsg ttm, DragonTopology topology, IOpSuccess success,
			IOpFailure failure) throws DragonInvalidContext {
		RemoveTopoGroupOp trgo = new RemoveTopoGroupOp(node.getComms(),ttm.topologyId,ttm.purge, success, failure);
		
		if(ttm.purge) {
			/*
			 * The purge operation needs to be robust to missing nodes in the topology.
			 * This case will not throw a DragonInvalidContext
			 */
			for (NodeDescriptor desc : topology.getReverseEmbedding().keySet()) {
				if(!node.getNodeProcessor().getContext().containsKey(desc.toString())) {
					log.warn("["+desc+"] does not exist");
				} else {
					trgo.add(desc);
				}
			}
			return trgo;
		}
		
		return (RemoveTopoGroupOp) newGroupOp(trgo, topology);
	}

	/**
	 * @param success
	 * @param failure
	 * @return
	 */
	public ListToposGroupOp newListToposGroupOp(IOpSuccess success, IOpFailure failure) {
		ListToposGroupOp ltgo = new ListToposGroupOp(node.getComms(),success, failure);
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
	 * @throws DragonInvalidContext 
	 */
	public HaltTopoGroupOp newHaltTopoGroupOp(String topologyId, IOpSuccess success, IOpFailure failure) throws DragonInvalidContext {
		HaltTopoGroupOp htgo = new HaltTopoGroupOp(node.getComms(),topologyId, success, failure);
		return (HaltTopoGroupOp) newGroupOp(htgo, topologyId);
	}

	/**
	 * @param topologyId
	 * @param success
	 * @param failure
	 * @return
	 * @throws DragonInvalidContext 
	 */
	public ResumeTopoGroupOp newResumeTopoGroupOp(String topologyId, IOpSuccess success, IOpFailure failure) throws DragonInvalidContext {
		ResumeTopoGroupOp htgo = new ResumeTopoGroupOp(node.getComms(),topologyId, success, failure);
		return (ResumeTopoGroupOp) newGroupOp(htgo, topologyId);
	}
	
	/**
	 * @param desc
	 * @param success
	 * @param failure
	 * @return
	 */
	public JoinGroupOp newJoinGroupOp(NodeDescriptor desc,IOpSuccess success, IOpFailure failure) {
		JoinGroupOp jgo = new JoinGroupOp(node.getComms(),success,failure);
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
		AllocPartGroupOp apgo = new AllocPartGroupOp(node.getComms(),partitionId,allocation,success,failure);
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
		DeallocPartGroupOp apgo = new DeallocPartGroupOp(node.getComms(),partitionId,allocation,success,failure);
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
		GetStatusGroupOp apgo = new GetStatusGroupOp(node.getComms(),success,failure);
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
	 * @throws DragonInvalidContext 
	 */
	private GroupOp newGroupOp(GroupOp go, String topologyId) throws DragonInvalidContext {
		return newGroupOp(go, node.getLocalClusters().get(topologyId).getTopology());
	}

	/**
	 * Adds all of the nodes in the topology mapping to the group operation.
	 * @param go
	 * @param topology
	 * @return
	 * @throws DragonInvalidContext if a node in the topology does not exist
	 */
	private GroupOp newGroupOp(GroupOp go, DragonTopology topology) throws DragonInvalidContext {
		for (NodeDescriptor desc : topology.getReverseEmbedding().keySet()) {
			if(!node.getNodeProcessor().getContext().containsKey(desc.toString())) {
				throw new DragonInvalidContext("["+desc+"] does not exist");
			}
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
						go.start();
					} catch(InterruptedException e) {
						log.error("interrupted while waiting for node operations lock");
						break;
					} finally {
						node.getOperationsLock().unlock();
					}
					
				} else if(op instanceof ConditionalOp){
					try {
						node.getOperationsLock().lockInterruptibly();
						try {
							op.start();
						} catch (Exception e) {
							e.printStackTrace();
							log.error(e);
						}
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
						try {
							op.start();
						} catch (Exception e) {
							e.printStackTrace();
							log.error(e);
						}
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
