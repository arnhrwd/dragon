package dragon.network.messages.service.dealloc;

import java.util.Comparator;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dragon.Constants;
import dragon.network.Node;
import dragon.network.NodeContext;
import dragon.network.NodeDescriptor;
import dragon.network.comms.DragonCommsException;
import dragon.network.comms.IComms;
import dragon.network.messages.service.ServiceMessage;
import dragon.network.operations.DeallocPartGroupOp;
import dragon.network.operations.Ops;

/**
 * Deallocate a partition.
 * @author aaron
 *
 */
public class DeallocPartSMsg extends ServiceMessage {
	private static final long serialVersionUID = -7677487341344985961L;
	private final static Logger log = LogManager.getLogger(DeallocPartSMsg.class);


	/**
	 * The name of the partition to deallocate
	 */
	public final String partitionId;
	
	/**
	 * Number of daemons (JVMs) to remove
	 */
	public final Integer daemons;
	
	/**
	 * The deallocate strategy types
	 * <li>{@link #EACH}</li>
	 * <li>{@link #UNIFORM}</li>
	 * <li>{@link #BALANCED}</li>
	 * @author aaron
	 *
	 */
	public static enum Strategy {
		/**
		 * Deallocate the same number on each machine.
		 */
		EACH,
		
		/**
		 * Deallocate the number uniformly spread over
		 * the machines.
		 */
		UNIFORM,
		
		/**
		 * Deallocate the number spread over the machines
		 * so as to balance the machine load as much as
		 * possible.
		 */
		BALANCED
	}
	
	/**
	 * The allocation strategy to use when allocating a partition.
	 */
	public final Strategy strategy;
	
	/**
	 * 
	 * @param partitionId
	 * @param daemons
	 * @param strategy
	 */
	public DeallocPartSMsg(String partitionId,Integer daemons,Strategy strategy) {
		super(ServiceMessage.ServiceMessageType.DEALLOCATE_PARTITION);
		this.partitionId=partitionId;
		this.daemons=daemons;
		this.strategy=strategy;
	}
	
	/**
	 *
	 */
	@Override
	public void process() {
		final Node node = Node.inst();
		final IComms comms = node.getComms();
		int daemons = this.daemons;
		final NodeContext context = node.getNodeProcessor().getContext();
		// total partition counts
		final HashMap<String,Integer> partitionCount = new HashMap<>();
		// individual node codes
		final HashMap<NodeDescriptor,HashMap<String,Integer>> nodeCounts = new HashMap<>();
		// list of nodes that manage the partition, ordered by their individual counts
		final HashMap<String,PriorityQueue<NodeDescriptor>> pQueueMap = new HashMap<>();
		// list of deletions
		final HashMap<NodeDescriptor,Integer> deletions = new HashMap<>();
		
		if(partitionId.equals(Constants.DRAGON_PRIMARY_PARTITION)) {
			try {
				comms.sendServiceMsg(new DeallocPartErrorSMsg(partitionId,0,"can not delete the primary partition"));
			} catch (DragonCommsException e) {
				log.fatal("can't communicate with client: " + e.getMessage());
			}
			return;
		}
		/**
		 * list of existing partitions with their parent nodes...
		 */
		for(NodeDescriptor desc : context.values()) {
			String part = desc.getPartition();
			NodeDescriptor parent = desc.getParent();
			if(!partitionCount.containsKey(part)) {
				partitionCount.put(part,1);
			} else {
				partitionCount.put(part,partitionCount.get(part)+1);
			}
			if(!nodeCounts.containsKey(parent)) {
				nodeCounts.put(parent,new HashMap<>());
			}
			if(!nodeCounts.get(parent).containsKey(part)) {
				nodeCounts.get(parent).put(part,1);
			} else {
				nodeCounts.get(parent).put(part,nodeCounts.get(parent).get(part)+1);
			}
			
		}
		for(NodeDescriptor desc : context.values()) {
			String part = desc.getPartition();
			if(!part.equals(Constants.DRAGON_PRIMARY_PARTITION)) {
				if(!pQueueMap.containsKey(part)) {
					pQueueMap.put(part,new PriorityQueue<NodeDescriptor>(
							new Comparator<NodeDescriptor>() {
						@Override
						public int compare(NodeDescriptor arg0, NodeDescriptor arg1) {
							return nodeCounts.get(arg1).get(part).compareTo(nodeCounts.get(arg0).get(part));
						}
					}));
				}
				pQueueMap.get(part).add(desc.getParent());
			}
		}
		
		if(!partitionCount.containsKey(partitionId)) {
			client(new DeallocPartErrorSMsg(partitionId,0,"partition does not exist ["+partitionId+"]"));
			return;
		}
		if(partitionCount.get(partitionId)<daemons) {
			client(new DeallocPartErrorSMsg(partitionId,0,"only ["+partitionCount.get(partitionId)+"] partition instances exist"));
			return;
		}
		switch(strategy) {
		case BALANCED:
			while(daemons>0 && partitionCount.get(partitionId)>0) {
				NodeDescriptor desc = pQueueMap.get(partitionId).poll();
				nodeCounts.get(desc).put(partitionId,nodeCounts.get(desc).get(partitionId)-1);
				partitionCount.put(partitionId,partitionCount.get(partitionId)-1);
				daemons--;
				if(!deletions.containsKey(desc)) {
					deletions.put(desc,1);
				} else {
					deletions.put(desc,deletions.get(desc)+1);
				}
			}
			break;
		case EACH:
			for(NodeDescriptor host : nodeCounts.keySet()) {
				if(nodeCounts.get(host).containsKey(partitionId) &&
						nodeCounts.get(host).get(partitionId)>0) {
					int amount = Math.min(nodeCounts.get(host).get(partitionId),daemons);
					deletions.put(host,amount);
				}
			}
			break;
		case UNIFORM:
			while(daemons>0) {
				int inc = daemons>partitionCount.get(partitionId) 
						? daemons/partitionCount.get(partitionId):1;
				for(NodeDescriptor host : nodeCounts.keySet()) {
					if(nodeCounts.get(host).get(partitionId)>0) {
						if(!deletions.containsKey(host)) {
							deletions.put(host,1);
						} else {
							deletions.put(host,deletions.get(host)+1);
						}
						daemons-=inc;
						if(daemons==0) break;
					}
				}
			}
			break;
		default:
			client(new DeallocPartErrorSMsg(partitionId,0,"invalid strategy ["+strategy+"]"));
			return;
		}
		Ops.inst().newDeallocPartGroupOp(partitionId,deletions, (op)->{
			progress("waiting up to ["+node.getConf().getDragonServiceTimeoutMs()/1000+"] seconds...");
		},(op)->{
			client(new PartDeallocedSMsg(partitionId,0));
		}, (op,error)->{
			client(new DeallocPartErrorSMsg(partitionId,0,error));
		}).onRunning((op)->{
			if(deletions.containsKey(node.getComms().getMyNodeDesc())) {
				int a = node.deallocatePartition(partitionId, deletions.get(node.getComms().getMyNodeDesc()));
				DeallocPartGroupOp apgo = (DeallocPartGroupOp) op;
				if(a!=deletions.get(node.getComms().getMyNodeDesc())) {
					apgo.receiveError(comms.getMyNodeDesc(), "failed to delete partition on ["+node.getComms().getMyNodeDesc()+"]");
				} else {
					apgo.receiveSuccess(comms.getMyNodeDesc());
				}
			}
			
		}).onTimeout(node.getTimer(), node.getConf().getDragonServiceTimeoutMs(), TimeUnit.MILLISECONDS, (op)->{
			op.fail("timed out waiting for nodes to respond");
		});
	}

}
