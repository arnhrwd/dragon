package dragon.network.messages.service.allocpart;

import java.util.Comparator;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;

import dragon.Constants;
import dragon.network.Node;
import dragon.network.NodeContext;
import dragon.network.NodeDescriptor;
import dragon.network.comms.IComms;
import dragon.network.messages.service.ServiceMessage;
import dragon.network.operations.AllocPartGroupOp;
import dragon.network.operations.Ops;

/**
 * Allocate a new partition.
 * 
 * @author aaron
 *
 */
public class AllocPartSMsg extends ServiceMessage {
	private static final long serialVersionUID = -4383113891826020623L;
	
	/**
	 * The name of the partition to allocate
	 */
	public final String partitionId;
	
	/**
	 * Number of daemons (JVMs)
	 */
	public final Integer number;
	
	/**
	 * The allocation strategy types
	 * <li>{@link #EACH}</li>
	 * <li>{@link #UNIFORM}</li>
	 * <li>{@link #BALANCED}</li>
	 * @author aaron
	 *
	 */
	public static enum Strategy {
		/**
		 * Allocate the same number on each machine.
		 */
		EACH,
		
		/**
		 * Allocate the number uniformly spread over
		 * the machines.
		 */
		UNIFORM,
		
		/**
		 * Allocate the number spread over the machines
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
	 * @param partitionId
	 * @param number
	 * @param strategy
	 */
	public AllocPartSMsg(String partitionId,Integer number,Strategy strategy) {
		super(ServiceMessage.ServiceMessageType.ALLOCATE_PARTITION);
		this.partitionId=partitionId;
		this.number=number;
		this.strategy=strategy;
	}
	
	/**
	 *
	 */
	@Override
	public void process() {
		final Node node = Node.inst();
		final IComms comms = node.getComms();
		final NodeContext context = node.getNodeProcessor().getContext();
		int number = this.number;
		/*
		 * Who will allocate new processes and how many.
		 */
		final HashMap<NodeDescriptor,Integer> allocation = new HashMap<>();
		
		/*
		 * The load of a machine.
		 */
		final HashMap<NodeDescriptor,Integer> load = new HashMap<>();
		
		/* list of machines to consider, with the primary node that will be contacted
		 * for the group operation
		 */
		final HashMap<String,NodeDescriptor> machines = new HashMap<>();
		
		// first build a list of machines, and designate a primary
		for(NodeDescriptor desc : context.values()) {
			if(!machines.containsKey(desc.getHostName())) {
				if(desc.isPrimary()) {
					machines.put(desc.getHostName(),desc);
				}
			}
		}
		// now find the load for each machine
		for(NodeDescriptor desc : context.values()) {
			String hostname = desc.getHostName();
			if(!load.containsKey(machines.get(hostname))){
				load.put(machines.get(hostname),1);
			} else {
				load.put(machines.get(hostname),
						load.get(machines.get(hostname))+1);
			}
		}
		
		if(partitionId.equals(Constants.DRAGON_PRIMARY_PARTITION)) {
			client(new AllocPartErrorSMsg(partitionId,0,"can not add to the primary partition"));
			return;
		}
		
		switch(strategy) {
		case BALANCED:
			PriorityQueue<NodeDescriptor> pQueue = 
				new PriorityQueue<NodeDescriptor>(load.size(),
						new Comparator<NodeDescriptor>() {
				@Override
				public int compare(NodeDescriptor arg0, NodeDescriptor arg1) {
					return load.get(machines.get(arg0.getHostName()))
							.compareTo(load.get(machines.get(arg1.getHostName())));
				}
			}); 
			for(NodeDescriptor host: load.keySet()) {
				pQueue.add(host);
			}
			while(number>0) {
				NodeDescriptor host = pQueue.remove();
				if(!allocation.containsKey(host)) {
					allocation.put(host,1);
				} else {
					allocation.put(host,allocation.get(host)+1);
				}
				load.put(host,load.get(host)+1);
				pQueue.add(host);
				number--;
			}
			break;
		case EACH:
			for(NodeDescriptor host : load.keySet()) {
				allocation.put(host,number);
			}
			break;
		case UNIFORM:
			while(number>0) {
				int inc = number>load.size() ? number/load.size():1;
				for(NodeDescriptor host : load.keySet()) {
					if(!allocation.containsKey(host)) {
						allocation.put(host,1);
					} else {
						allocation.put(host,allocation.get(host)+1);
					}
					number-=inc;
					if(number==0) break;
				}
			}
			break;
		default:
			client(new AllocPartErrorSMsg(partitionId,0,"invalid strategy ["+strategy+"]"));
			return;
		}
		Ops.inst().newAllocPartGroupOp(partitionId,allocation, (op)->{
			progress("waiting up to ["+node.getConf().getDragonServiceTimeoutMs()/1000+"] seconds...");
		},(op)->{
			client(new PartAllocedSMsg(partitionId,0));
		}, (op,error)->{
			client(new AllocPartErrorSMsg(partitionId,0,error));
		}).onRunning((op)->{
			if(allocation.containsKey(node.getComms().getMyNodeDesc())) {
				int a = node.allocatePartition(partitionId, allocation.get(node.getComms().getMyNodeDesc()));
				AllocPartGroupOp apgo = (AllocPartGroupOp) op;
				if(a!=allocation.get(node.getComms().getMyNodeDesc())) {
					apgo.receiveError(comms.getMyNodeDesc(), "failed to allocate partitions on ["+node.getComms().getMyNodeDesc()+"]");
				} else {
					apgo.receiveSuccess(comms.getMyNodeDesc());
					
				}
			}
			
		}).onTimeout(node.getTimer(), node.getConf().getDragonServiceTimeoutMs(), TimeUnit.MILLISECONDS, (op)->{
			op.fail("timed out waiting for nodes to respond");
		});
	}

}
