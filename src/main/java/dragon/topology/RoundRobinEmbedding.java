package dragon.topology;

import java.util.ArrayList;

import dragon.Config;
import dragon.network.NodeContext;

/**
 * @author aaron
 *
 */
public class RoundRobinEmbedding  implements IEmbeddingAlgo {

	/* (non-Javadoc)
	 * @see dragon.topology.IEmbeddingAlgo#generateEmbedding(dragon.topology.DragonTopology, dragon.network.NodeContext, dragon.Config)
	 */
	public ComponentEmbedding generateEmbedding(DragonTopology topology, NodeContext context, Config config) {
		ComponentEmbedding componentEmbedding = new ComponentEmbedding();
		ArrayList<String> nodes = new ArrayList<String>(context.keySet());
		int node=0;
		for(String spoutId : topology.getSpoutMap().keySet()) {
			SpoutDeclarer spoutDeclarer = topology.getSpoutMap().get(spoutId);
			for(int i=0;i<spoutDeclarer.getNumTasks();i++) {
				componentEmbedding.put(spoutId, i, context.get(nodes.get(node)));
				node=(node+1)%nodes.size();
			}
		}
		
		for(String boltId : topology.getBoltMap().keySet()) {
			BoltDeclarer boltDeclarer = topology.getBoltMap().get(boltId);
			for(int i=0;i<boltDeclarer.getNumTasks();i++) {
				componentEmbedding.put(boltId, i, context.get(nodes.get(node)));
				node=(node+1)%nodes.size();
			}
		}
		return componentEmbedding;
	}

}
