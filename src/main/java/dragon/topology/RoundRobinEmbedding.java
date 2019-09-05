package dragon.topology;

import java.util.ArrayList;

import dragon.network.NodeContext;

public class RoundRobinEmbedding  implements IEmbeddingAlgo {

	public ComponentEmbedding generateEmbedding(DragonTopology topology, NodeContext context) {
		ComponentEmbedding componentEmbedding = new ComponentEmbedding();
		ArrayList<String> nodes = new ArrayList<String>(context.keySet());
		int node=0;
		for(String spoutId : topology.spoutMap.keySet()) {
			SpoutDeclarer spoutDeclarer = topology.spoutMap.get(spoutId);
			for(int i=0;i<spoutDeclarer.getNumTasks();i++) {
				componentEmbedding.put(spoutId, i, context.get(nodes.get(node)));
				node=(node+1)%nodes.size();
			}
		}
		
		for(String boltId : topology.boltMap.keySet()) {
			BoltDeclarer boltDeclarer = topology.boltMap.get(boltId);
			for(int i=0;i<boltDeclarer.getNumTasks();i++) {
				componentEmbedding.put(boltId, i, context.get(nodes.get(node)));
				node=(node+1)%nodes.size();
			}
		}
		return componentEmbedding;
	}

}
