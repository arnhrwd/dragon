package dragon.topology;

import dragon.network.NodeContext;

public interface IEmbeddingAlgo {
	public ComponentEmbedding generateEmbedding(DragonTopology topology, NodeContext context);
}
