package dragon.topology;

import dragon.Config;
import dragon.network.NodeContext;

/**
 * @author gayashan
 *
 */
public interface IEmbeddingAlgo {
	/**
	 * @param topology
	 * @param context
	 * @param config
	 * @return
	 */
	public ComponentEmbedding generateEmbedding(DragonTopology topology, NodeContext context, Config config);
}
