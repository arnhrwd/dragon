package dragon.topology;

import dragon.Config;
import dragon.network.NodeContext;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Map;
import java.util.stream.Collectors;

/***
 * This class implements {@link IEmbeddingAlgo} interface to embed tasks on to host devices based
 * on a placement plan provided by an external YAML file.
 *
 * The format of the file should be in standard YAML syntax,
 *
 * "spout name or bolt name": ["node 1 host name:node 1 port", "node 2 host name:node 2 port",...]
 *
 * eg:
 * "So1": ["localhost:4001"]
 * "So2": ["localhost:4001","localhost:4101"]
 * "Op1": ["localhost:4101"]
 * "Op2": ["localhost:4101"]
 * "Op3": ["localhost:41201","localhost:4101","localhost:4001"]
 *
 * The host names of the nodes should be as same as used in the dragon.properties file of each node.
 *
 * The embedding algorithm can be configured via the Config.DRAGON_EMBEDDING_ALGORITHM property which
 * can be defined in the topology or via the dragon.properties file.
 * Similarly, the name of the custom embedding plan file can be configured via the
 * Config.DRAGON_EMBEDDING_CUSTOM_FILE property and the {@link FileBasedCustomEmbedding} will try to
 * first load the file from the current directory and then from the classpath of the topology jar file.
 * Default embedding file name is "embedding.yaml".
 *
 */
public class FileBasedCustomEmbedding implements IEmbeddingAlgo {
    private static Logger log = LogManager.getLogger(FileBasedCustomEmbedding.class);

    @Override
    public ComponentEmbedding generateEmbedding(DragonTopology topology, NodeContext context, Config config) {
        ComponentEmbedding componentEmbedding = new ComponentEmbedding();
        String embeddingPlanFileName = String.valueOf(config.getDragonEmbeddingCustomFile());

        Yaml embeddingPlan = new Yaml();
        try (InputStream inputStream = loadByFileName(embeddingPlanFileName)) {
            Map<String, ArrayList<String>> embeddingPlanMap = embeddingPlan.load(inputStream);

            for (String spoutId : topology.getSpoutMap().keySet()) {
                SpoutDeclarer spoutDeclarer = topology.getSpoutMap().get(spoutId);
                generateTaskToNodeEmbedding(context, componentEmbedding, embeddingPlanMap, spoutId,
                        spoutDeclarer.getNumTasks());
            }

            for (String boltId : topology.getBoltMap().keySet()) {
                BoltDeclarer boltDeclarer = topology.getBoltMap().get(boltId);
                generateTaskToNodeEmbedding(context, componentEmbedding, embeddingPlanMap, boltId,
                        boltDeclarer.getNumTasks());
            }
            log.debug("Generated custom embedding : " + componentEmbedding);
            return componentEmbedding;
        } catch (FileNotFoundException e) {
            String msg = "The custom embedding plan file " + embeddingPlanFileName + " is not provided.";
            log.error(msg);
            throw new RuntimeException(msg, e);
        } catch (IOException e) {
            String msg = "Unable to open the custom embedding plan file " + embeddingPlanFileName;
            log.error(msg);
            throw new RuntimeException(msg, e);
        }
    }

    /***
     * Add the embedding for the taskId -> node as given in the embedding placement plan. This method only adds an
     * embedding for nodes available in the system context at the topology submission time. Other nodes mentioned in
     * the embedding plan are ignored.
     *
     * If there are replicas for tasks, they are placed in the available nodes out of the nodes assigned
     * in the custom plan, in a round robin manner.
     *
     * @param context - System context that contains all the connected nodes
     * @param componentEmbedding - Resulting component embedding
     * @param embeddingPlanMap - Plan provided externally via file
     * @param taskId - Task being mapped to a node
     * @param numTasks - number of tasks (replicas) to be mapped
     */
    private void generateTaskToNodeEmbedding(NodeContext context, ComponentEmbedding componentEmbedding,
                                             Map<String, ArrayList<String>> embeddingPlanMap, String taskId,
                                             int numTasks) {
        if (embeddingPlanMap.containsKey(taskId)) {
            ArrayList<String> nodesFromPlan = embeddingPlanMap.get(taskId);
            // intersection of the list of nodes given in the custom placement plan
            // for the particular task and the currently available nodes in the system context
            ArrayList<String> availableNodes = (ArrayList<String>) nodesFromPlan.stream()
                                                                                .filter(context::containsKey)
                                                                                .collect(Collectors.toList());
            if (!availableNodes.isEmpty()) {
                for (int i = 0; i < numTasks; i++) {
                    String taskEmbeddingNode = availableNodes.get(i % availableNodes.size());
                    componentEmbedding.put(taskId, i, context.get(taskEmbeddingNode));
                }
            }
        }
    }

    private InputStream loadByFileName(String name) throws FileNotFoundException {
        File f = new File(name);
        if (f.isFile()) {
            return new FileInputStream(f);
        } else {
            return this.getClass().getClassLoader().getResourceAsStream(name);
        }
    }
}
