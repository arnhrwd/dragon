package dragon.topology;

import dragon.Config;
import dragon.network.NodeContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.util.ArrayList;
import java.util.Map;
import java.util.stream.Collectors;

public class FileBasedCustomEmbedding implements IEmbeddingAlgo {
    private static Log log = LogFactory.getLog(FileBasedCustomEmbedding.class);

    @Override
    public ComponentEmbedding generateEmbedding(DragonTopology topology, NodeContext context, Config config) {
        ComponentEmbedding componentEmbedding = new ComponentEmbedding();
        String embeddingPlanFileName = String.valueOf(config.get(Config.DRAGON_EMBEDDING_CUSTOM_FILE));

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
