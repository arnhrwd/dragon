package dragon.network.messages.node.preparetopo;

import dragon.Config;
import dragon.DragonRequiresClonableException;
import dragon.network.DragonTopologyException;
import dragon.network.Node;
import dragon.network.messages.node.NodeMessage;
import dragon.topology.DragonTopology;

/**
 * @author aaron
 *
 */
public class PrepareTopoNMsg extends NodeMessage {
	private static final long serialVersionUID = 2867515610457893626L;
	
	/**
	 * 
	 */
	public DragonTopology topology;
	
	/**
	 * 
	 */
	public String topoloyId;
	
	/**
	 * 
	 */
	public Config conf;

	/**
	 * @param topologyName
	 * @param conf
	 * @param dragonTopology
	 */
	public PrepareTopoNMsg(String topologyName, Config conf, DragonTopology dragonTopology) {
		super(NodeMessage.NodeMessageType.PREPARE_TOPOLOGY);
		this.topoloyId=topologyName;
		this.topology=dragonTopology;
		this.conf=conf;
		
	}
	
	/**
	 *
	 */
	@Override
	public void process() {
		final Node node = Node.inst();
		try {
			try {
				node.prepareTopology(topoloyId, conf, topology, false);
			} catch (DragonTopologyException e) {
				sendError(e.getMessage());
			}
			sendSuccess();
		} catch (DragonRequiresClonableException e) {
			sendError(e.getMessage());
		}
	}

}
