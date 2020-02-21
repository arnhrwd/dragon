package dragon.network.messages.node.gettopoinfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import dragon.ComponentError;
import dragon.metrics.Sample;
import dragon.network.messages.node.NodeMessage;

/**
 * @author aaron
 *
 */
public class TopoInfoNMsg extends NodeMessage {
	private static final long serialVersionUID = 4785147438021153895L;
	
	/**
	 * 
	 */
	public final HashMap<String,String> state;
	
	/**
	 * 
	 */
	public final HashMap<String,HashMap<String,ArrayList<ComponentError>>> errors;
	
	/**
	 * 
	 */
	public final HashMap<String,List<String>> components;
	
	/**
	 * 
	 */
	public final HashMap<String,HashMap<String,Sample>> metrics;
	
	/**
	 * @param state
	 * @param errors
	 * @param components
	 */
	public TopoInfoNMsg(HashMap<String,String> state,
			HashMap<String,HashMap<String,ArrayList<ComponentError>>> errors,
			HashMap<String,List<String>> components,
			HashMap<String,HashMap<String,Sample>> metrics) {
		super(NodeMessage.NodeMessageType.TOPOLOGY_INFORMATION);
		this.state=state;
		this.errors=errors;
		this.components=components;
		this.metrics=metrics;
	}
}
