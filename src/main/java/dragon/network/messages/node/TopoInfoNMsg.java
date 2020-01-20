package dragon.network.messages.node;

import java.util.ArrayList;
import java.util.HashMap;

import dragon.ComponentError;

public class TopoInfoNMsg extends NodeMessage {
	private static final long serialVersionUID = 4785147438021153895L;
	public final HashMap<String,String> state;
	public final HashMap<String,HashMap<String,ArrayList<ComponentError>>> errors;
	public TopoInfoNMsg(HashMap<String,String> state,
			HashMap<String,HashMap<String,ArrayList<ComponentError>>> errors) {
		super(NodeMessage.NodeMessageType.TOPOLOGY_INFORMATION);
		this.state=state;
		this.errors=errors;
	}
}
