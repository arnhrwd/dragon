package dragon.network.operations;

import java.util.ArrayList;
import java.util.HashMap;

import dragon.ComponentError;
import dragon.network.NodeDescriptor;
import dragon.network.messages.Message;
import dragon.network.messages.node.GetTopologyInformationMessage;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.TopologyInformationMessage;
import dragon.network.messages.service.ServiceMessage;
import dragon.network.messages.service.TopologyListMessage;

public class ListTopologiesGroupOperation extends GroupOperation {
	private static final long serialVersionUID = 7346932652353465012L;
	public HashMap<String,String> state;
	public HashMap<String,HashMap<String,ArrayList<ComponentError>>> errors;
	private final HashMap<String,HashMap<String,String>> descState;
	private final HashMap<String,HashMap<String,HashMap<String,ArrayList<ComponentError>>>> descErrors;
	
	public ListTopologiesGroupOperation(Message orig) {
		super(orig);
		descState=new HashMap<String,HashMap<String,String>>();
		descErrors=new HashMap<String,HashMap<String,HashMap<String,ArrayList<ComponentError>>>>();
	}
	
	public void aggregate(NodeDescriptor desc,
			HashMap<String,String> state,
			HashMap<String,HashMap<String,ArrayList<ComponentError>>> errors) {
			descState.put(desc.toString(),state);
			descErrors.put(desc.toString(),errors);
	}


	@Override
	public NodeMessage initiateNodeMessage() {
		return new GetTopologyInformationMessage("");
	}
	
	@Override
	public NodeMessage successNodeMessage() {
		return new TopologyInformationMessage(state,errors);
	}
	
	@Override
	public ServiceMessage successServiceMessage() {
		return new TopologyListMessage(descState,descErrors);
	}

}
