package dragon.network.operations;

import java.util.ArrayList;
import java.util.HashMap;

import dragon.ComponentError;
import dragon.network.NodeDescriptor;
import dragon.network.messages.Message;
import dragon.network.messages.node.GetTopologyInformationMessage;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.TopologyInformationMessage;

public class ListTopologiesGroupOperation extends GroupOperation {
	private static final long serialVersionUID = 7346932652353465012L;
	public transient HashMap<String,String> state;
	public transient HashMap<String,HashMap<String,ArrayList<ComponentError>>> errors;
	public transient final HashMap<String,HashMap<String,String>> descState;
	public transient final HashMap<String,HashMap<String,HashMap<String,ArrayList<ComponentError>>>> descErrors;
	
	public ListTopologiesGroupOperation(Message orig) {
		super(orig,null,null);
		descState=new HashMap<String,HashMap<String,String>>();
		descErrors=new HashMap<String,HashMap<String,HashMap<String,ArrayList<ComponentError>>>>();
	}
	
//	public ListTopologiesGroupOperation(Message orig, IOperationSuccess success, IOperationFailure failure) {
//		super(orig,success,failure);
//		descState=new HashMap<String,HashMap<String,String>>();
//		descErrors=new HashMap<String,HashMap<String,HashMap<String,ArrayList<ComponentError>>>>();
//	}
	
	public void aggregate(NodeDescriptor desc,
			HashMap<String,String> state,
			HashMap<String,HashMap<String,ArrayList<ComponentError>>> errors) {
			descState.put(desc.toString(),state);
			descErrors.put(desc.toString(),errors);
	}


	@Override
	public NodeMessage initiateNodeMessage() {
		return new GetTopologyInformationMessage();
	}
	
	@Override
	public NodeMessage successNodeMessage() {
		return new TopologyInformationMessage(state,errors);
	}

}
