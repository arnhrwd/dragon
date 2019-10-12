package dragon.network.operations;

import java.util.ArrayList;
import java.util.HashMap;

import dragon.ComponentError;
import dragon.network.NodeDescriptor;
import dragon.network.messages.node.GetTopoInfoNMsg;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.TopoInfoNMsg;

public class ListToposGroupOp extends GroupOp {
	private static final long serialVersionUID = 7346932652353465012L;
	public transient HashMap<String,String> state;
	public transient HashMap<String,HashMap<String,ArrayList<ComponentError>>> errors;
	public transient final HashMap<String,HashMap<String,String>> descState;
	public transient final HashMap<String,HashMap<String,HashMap<String,ArrayList<ComponentError>>>> descErrors;
	
	public ListToposGroupOp(IOpSuccess success, IOpFailure failure) {
		super(success,failure);
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
		return new GetTopoInfoNMsg();
	}
	
	@Override
	public NodeMessage successNodeMessage() {
		return new TopoInfoNMsg(state,errors);
	}

}
