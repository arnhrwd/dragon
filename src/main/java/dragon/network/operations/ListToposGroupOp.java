package dragon.network.operations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import dragon.ComponentError;
import dragon.network.NodeDescriptor;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.gettopoinfo.GetTopoInfoNMsg;
import dragon.network.messages.node.gettopoinfo.TopoInfoNMsg;

/**
 * @author aaron
 *
 */
public class ListToposGroupOp extends GroupOp {
	private static final long serialVersionUID = 7346932652353465012L;
	
	/*
	 * Holding variables prior to transmitting a success message.
	 */
	
	/**
	 *
	 */
	public transient HashMap<String,String> state;
	
	/**
	 * 
	 */
	public transient HashMap<String,HashMap<String,ArrayList<ComponentError>>> errors;
	
	/**
	 * 
	 */
	public transient HashMap<String,List<String>> components;
	
	/*
	 * Holding variables for collecting all of the responses.
	 */
	
	/**
	 * 
	 */
	public transient final HashMap<String,HashMap<String,String>> descState;
	
	/**
	 * 
	 */
	public transient final HashMap<String,HashMap<String,HashMap<String,ArrayList<ComponentError>>>> descErrors;
	
	/**
	 * 
	 */
	public transient final HashMap<String,HashMap<String,List<String>>> descComponents;
	
	/**
	 * @param success
	 * @param failure
	 */
	public ListToposGroupOp(IOpSuccess success, IOpFailure failure) {
		super(success,failure);
		descState=new HashMap<>();
		descErrors=new HashMap<>();
		descComponents=new HashMap<>();
	}
	
	/**
	 * @param desc
	 * @param state
	 * @param errors
	 */
	public synchronized void aggregate(NodeDescriptor desc,
			HashMap<String,String> state,
			HashMap<String,HashMap<String,ArrayList<ComponentError>>> errors,
			HashMap<String,List<String>> comps) {
			descState.put(desc.toString(),state);
			descErrors.put(desc.toString(),errors);
			descComponents.put(desc.toString(),comps);
	}

	/* (non-Javadoc)
	 * @see dragon.network.operations.GroupOp#initiateNodeMessage(dragon.network.NodeDescriptor)
	 */
	@Override
	public NodeMessage initiateNodeMessage(NodeDescriptor desc) {
		return new GetTopoInfoNMsg();
	}
	
	/* (non-Javadoc)
	 * @see dragon.network.operations.GroupOp#successNodeMessage()
	 */
	@Override
	public NodeMessage successNodeMessage() {
		return new TopoInfoNMsg(state,errors,components);
	}

	/* (non-Javadoc)
	 * @see dragon.network.operations.GroupOp#errorNodeMessage(java.lang.String)
	 */
	@Override
	protected NodeMessage errorNodeMessage(String error) {
		// TODO Auto-generated method stub
		return null;
	}

}
