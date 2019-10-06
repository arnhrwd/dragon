package dragon.network.messages.service;

import java.util.ArrayList;
import java.util.HashMap;

import dragon.ComponentError;

public class TopologyListMessage extends ServiceMessage {
	private static final long serialVersionUID = -4010036400846816662L;
	public final HashMap<String, HashMap<String, String>> descState;
	public final HashMap<String, HashMap<String, HashMap<String, ArrayList<ComponentError>>>> descErrors;
	public TopologyListMessage(HashMap<String, HashMap<String, String>> descState, 
			HashMap<String, HashMap<String, HashMap<String, ArrayList<ComponentError>>>> descErrors) {
		super(ServiceMessage.ServiceMessageType.TOPOLOGY_LIST);		
		this.descState=descState;
		this.descErrors=descErrors;
	}
}
