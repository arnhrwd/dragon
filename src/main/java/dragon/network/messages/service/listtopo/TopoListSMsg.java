package dragon.network.messages.service.listtopo;

import java.util.ArrayList;
import java.util.HashMap;

import dragon.ComponentError;
import dragon.network.messages.service.ServiceMessage;

/**
 * @author aaron
 *
 */
public class TopoListSMsg extends ServiceMessage {
	private static final long serialVersionUID = -4010036400846816662L;
	
	/**
	 * 
	 */
	public final HashMap<String, HashMap<String, String>> descState;
	
	/**
	 * 
	 */
	public final HashMap<String, HashMap<String, HashMap<String, ArrayList<ComponentError>>>> descErrors;
	
	/**
	 * @param descState
	 * @param descErrors
	 */
	public TopoListSMsg(HashMap<String, HashMap<String, String>> descState, 
			HashMap<String, HashMap<String, HashMap<String, ArrayList<ComponentError>>>> descErrors) {
		super(ServiceMessage.ServiceMessageType.TOPOLOGY_LIST);		
		this.descState=descState;
		this.descErrors=descErrors;
	}
}
