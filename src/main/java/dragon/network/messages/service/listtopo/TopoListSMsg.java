package dragon.network.messages.service.listtopo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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
	 * 
	 */
	public final HashMap<String,HashMap<String,List<String>>> descComponents;
	
	/**
	 * @param descState
	 * @param descErrors
	 * @param descComponents
	 */
	public TopoListSMsg(HashMap<String, HashMap<String, String>> descState, 
			HashMap<String, HashMap<String, HashMap<String, ArrayList<ComponentError>>>> descErrors,
			HashMap<String,HashMap<String,List<String>>> descComponents) {
		super(ServiceMessage.ServiceMessageType.TOPOLOGY_LIST);		
		this.descState=descState;
		this.descErrors=descErrors;
		this.descComponents=descComponents;
	}
}
