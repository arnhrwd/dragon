package dragon.network.messages.service.execRlAction;

import dragon.network.messages.service.ServiceErrorMessage;
import dragon.network.messages.service.ServiceMessage;

public class ExecRlActionErrorSMsg extends ServiceErrorMessage {
	
	private static final long serialVersionUID = -6863400143355788335L;
	
	public ExecRlActionErrorSMsg(String error) {
		super(ServiceMessage.ServiceMessageType.EXEC_RL_ACTION_ERROR, error);
	}

}
