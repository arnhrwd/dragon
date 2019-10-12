package dragon.network.messages.service;

import dragon.network.messages.IErrorMessage;

public class GetMetricsErrorSMsg extends ServiceMessage implements IErrorMessage {
	private static final long serialVersionUID = 779753438805275630L;
	public final String error;
	public GetMetricsErrorSMsg(String error) {
		super(ServiceMessage.ServiceMessageType.GET_METRICS_ERROR);
		this.error=error;
	}
	@Override
	public String getError() {
		return error;
	}
	
}
