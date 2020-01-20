package dragon.network.messages.service;

import dragon.network.messages.IErrorMessage;

public class UploadJarFailedSMsg extends ServiceMessage implements IErrorMessage {
	private static final long serialVersionUID = -562124896589218017L;
	public final String topologyId;
	public final String error;
	public UploadJarFailedSMsg(String topologyId,String error) {
		super(ServiceMessage.ServiceMessageType.UPLOAD_JAR_FAILED);
		this.topologyId=topologyId;
		this.error=error;
	}
	@Override
	public String getError() {
		// TODO Auto-generated method stub
		return null;
	}

}
