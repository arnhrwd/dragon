package dragon.network.messages;

import java.io.Serializable;


public class Message implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -6123498202112069826L;
	
	
	private String messageId;
	
	public Message(){
		this.messageId="";
	}
	
	public void setMessageId(String messageId) {
		this.messageId=messageId;
	}
	
	public String getMessageId() {
		return messageId;
	}
	
}
