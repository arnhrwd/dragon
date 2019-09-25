package dragon.network.messages;

import java.io.Serializable;

import dragon.network.GroupOperation;


public class Message implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -6123498202112069826L;
	
	
	private String messageId;
	private GroupOperation groupOperation;
	
	public Message(){
		this.messageId="";
	}
	
	public void setMessageId(String messageId) {
		this.messageId=messageId;
	}
	
	public String getMessageId() {
		return messageId;
	}
	
	public void setGroupOperation(GroupOperation groupOperation) {
		this.groupOperation=groupOperation;
	}
	
	public GroupOperation getGroupOperation() {
		return this.groupOperation;
	}
	
}
