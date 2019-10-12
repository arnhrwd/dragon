package dragon.network.messages;

import java.io.Serializable;

import dragon.network.operations.GroupOp;


public class Message implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -6123498202112069826L;
	
	
	private String messageId;
	private GroupOp groupOperation;
	
	public Message(){
		this.messageId="";
	}
	
	public void setMessageId(String messageId) {
		this.messageId=messageId;
	}
	
	public String getMessageId() {
		return messageId;
	}
	
	public void setGroupOperation(GroupOp groupOperation) {
		this.groupOperation=groupOperation;
	}
	
	public GroupOp getGroupOperation() {
		return this.groupOperation;
	}
	
}
