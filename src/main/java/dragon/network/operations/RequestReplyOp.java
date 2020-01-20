package dragon.network.operations;

public class RequestReplyOp extends Op {
	private static final long serialVersionUID = -570864536417685701L;
	
	public RequestReplyOp(IOpStart start,IOpSuccess success, IOpFailure failure) {
		super(start,success, failure);
	}

	public RequestReplyOp() {
		super();
	}

}
