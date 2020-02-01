package dragon.network.operations;

/**
 * @author aaron
 *
 */
public class ConditionalOp extends Op {
	private static final long serialVersionUID = 6530183395216329821L;
	
	/**
	 * 
	 */
	private final IOpCondition condition;
	
	/**
	 * @param condition
	 * @param success
	 * @param failure
	 */
	public ConditionalOp(IOpCondition condition,IOpSuccess success,IOpFailure failure) {
		super(success,failure);
		this.condition=condition;
	}
	
	/**
	 * @return
	 */
	public boolean check() {
		if(condition.condition(this)) {
			success();
			return true;
		}
		return false;
	}
}
