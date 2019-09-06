package dragon.topology.base;

@Deprecated
public interface IRichBolt  {
	

	public void ack(Object id);
	

	public void fail(Object id);
	
}
