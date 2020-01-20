package dragon.tuple;

public interface IRecyclable {
	/**
	 * Called when the object is being returned for recycling.
	 * Allows the object to subsequently
	 * recycle any of its fields that are also recyclable.
	 */
	public void recycle();
	
	/**
	 * Like clone, used for expanding the capacity of the recycler.
	 * @return a new object that is ready to be recycled.
	 */
	public IRecyclable newRecyclable();
	
}
