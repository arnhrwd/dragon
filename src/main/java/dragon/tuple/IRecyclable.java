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
	
	/**
	 * Increment the number of references to the object.
	 */
	public void shareRecyclable(int n);
	
	/**
	 * Decrement the number of references to the object.
	 * @return true if the object can be recycled, i.e. there are no references to it.
	 */
	public void crushRecyclable(int n);

	/**
	 * Set which recycler this object uses.
	 * @param recycler
	 */
	void setRecycler(Recycler<?> recycler);
}
