package dragon.tuple;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

public class RecyclableObject implements Serializable, IRecyclable {
	private static final long serialVersionUID = 5381181467173442671L;
	public transient AtomicLong count=new AtomicLong(0);
	private transient Recycler<RecyclableObject> recycler;
	
	@SuppressWarnings("unchecked")
	@Override
	public void setRecycler(Recycler<?> recycler) {
		this.recycler=(Recycler<RecyclableObject>)recycler;
	}
	
	@Override
	public void recycle() {
		
	}

	@Override
	public IRecyclable newRecyclable() {
		return new RecyclableObject();
	}

	@Override
	public void shareRecyclable(int n) {
		count.addAndGet(n);
	}

	@Override
	public void crushRecyclable(int n) {
		long c = count.addAndGet(-n);
		if(c==0) {
			recycler.recycleObject(this);
		}
	}

	

}
