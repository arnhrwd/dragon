package dragon.tuple;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Recycler<T> {
	private final static Log log = LogFactory.getLog(Recycler.class);
	private final int expansion;
	private final T obj;
	private final double compact;
	
	
	private final AtomicInteger capacity;
	private AtomicReferenceArray<T> objects;
	private int next;
	private final HashMap<T,Integer> map;
	private final HashMap<T,AtomicInteger> refCount;
		
	@SuppressWarnings("unchecked")
	public Recycler(T obj,int capacity,int expansion, double compact) {
		next=0;
		objects = new AtomicReferenceArray<T>(capacity);
		refCount = new HashMap<T,AtomicInteger>(capacity);
		this.capacity=new AtomicInteger(capacity);
		this.expansion=expansion;
		this.obj=obj;
		this.compact=compact;
		map=new HashMap<T,Integer>(capacity);
		for(int i=0;i<capacity;i++) {
			T t = (T) ((IRecyclable)obj).newRecyclable();
			objects.set(i, t);
			refCount.put(objects.get(i),new AtomicInteger(0));
			map.put(objects.get(i), i);
		}
	}
	
	public void recycleObject(T t) {
		final AtomicInteger capacity = this.capacity;
		final AtomicReferenceArray<T> objects = this.objects;
		((IRecyclable)t).recycle();
		synchronized(map) {
			int r = map.get(t);
			next--;
			if(r==next) return;
			T rn = objects.get(next);
			objects.set(next,t);
			objects.set(r, rn);
			map.put(t,next);
			map.put(rn,r);
			final int c=capacity.get();
			if(next>expansion && next<c*compact) {
				log.warn(obj.getClass().getName()+" excess capacity "+((float)next/c));
				final int newCapacity=c/2;
				AtomicReferenceArray<T> newObjects = new AtomicReferenceArray<T>(newCapacity);
				for(int i=0;i<newCapacity;i++) {
					newObjects.set(i, objects.get(i));
				}
				for(int i=newCapacity;i<c;i++) {
					map.remove(objects.get(newCapacity));
					refCount.remove(objects.get(newCapacity));
				}
				this.objects=newObjects;
				capacity.set(newCapacity);
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	public T newObject() {
		final AtomicInteger capacity=this.capacity;
		final AtomicReferenceArray<T> objects=this.objects;
		synchronized(map) {
			final int c=capacity.get();
			if(next==c) {
				log.warn(obj.getClass().getName()+" capacity reached, expanding by "+expansion+" to "+(c+expansion));
				final AtomicReferenceArray<T> newObjects = new AtomicReferenceArray<T>(c+expansion);
				for(int i=0;i<c+expansion;i++) {
					if(i<c)
						newObjects.set(i, objects.get(i));
					else {
						newObjects.set(i, (T) ((IRecyclable)obj).newRecyclable());
						map.put(newObjects.get(i),i);
						refCount.put(newObjects.get(i),new AtomicInteger(0));
					}
				}
				this.objects=newObjects;
				capacity.addAndGet(expansion);
			}
			T t = objects.get(next);
			next++;
			shareRecyclable(t,1);
			return t;
		}
	}

	public void shareRecyclable(T t,int n) {
		synchronized(map) {
			refCount.get(t).addAndGet(n);
		}
	}

	public void crushRecyclable(T t,int n) {
		synchronized(map) {
			long c = refCount.get(t).addAndGet(-n);
			if(c==0) {
				recycleObject(t);
			}
		}
	}
}
