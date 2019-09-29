package dragon.tuple;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Recycler<T> {
	private static Log log = LogFactory.getLog(Recycler.class);
	private int capacity;
	private final int expansion;
	private final ArrayList<T> objects;
	private int next;
	private final HashMap<T,Integer> map;
	private final T obj;
	private double compact;
		
	@SuppressWarnings("unchecked")
	public Recycler(T obj,int capacity,int expansion, double compact) {
		next=0;
		objects = new ArrayList<T>(capacity);
		this.capacity=capacity;
		this.expansion=expansion;
		this.obj=obj;
		this.compact=compact;
		map=new HashMap<T,Integer>(capacity);
		for(int i=0;i<capacity;i++) {
			T t = (T) ((IRecyclable)obj).newRecyclable();
			objects.add(t);
			map.put(objects.get(i), i);
			((IRecyclable)objects.get(i)).setRecycler(this);
		}
	}
	
	public void recycleObject(T t) {
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
			if(next>expansion && next<capacity*compact) {
				log.warn(obj.getClass().getName()+" excess capacity "+((float)next/capacity));
				int newCapacity=capacity/2;
				for(int i=newCapacity;i<capacity;i++) {
					map.remove(objects.get(newCapacity));
					objects.remove(newCapacity);
				}
				objects.trimToSize();
				capacity=newCapacity;
			}
		}
	}
	
	
	@SuppressWarnings("unchecked")
	public T newObject() {
		synchronized(map) {
			if(next==capacity) {
				log.warn(obj.getClass().getName()+" capacity reached, expanding by "+expansion+" to "+(capacity+expansion));
				objects.ensureCapacity(capacity+expansion);
				for(int i=capacity;i<capacity+expansion;i++) {
					objects.add(i, (T) ((IRecyclable)obj).newRecyclable());
					map.put(objects.get(i),i);
					((IRecyclable)objects.get(i)).setRecycler(this);
				}
				capacity+=expansion;
			}
			T t = objects.get(next);
			next++;
			((IRecyclable)t).shareRecyclable(1);
			return t;
		}
	}
}
