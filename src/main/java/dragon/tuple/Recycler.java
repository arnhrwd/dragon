package dragon.tuple;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Recycler<T> {
	private static Log log = LogFactory.getLog(Recycler.class);
	private int capacity;
	private int expansion;
	private ArrayList<T> objects;
	private ArrayList<Integer> available;
	private int next;
	private HashMap<T,Integer> map;
	private T obj;

	@SuppressWarnings("unchecked")
	public Recycler(T obj,int capacity,int expansion) {
		next=0;
		objects = new ArrayList<T>(capacity); //(T[]) new Object[capacity];
		available = new ArrayList<Integer>(capacity);
		this.capacity=capacity;
		this.expansion=expansion;
		this.obj=obj;
		map=new HashMap<T,Integer>();
		for(int i=0;i<capacity;i++) {
			objects.add(i,(T) ((IRecyclable)obj).newRecyclable());
			map.put(objects.get(i), i);
			available.add(i,i);
		}
	}
	
	public void recycleObject(T t) {
		((IRecyclable)t).recycle();
		synchronized(map) {
			next--;
			available.set(next, map.get(t));
		}
	}
	
	@SuppressWarnings("unchecked")
	public T newObject() {
		synchronized(map) {
			if(next==capacity) {
				log.warn(obj.getClass().getName()+" capacity reached, expanding by "+expansion+" to "+(capacity+expansion));
				available.ensureCapacity(capacity+expansion);
				objects.ensureCapacity(capacity+expansion);
				for(int i=capacity;i<capacity+expansion;i++) {
					objects.add(i, (T) ((IRecyclable)obj).newRecyclable());
					map.put(objects.get(i),i);
					available.add(i, i);
				}
				capacity+=expansion;
			}
			T t = objects.get(available.get(next));
			next++;
			((IRecyclable)t).shareRecyclable(1);
			((IRecyclable)t).setRecycler(this);
			return t;
		}
	}
}
