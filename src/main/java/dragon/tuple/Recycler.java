package dragon.tuple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author aaron
 *
 * @param <T>
 */
public class Recycler<T> {
	private final static Logger log = LogManager.getLogger(Recycler.class);
	
	/**
	 * 
	 */
	private int expansion;
	
	/**
	 * 
	 */
	private final T obj;
	
	/**
	 * 
	 */
	private final double compact;
	
	/**
	 * 
	 */
	private int capacity;
	
	/**
	 * 
	 */
	private final Map<Integer,AtomicReference<T>> objects;
	
	/**
	 * 
	 */
	private int stackIndex;
	
	/**
	 * 
	 */
	private final Map<T,AtomicInteger> refCount;
	
	/**
	 * 
	 */
	//private final Lock lock = new ReentrantLock();
		
	/**
	 * Initialize the recycler.
	 * @param obj
	 * @param capacity
	 * @param expansion
	 * @param compact
	 */
	@SuppressWarnings("unchecked")
	public Recycler(T obj,int capacity,int expansion, double compact) {
		objects = new HashMap<Integer,AtomicReference<T>>(capacity);
		refCount = new HashMap<T, AtomicInteger>(capacity);
		this.expansion=expansion;
		this.obj=obj;
		this.compact=compact;
		this.capacity=capacity;
		this.stackIndex=0; // stack starts at 0 and increases as we go deeper
	
		for(int i=0;i<capacity;i++) {
			T t = (T) ((IRecyclable)obj).newRecyclable();
			objects.put(i,new AtomicReference<T>(t));
			refCount.put(t,new AtomicInteger(0));
		}
	}
	
	/**
	 * Return an object for recycling, which puts
	 * it back on the stack.
	 * @param t
	 */
	private void _recycle(T t) {
		stackIndex--;
		objects.get(stackIndex).set(t);
	}
	
	/**
	 * Obtain an object, which takes it off the stack.
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private T _newObject() {
		T t;
		if(stackIndex<capacity)
			t= objects.get(stackIndex++).get();
		else {
			log.debug(obj.getClass().getName()+" expanding by "+expansion+" to "+(capacity+expansion));
			for(int i=0;i<expansion;i++) {
				T tp = (T) ((IRecyclable)obj).newRecyclable();
				objects.put(i+capacity,new AtomicReference<T>(tp));
				refCount.put(tp,new AtomicInteger(0));
			}
			capacity=capacity+expansion;
			expansion<<=1;
			t=objects.get(stackIndex++).get();
		}
		shareRecyclable(t,1);
		return t;
	}
	
	/**
	 * Recycle an object.
	 * @param t
	 */
	public synchronized void recycleObject(T t) {
		((IRecyclable)t).recycle();
		//lock.lock();
		try {
			_recycle(t);
		} finally {
			//lock.unlock();
		}

	}
	
	/**
	 * Return a new object.
	 * @return
	 */
	public synchronized T newObject() {
		//lock.lock();
		try {
			return _newObject();
		} finally {
			//lock.unlock();
		}
	}
	
	/**
	 * Fill the array with objects.
	 * @param pool
	 */
	public synchronized void fillPool(T[] pool) {
		//lock.lock();
		try {
			for(int j=0;j<pool.length;j++) {
				pool[j]=_newObject();
			}
		} finally {
			//lock.unlock();
		}
	}

	/**
	 * Increase the reference count of t by n.
	 * @param t
	 * @param n
	 */
	public synchronized void shareRecyclable(T t,int n) {
		refCount.get(t).addAndGet(n);
	}
	
	/**
	 * Increase the reference count of all t up
	 * to the first null by n.
	 * @param t
	 * @param n
	 */
	public synchronized void shareRecyclables(T[] t,int n) {
		for(int i=0;i<t.length;i++) {
			if(t[i]!=null) refCount.get(t[i]).addAndGet(n);
			else break;
		}
	}

	/**
	 * Reduce the reference count of t by n and
	 * recycle the object if it reaches 0.
	 * @param t
	 * @param n
	 */
	public synchronized void crushRecyclable(T t,int n) {
		long c = refCount.get(t).addAndGet(-n);
		if(c==0) {
			((IRecyclable)t).recycle();
			//lock.lock();
			try {
				_recycle(t);
			} finally {
				//lock.unlock();
			}
		}	
	}
	
	/**
	 * Reduce the reference count of all t up to
	 * the first null by n and recycle those that
	 * reach 0.
	 * @param t
	 * @param n
	 */
	public synchronized void crushRecyclables(T[] t,int n) {
		boolean hit=true;
		long c;
		for(int i=0;i<t.length;i++) {
			if(t[i]!=null) {
				c = refCount.get(t[i]).addAndGet(-n);
				if(c==0) hit=true;
			} else break;
		}
		if(hit) {
			//lock.lock();
			try {
				for(int i=0;i<t.length;i++) {
					if(t[i]!=null) {
						if(refCount.get(t[i]).get()==0) {
							((IRecyclable)t[i]).recycle();
							_recycle(t[i]);
						}
					} else break;
				}
			} finally {
				//lock.unlock();
			}
		}
	}
}
