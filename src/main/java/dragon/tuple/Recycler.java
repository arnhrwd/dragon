package dragon.tuple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dragon.utils.CircularBlockingQueue;

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
	private final int expansion;
	
	/**
	 * 
	 */
	private final T obj;
	
	/**
	 * 
	 */
	private final double compact;
	
	private int capacity;
//	private final AtomicInteger capacity;
//	private AtomicReferenceArray<T> objects;
//	private int next;
//	private final HashMap<T,Integer> map;
	
	/**
	 * 
	 */
	//private volatile CircularBlockingQueue<T> objects;
	//private volatile Stack<T> objects;
	private volatile ArrayList<T> objects;
	private volatile int stackIndex;
	
	/**
	 * 
	 */
	private final Map<T,AtomicInteger> refCount;
	
	/**
	 * 
	 */
	private final ReentrantLock lock = new ReentrantLock();
		
	/**
	 * @param obj
	 * @param capacity
	 * @param expansion
	 * @param compact
	 */
	@SuppressWarnings("unchecked")
	public Recycler(T obj,int capacity,int expansion, double compact) {
		//objects = new CircularBlockingQueue<T>(capacity);
		objects = new ArrayList<T>(capacity);
		refCount = (Map<T, AtomicInteger>) Collections.synchronizedMap(new HashMap<T, AtomicInteger>(capacity));//new HashMap<T,AtomicInteger>(capacity);
		//this.capacity=new AtomicInteger(capacity);
		this.expansion=expansion;
		this.obj=obj;
		this.compact=compact;
		this.capacity=capacity;
		this.stackIndex=this.capacity-1;
		//map=new HashMap<T,Integer>(capacity);
		for(int i=0;i<capacity;i++) {
			T t = (T) ((IRecyclable)obj).newRecyclable();
//			try {
				objects.add(t);
//			} catch (InterruptedException e) {
//				log.error("could not put on queue");
//			}
			refCount.put(t,new AtomicInteger(0));
			//map.put(objects.get(i), i);
		}
	}
	
	/**
	 * @param t
	 */
	public void recycleObject(T t) {
		((IRecyclable)t).recycle();
		lock.lock();
		try {
			stackIndex++;
			objects.set(stackIndex,t);
//			if(objects.size()>expansion &&
//					objects.remainingCapacity()<objects.getCapacity()*compact) {
//				final int newCapacity = (int)(objects.getCapacity()*0.5);
//				final int c1 = objects.remainingCapacity();
//				log.warn(obj.getClass().getName()+" shrinking to "+(newCapacity));
//				CircularBlockingQueue<T> newObjects = new CircularBlockingQueue<T>(newCapacity);
//				for(int i=0;i<newCapacity-c1;i++) {
//					newObjects.put(objects.take()); // ref counts are retained in refCount
//				}
//				while(objects.size()>0) {
//					refCount.remove(objects.take()); // remove these unneeded ref counts
//				}
//				objects=newObjects;
//			}
//		} catch (InterruptedException e) {
//			log.error("could not recycle object");
		} finally {
			lock.unlock();
		}

	}
	
	/**
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public T newObject() {
		lock.lock();
		try {
			T t;
			if(stackIndex>=0)
				t= objects.get(stackIndex--);
			else {
//				final int capacity = objects.getCapacity();
				log.warn(obj.getClass().getName()+" expanding by "+expansion+" to "+(capacity+expansion));
				capacity=capacity+expansion;
//				this.objects = new CircularBlockingQueue<T>(capacity+expansion);
				for(int i=0;i<expansion;i++) {
					T tp = (T) ((IRecyclable)obj).newRecyclable();
//					try
						objects.add(0,tp);
//					} catch (InterruptedException e) {
//						log.error("could not put new object on queue");
//					}
					refCount.put(tp,new AtomicInteger(0));
				}
				stackIndex+=expansion;
//				try {
					t=objects.get(stackIndex--);
//				} catch (InterruptedException e) {
//					log.error("could not get new object from queue");
//				}
			}
			shareRecyclable(t,1);
			return t;
		} finally {
			lock.unlock();
		}
	}
	
	
	public void fillPool(T[] pool) {
		lock.lock();
		try {
			for(int j=0;j<pool.length;j++) {
				T t;
				if(stackIndex>=0)
					t= objects.get(stackIndex--);
				else {
	//				final int capacity = objects.getCapacity();
					log.warn(obj.getClass().getName()+" expanding by "+expansion+" to "+(capacity+expansion));
					capacity=capacity+expansion;
	//				this.objects = new CircularBlockingQueue<T>(capacity+expansion);
					for(int i=0;i<expansion;i++) {
						T tp = (T) ((IRecyclable)obj).newRecyclable();
	//					try
							objects.add(0,tp);
	//					} catch (InterruptedException e) {
	//						log.error("could not put new object on queue");
	//					}
						refCount.put(tp,new AtomicInteger(0));
					}
					stackIndex+=expansion;
	//				try {
						t=objects.get(stackIndex--);
	//				} catch (InterruptedException e) {
	//					log.error("could not get new object from queue");
	//				}
				}
				shareRecyclable(t,1);
				pool[j]=t;
			}
		} finally {
			lock.unlock();
		}
	}

	/**
	 * @param t
	 * @param n
	 */
	public void shareRecyclable(T t,int n) {
		refCount.get(t).addAndGet(n);
	}
	
	/**
	 * 
	 * @param t
	 * @param n
	 */
	public void shareRecyclables(T[] t,int n) {
		for(int i=0;i<t.length;i++) {
			if(t[i]!=null) shareRecyclable(t[i],n);
			else break;
		}
	}

	/**
	 * @param t
	 * @param n
	 */
	public void crushRecyclable(T t,int n) {
		long c = refCount.get(t).addAndGet(-n);
		if(c==0) {
			recycleObject(t);
		}	
	}
	
	/**
	 * 
	 * @param t
	 * @param n
	 */
	public void crushRecyclables(T[] t,int n) {
		for(int i=0;i<t.length;i++) {
			if(t[i]!=null)crushRecyclable(t[i],n);
			else break;
		}
	}
}
