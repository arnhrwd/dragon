package dragon.tuple;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.utils.CircularBlockingQueue;

public class Recycler<T> {
	private final static Log log = LogFactory.getLog(Recycler.class);
	private final int expansion;
	private final T obj;
	private final double compact;
	
	
//	private final AtomicInteger capacity;
//	private AtomicReferenceArray<T> objects;
//	private int next;
//	private final HashMap<T,Integer> map;
	private volatile CircularBlockingQueue<T> objects;
	private final Map<T,AtomicInteger> refCount;
	private final ReentrantLock lock = new ReentrantLock();
		
	@SuppressWarnings("unchecked")
	public Recycler(T obj,int capacity,int expansion, double compact) {
		objects = new CircularBlockingQueue<T>(capacity);
		refCount = (Map<T, AtomicInteger>) Collections.synchronizedMap(new HashMap<T, AtomicInteger>(capacity));//new HashMap<T,AtomicInteger>(capacity);
		//this.capacity=new AtomicInteger(capacity);
		this.expansion=expansion;
		this.obj=obj;
		this.compact=compact;
		//map=new HashMap<T,Integer>(capacity);
		for(int i=0;i<capacity;i++) {
			T t = (T) ((IRecyclable)obj).newRecyclable();
			try {
				objects.put(t);
			} catch (InterruptedException e) {
				log.error("could not put on queue");
			}
			refCount.put(t,new AtomicInteger(0));
			//map.put(objects.get(i), i);
		}
	}
	
	public void recycleObject(T t) {
		((IRecyclable)t).recycle();
		lock.lock();
		try {
			objects.put(t);
//			if(objects.size()>expansion &&
//					objects.remainingCapacity()<objects.getCapacity()*compact) {
//				final int newCapacity = (int)(objects.getCapacity()*0.5);
//				final int c1 = objects.remainingCapacity();
//				log.debug("used objects = "+c1);
//				log.warn(obj.getClass().getName()+" shrinking to "+(newCapacity));
//				CircularBlockingQueue<T> newObjects = new CircularBlockingQueue<T>(newCapacity);
//				log.debug("objects size = "+objects.size());
//				for(int i=0;i<newCapacity;i++) {
//					T t1 = objects.take();
//					if(refCount.containsKey(t1)) {
//						if(refCount.get(t1).get()!=0) {
//							System.out.println("error");
//							System.exit(-1);
//						}
//					} else {
//						System.out.println("error2");
//						System.exit(-1);
//					}
//					newObjects.put(t1); // ref counts are retained in refCount
//				}
//				log.debug("objects size = "+objects.size());
//				while(objects.size()>0) {
//					refCount.remove(objects.take()); // remove these unneeded ref counts
//				}
//				log.debug("objects size = "+objects.size());
//				objects=newObjects;
//				log.debug("finished shrinking");
//				if(c1<objects.remainingCapacity()) {
//					System.out.println("error3");
//					System.exit(-1);
//				}
//			}
		} catch (InterruptedException e) {
			log.error("could not recycle object");
		} finally {
			lock.unlock();
		}

	}
	
	@SuppressWarnings("unchecked")
	public T newObject() {
		lock.lock();
		try {
			T t = objects.poll();
			if(t==null) {
				final int capacity = objects.getCapacity();
				log.warn(obj.getClass().getName()+" expanding by "+expansion+" to "+(capacity+expansion));
				this.objects = new CircularBlockingQueue<T>(capacity+expansion);
				for(int i=0;i<expansion;i++) {
					T tp = (T) ((IRecyclable)obj).newRecyclable();
					try {
						objects.put(tp);
					} catch (InterruptedException e) {
						log.error("could not put new object on queue");
					}
					refCount.put(tp,new AtomicInteger(0));
				}
				try {
					t=objects.take();
				} catch (InterruptedException e) {
					log.error("could not get new object from queue");
				}
			}
			shareRecyclable(t,1);
			return t;
		} finally {
			lock.unlock();
		}
	}

	public void shareRecyclable(T t,int n) {
		if(!refCount.containsKey(t)) {
			System.out.println("error4");
			System.exit(-1);
		}
		refCount.get(t).addAndGet(n);
	}

	public void crushRecyclable(T t,int n) {
		if(!refCount.containsKey(t)) {
			System.out.println("error5");
			System.exit(-1);
		}
		long c = refCount.get(t).addAndGet(-n);
		if(c==0) {
			recycleObject(t);
		}
		
	}
}
