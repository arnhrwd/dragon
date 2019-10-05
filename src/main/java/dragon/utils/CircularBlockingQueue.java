package dragon.utils;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class CircularBlockingQueue<T> extends AbstractQueue<T>
         implements BlockingQueue<T> {
	private AtomicReferenceArray<T> elements;
	private final AtomicInteger head=new AtomicInteger(0);
	private final AtomicInteger tail=new AtomicInteger(0);
	private final int capacity;
	private final AtomicInteger count = new AtomicInteger(0);
	private final ReentrantLock putLock = new ReentrantLock();
	private final ReentrantLock takeLock = new ReentrantLock();
	private final Condition notEmpty = takeLock.newCondition();
	private final Condition notFull = putLock.newCondition();
	
	public final ReentrantLock bufferLock = new ReentrantLock();
	
	public CircularBlockingQueue(){
		capacity=1024;
		elements = new AtomicReferenceArray<T>(capacity);
		for(int i=0;i<capacity;i++) {
			elements.set(i,null);
		}
	}

	public CircularBlockingQueue(int capacity){
		this.capacity=capacity+1;
		elements = new AtomicReferenceArray<T>(this.capacity);
		for(int i=0;i<this.capacity;i++) {
			elements.set(i,null);
		}
	}
	
	public int getCapacity() {
		return capacity;
	}
	
	private void signalNotEmpty() {
		final ReentrantLock takeLock = this.takeLock;
		takeLock.lock();
		try {
			notEmpty.signal();
		} finally {
			takeLock.unlock();
		}
	}

	private void signalNotFull() {
		final ReentrantLock putLock = this.putLock;
		putLock.lock();
		try {
			notFull.signal();
		} finally {
			putLock.unlock();
		}
	}

	private void fullyLock() {
		putLock.lock();
		takeLock.lock();
	}

	private void fullyUnlock() {
		takeLock.unlock();
		putLock.unlock();
	}

	public int size() {
		return count.get();
	}
	
	private static int quickNext(int x,int max) {
		return (x+1 < max) ? x+1 : 0;
	}
	
	@Override
	public void put(T element) throws InterruptedException {
		if(element==null) throw new NullPointerException();
		final ReentrantLock putLock = this.putLock;
		final AtomicInteger count = this.count;
		final AtomicInteger tail = this.tail;
		int c=-1;
		putLock.lockInterruptibly();
		try {
			try {
				while(count.get() == capacity)
					notFull.await();
			} catch (InterruptedException ie) {
				notFull.signal();
				throw ie;
			}
			elements.set(tail.get(),element);
			tail.set(quickNext(tail.get(),capacity));
			c = count.getAndIncrement();
			if (c + 1 < capacity)
				notFull.signal();
		} finally {
			putLock.unlock();
		}
		if (c == 0)
			signalNotEmpty();
	}
	
	@Override
	public boolean offer(T element, long timeout, TimeUnit unit) throws InterruptedException{
		if(element==null) throw new NullPointerException();
		long nanos=unit.toNanos(timeout);
		int c=-1;
		final ReentrantLock putLock = this.putLock;
		final AtomicInteger count = this.count;
		final AtomicInteger tail = this.tail;
		putLock.lockInterruptibly();
		try {
			for (;;) {
				if (count.get()<capacity) {
					elements.set(tail.get(),element);
					tail.set(quickNext(tail.get(),capacity));
					c = count.getAndIncrement();
					if(c+1<capacity) {
						notFull.signal();
					}
					break;
				}
				if (nanos <= 0)
					return false;
				try {
					nanos=notFull.awaitNanos(nanos);
				} catch (InterruptedException ie) {
					notFull.signal();
					throw ie;
				}
			}
		} finally {
			putLock.unlock();
		}
		if(c==0)
			signalNotEmpty();
		return true;
	}
	
	@Override
	public boolean offer(T element) {
		if(element==null) throw new NullPointerException();
		final AtomicInteger count = this.count;
		if(count.get()==capacity) return false;
		int c=-1;
		final AtomicInteger tail = this.tail;
		final ReentrantLock putLock = this.putLock;
		putLock.lock();
		try {
			if (count.get()<capacity) {
				elements.set(tail.get(),element);
				tail.set(quickNext(tail.get(),capacity));
				c = count.getAndIncrement();
				if(c+1<capacity) {
					notFull.signal();
				}
			}
		} finally {
			putLock.unlock();
		}
		if(c==0)
			signalNotEmpty();
		return c>=0;
	}
	
	@Override
	public T take() throws InterruptedException {
		T element;
		int c = -1;
		final AtomicInteger count = this.count;
		final ReentrantLock takeLock = this.takeLock;
		final AtomicInteger head = this.head;
		takeLock.lockInterruptibly();
		try {
			try {
				while (count.get() == 0)
					notEmpty.await();
			} catch (InterruptedException ie) {
				notEmpty.signal(); // propagate to a non-interrupted thread
				throw ie;
			}
			element = elements.getAndSet(head.get(),null);
			head.set(quickNext(head.get(),capacity));
			c = count.getAndDecrement();
			if (c > 1)
				notEmpty.signal();
		} finally {
			takeLock.unlock();
		}
		if (c==capacity)
			signalNotFull();
		return element;
	}
	
	@Override
	public T poll(long timeout, TimeUnit unit) throws InterruptedException{
		T element = null;
		int c=-1;
		long nanos=unit.toNanos(timeout);
		final AtomicInteger count=this.count;
		final ReentrantLock takeLock = this.takeLock;
		final AtomicInteger head = this.head;
		takeLock.lockInterruptibly();
		try {
			for (;;) {
				if (count.get()>0) {
					element = elements.getAndSet(head.get(),null);
					head.set(quickNext(head.get(),capacity));
					c=count.getAndDecrement();
					if(c>1) {
						notEmpty.signal();
					}
					break;
				}
				if (nanos <=0){
					return null;
				}
				try {
					nanos=notEmpty.awaitNanos(nanos);
				} catch(InterruptedException ie) {
					notEmpty.signal();
					throw ie;
				}
			}
		} finally {
			takeLock.unlock();
		}
		if (c==capacity)
			signalNotFull();
		return element;
	}
	
	@Override
	public T poll() {
		final AtomicInteger count = this.count;
		if (count.get() == 0)
			return null;
		T element = null;
		int c = -1;
		final ReentrantLock takeLock = this.takeLock;
		final AtomicInteger head = this.head;
		takeLock.lock();
		try {
			if (count.get() > 0) {
				element = elements.getAndSet(head.get(),null);
				head.set(quickNext(head.get(), capacity));
				c = count.getAndDecrement();
				if (c > 1) {
					notEmpty.signal();
				}
			}
		} finally {
			takeLock.unlock();
		}
		if (c == capacity)
			signalNotFull();
		return element;
	}

	@Override
	public T peek(){
		if (count.get()==0) return null;
		final ReentrantLock takeLock = this.takeLock;
		final AtomicInteger head = this.head;
		takeLock.lock();
		try {
			if(count.get()>0) {
				return elements.get(head.get());
			}
			return null;
		} finally {
			takeLock.unlock();
		}
	}

	@Override
	public int drainTo(Collection<? super T> c) {
		if (c==null){
			throw new NullPointerException();
		}
		if(c==this) {
			throw new IllegalArgumentException();
		}
		final AtomicInteger head = this.head;
		final AtomicInteger tail = this.tail;
		int oldhead;
		int oldtail;
		AtomicReferenceArray<T> oldElements;
		fullyLock();
		try {
			oldhead=head.get();
			oldtail=tail.get();
			oldElements=elements;
			head.set(0);
			tail.set(0);
			elements=new AtomicReferenceArray<T>(capacity);
			if(count.getAndSet(0)==capacity) {
				notFull.signalAll();
			}
		} finally {
			fullyUnlock();
		}
		int n = 0;
		while(oldhead!=oldtail) {
			c.add(oldElements.getAndSet(oldhead,null));
			oldhead = quickNext(oldhead, capacity);
			n++;
		}
		return n;
	}

	@Override
	public int drainTo(Collection<? super T> c, int maxElements) {
		if (c==null)
			throw new NullPointerException();
		if (c==this)
			throw new IllegalArgumentException();
		final AtomicInteger head = this.head;
		final AtomicInteger tail = this.tail;
		final AtomicInteger count = this.count;
		fullyLock();
		try {
			int n = 0;
			while(head.get()!=tail.get() && n < maxElements) {
				c.add(elements.getAndSet(head.get(),null));
				head.set(quickNext(head.get(),capacity));
				n++;
			}
			if(n!=0) {
				if(count.getAndAdd(-n)==capacity) {
					notFull.signalAll();
				}
			}
			return n;
		} finally {
			fullyUnlock();
		}
	}

	@Override
	public int remainingCapacity() {
		return capacity-count.get();
	}

	@Override
	public Iterator<T> iterator() {
		throw new UnsupportedOperationException();
	}
	
}
