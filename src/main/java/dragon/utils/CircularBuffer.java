package dragon.utils;

import java.util.ArrayList;
import java.util.List;

public class CircularBuffer<T> {
	
	protected List<T> elements;
	protected Integer head;
	protected Integer tail;
	protected Integer prev_tail;
	protected Integer prev_head;
	protected Integer capacity=1024;
	public Object lock = new Object();
	public Object putLock = new Object();
	public Object takeLock = new Object();
	
	public CircularBuffer(){
		init();
	}
	
	public CircularBuffer(int capacity){
		this.capacity=capacity;
		init();
	}
	
	private void init(){
		elements = new ArrayList<T>(capacity);
		for(int i=0;i<capacity;i++) {
			elements.add( null);
		}
		head=0;
		tail=0;
	}
	
	public int size() {
		synchronized(elements) {
			if(tail.equals(head)) return 0;
			if(tail>head) return tail-head;
			return capacity-(head-tail);
		}
	}
	
	public void put(T element) throws InterruptedException {
		synchronized(putLock) {
			while(!offer(element)) {
				putLock.wait();
			}
		}
	}
	
	public boolean offer(T element){
		synchronized(takeLock) {
			synchronized(elements){
				if((tail+1)%capacity==head){
					return false;
				}
				elements.set(tail,element);
				prev_tail=tail;
				tail=(tail+1)%capacity;
				takeLock.notify();
				return true;
			}
		}
	}
	
	public T poll(){
		synchronized(putLock) {
			synchronized(elements){
				if(head!=tail){	
					T element=elements.get(head);
					elements.set(head, null);
					prev_head=head;
					head=(head+1)%capacity;
					putLock.notify();
					return element;
				}
				return null;
			}
		}
	}
	
	public T peek(){
		synchronized(elements){
			if(head!=tail){
				T element=elements.get(head);
				return element;
			}
			return null;
		}
	}
	
	public T take() throws InterruptedException {
		T element;
		synchronized(takeLock) {
			element = poll();
			while(element==null) {
				takeLock.wait();
				element = poll();
			}
		}
		return element;
	}
}
