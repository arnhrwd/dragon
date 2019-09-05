package dragon.utils;

import java.util.ArrayList;
import java.util.List;

public class CircularBuffer<T> {
	
	protected List<T> elements;
	protected Integer head;
	protected Integer tail;
	protected Integer prev_tail;
	protected Integer prev_head;
	protected Integer size=1024;
	public Object lock = new Object();
	
	public CircularBuffer(){
		init();
	}
	
	public CircularBuffer(int size){
		this.size=size;
		init();
	}
	
	private void init(){
		elements = new ArrayList<T>(size);
		for(int i=0;i<size;i++) {
			elements.add( null);
		}
		head=0;
		tail=0;
	}
	
	public int getNumElements() {
		synchronized(elements) {
			if(tail==head) return 0;
			if(tail>head) return tail-head;
			return size-(head-tail);
		}
	}
	
	public void put(T element) throws InterruptedException {
		while(!offer(element)) {
			synchronized(this) {
				wait();
			}
		}
	}
	
	public boolean offer(T element){
		synchronized(elements){
			if((tail+1)%size==head){
				return false;
			}
			
			elements.set(tail,element);
			prev_tail=tail;
			tail=(tail+1)%size;
			return true;
		}
	}
	
	public T poll(){
		synchronized(elements){
			if(head!=tail){
				
				T element=elements.get(head);
				elements.set(head, null);
				prev_head=head;
				head=(head+1)%size;
				synchronized(this) {
					notify();
				}
				return element;
			}
			return null;
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
}
