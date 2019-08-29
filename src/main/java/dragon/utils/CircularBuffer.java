package dragon.utils;

import java.util.ArrayList;
import java.util.List;

public class CircularBuffer<T> {
	
	protected List<T> elements;
	protected Integer head;
	protected Integer tail;
	protected Integer prev_tail;
	protected Integer size=1024;
	
	public CircularBuffer(){
		init();
	}
	
	public CircularBuffer(int size){
		this.size=size;
		init();
	}
	
	private void init(){
		elements = new ArrayList<T>(size);
		head=0;
		tail=0;
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
				head=(head+1)%size;
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
