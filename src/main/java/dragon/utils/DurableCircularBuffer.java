package dragon.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class DurableCircularBuffer<T> extends CircularBuffer<T> {
	private Log log = LogFactory.getLog(DurableCircularBuffer.class);
	private final String INDEX_FILE = "index.dat";
	private String dir;
	
	public DurableCircularBuffer(String dir){
		super();
		this.dir=dir;
		recover();
	}
	
	public DurableCircularBuffer(int size, String dir){
		super(size);
		this.dir=dir;
		recover();
	}
	
	private void recover(){
		synchronized(dir){
			recoverIndex();
			recoverElements();
		}
	}
	
	private void recoverIndex(){
		FileInputStream file;
		try {
			file = new FileInputStream(Paths.get(dir, INDEX_FILE).toString());
			ObjectInputStream in = new ObjectInputStream(file); 
			head=(Integer)in.readObject();
			tail=(Integer)in.readObject();
	        in.close(); 
	        file.close(); 
		} catch (FileNotFoundException e) {
			// may not have been written yet
		} catch (IOException e) {
			log.error("cannot recover index: "+e.toString());
		} catch (ClassNotFoundException e) {
			log.error("cannot recover index: "+e.toString());
		} 
	}
	
	@SuppressWarnings("unchecked")
	private void recoverElements(){
		for(int i=0;i<capacity;i++){
			FileInputStream file;
			try {
				file = new FileInputStream(Paths.get(dir, i+".dat").toString());
				ObjectInputStream in = new ObjectInputStream(file); 
				elements.set(i,(T)in.readObject());
		        in.close(); 
		        file.close(); 
			} catch (FileNotFoundException e) {
				// not all elements may be found on disk
			} catch (IOException e) {
				log.error("cannot recover element: "+e.toString());
			} catch (ClassNotFoundException e) {
				log.error("cannot recover element: "+e.toString());
			} 
		}
	}
	
	private void persistIndex(){
		FileOutputStream file;
		try {
			Path path = Paths.get(dir, INDEX_FILE);
			File f = new File(path.getParent().toString());
			f.mkdirs();
			file = new FileOutputStream(path.toString());
			ObjectOutputStream out = new ObjectOutputStream(file); 
			out.writeObject(head);
			out.writeObject(tail);
	        out.close(); 
	        file.close(); 
		} catch (FileNotFoundException e) {
			log.error("cannot persist index: "+e.toString());
		} catch (IOException e) {
			log.error("cannot persist index: "+e.toString());
		} 
	}
	
	private void persistElement(Integer i,T element){
		FileOutputStream file;
		try {
			Path path = Paths.get(dir, i+".dat");
			File f = new File(path.getParent().toString());
			f.mkdirs();
			file = new FileOutputStream(Paths.get(dir, i+".dat").toString());
			ObjectOutputStream out = new ObjectOutputStream(file); 
			out.writeObject(element);
	        out.close(); 
	        file.close(); 
		} catch (FileNotFoundException e) {
			log.error("cannot persist element: "+e.toString());
		} catch (IOException e) {
			log.error("cannot persist element: "+e.toString());
		} 
	}
	
	public void put(T element) throws InterruptedException{
		while(!offer(element)){
			synchronized(this) {
				wait();
			}
		}
	}
	
	public boolean offer(T element){
        synchronized(dir){
	        boolean ret = super.offer(element);  
	        if(ret){
	        	persistIndex();
	        	persistElement(prev_tail,element);
	        }
			return ret;
        }
	}
	
	public T poll(){
		synchronized(dir){
			T element = super.poll();
			if(element!=null){
				persistIndex();
				persistElement(prev_head,null);
				synchronized(this) {
					notify();
				}
			}
			return element;
		}
	}
	
	public T peek(){
		synchronized(dir){
			return super.peek();
		}
	}
	
}
