package dragon;

import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import dragon.utils.CircularBlockingQueue;

/**
 * Standalone class for various low level tests. Not important.
 * @author aaron
 *
 */
public class Test {

	private static volatile boolean startWork=false;
	
	public static void main(String[] args) {
		
		Options options = new Options();
		Option threadsOption = new Option("t", "threads", true, "number of threads to use for testing");
		options.addOption(threadsOption);
		Option capacityOption = new Option("c", "capacity", true, "capacity of buffer for testing");
		options.addOption(capacityOption);
		Option numOption = new Option("n", "num", true, "number of element to generate");
		options.addOption(numOption);

		CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;
        
        try {
			cmd = parser.parse(options, args);
			int threads = 1;
			int num = 1000000;
			int capacity = 16;
			if(cmd.hasOption("threads")) {
				threads=Integer.parseInt(cmd.getOptionValue("threads"));
			}
			if(cmd.hasOption("capacity")) {
				capacity=Integer.parseInt(cmd.getOptionValue("capacity"));
			}
			if(cmd.hasOption("num")) {
				num=Integer.parseInt(cmd.getOptionValue("num"));
			}
			CircularBlockingQueue<Integer> circularBuffer = new CircularBlockingQueue<Integer>(capacity);
			ArrayList<Thread> outputThreads = new ArrayList<Thread>(threads);
			ArrayList<Thread> inputThreads = new ArrayList<Thread>(threads);
			for(int i=0;i<threads;i++) {
				outputThreads.add(new Thread() {
					@Override
					public void run() {
						while(!isInterrupted()) {
							try {
								circularBuffer.take();
								//System.out.println("take "+i);
							} catch (InterruptedException e) {
								break;
							}
						}
					}
				});
				outputThreads.get(i).setName("output "+i);
				outputThreads.get(i).start();
			}
			final int perThread = num/threads;
			
			for(int i=0;i<threads;i++) {
				inputThreads.add(new Thread() {
					@Override
					public void run() {
						while(!startWork);
						
						for(int i=0;i<perThread;i++) {
							try {
								circularBuffer.put(i);
								//System.out.println("put "+i);
							} catch (InterruptedException e) {
								//ignore
							}
						}
					}
					
				});
				inputThreads.get(i).setName("input "+i);
				inputThreads.get(i).start();
			}
			long start;
			long end;
			System.out.println("starting circular buffer test");
			
			start=(new Date()).getTime();
			startWork=true;
		
			for(int i=0;i<threads;i++) {
				try {
					inputThreads.get(i).join();
				} catch (InterruptedException e) {
					// ignore
				}
			}
			end=(new Date()).getTime();
			System.out.println("circular_buffer_time "+(end-start));
			for(int i=0;i<threads;i++) {
				outputThreads.get(i).interrupt();
			}
			for(int i=0;i<threads;i++) {
				try {
					outputThreads.get(i).join();
				} catch (InterruptedException e) {
					// ignore
				}
			}
			
			/*
			 * Next test
			 */
			
			startWork=false;
			LinkedBlockingQueue<Integer> linkedBlockingQueue = new LinkedBlockingQueue<Integer>(capacity);
			outputThreads = new ArrayList<Thread>(threads);
			inputThreads = new ArrayList<Thread>(threads);
			for(int i=0;i<threads;i++) {
				outputThreads.add(new Thread() {
					@Override
					public void run() {
						while(!isInterrupted()) {
							try {
								linkedBlockingQueue.take();
							} catch (InterruptedException e) {
								break;
							}
						}
					}
				});
				outputThreads.get(i).setName("output "+i);
				outputThreads.get(i).start();
			}
			for(int i=0;i<threads;i++) {
				inputThreads.add(new Thread() {
					@Override
					public void run() {
						while(!startWork);
						
						for(int i=0;i<perThread;i++) {
							try {
								linkedBlockingQueue.put(i);
							} catch (InterruptedException e) {
								//ignore
							}
						}
					}
				});
				inputThreads.get(i).setName("input "+i);
				inputThreads.get(i).start();
			}
			System.out.println("starting linked blocking queue test");
			
			start=(new Date()).getTime();
			startWork=true;
			
			for(int i=0;i<threads;i++) {
				try {
					inputThreads.get(i).join();
				} catch (InterruptedException e) {
					// ignore
				}
			}
			end=(new Date()).getTime();
			System.out.println("linked_blocking_queue_time "+(end-start));
			for(int i=0;i<threads;i++) {
				outputThreads.get(i).interrupt();
				try {
					outputThreads.get(i).join();
				} catch (InterruptedException e) {
					// ignore
				}
			}
			
			
		} catch (ParseException e) {
			System.out.println(e.getMessage());
            formatter.printHelp("see the README.md file for usage information", options);
            System.exit(1);
		}
	}

}
