
package edu.berkeley.cs162;

import java.util.LinkedList;

public class ThreadPool {
	/**
	 * Set of threads in the threadpool
	 */
	protected Thread threads[] = null;

	private LinkedList<Runnable> _jobs;
	/**
	 * Initialize the number of threads required in the threadpool. 
	 * 
	 * @param size  How many threads in the thread pool.
	 */
	
	public ThreadPool(int size) {
		_jobs = new LinkedList<Runnable>();
		threads =new Thread[size];
		for (int i = 0; i < size; i++) {
			threads[i] = new WorkerThread(this);
			threads[i].start();
		}
	}
	
	//+chen
	public int getNumWorkerThreads() {
		return threads.length;
	}
	
	public int getNumJobs() {
		return _jobs.size();
	}
	//-chen

	/**
	 * Add a job to the queue of tasks that has to be executed. As soon as a thread is available, 
	 * it will retrieve tasks from this queue and start processing.
	 * @param r job that has to be executed asynchronously
	 * @throws InterruptedException 
	 */
	public synchronized void addToQueue(Runnable r) throws InterruptedException
	{
		_jobs.add(r);
		this.notify();
	}
	
	/** 
	 * Block until a job is available in the queue and retrieve the job
	 * @return A runnable task that has to be executed
	 * @throws InterruptedException 
	 */
	public synchronized Runnable getJob() throws InterruptedException {
		while(_jobs.size() == 0) {
			//System.out.println("jobs size 0!");
			this.wait();
		}
		return _jobs.pop();
	}
}
/**
 * The worker threads that make up the thread pool.
 */
class WorkerThread extends Thread {
	//+chen
	private final ThreadPool pool;
	/**
	 * The constructor.
	 * 
	 * @param o the thread pool 
	 */
	WorkerThread(ThreadPool o)
	{
		this.pool = o;
	}

	/**
	 * Scan for and execute tasks.
	 */
	public void run()
	{
		try {
			while (true) {
				System.out.println("pulled new job to run....");
				Runnable newJob = pool.getJob();
				System.out.println("The running job is: " + newJob.toString());

				newJob.run();	
				//	pool.getJob().run();
			}
		} catch (InterruptedException e) {
			System.err.println(e.getMessage());
		}
	
	}
}
