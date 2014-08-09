package common.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class MultiTask {
	private List<Runnable> tasks;
	private int finished;
	
	public MultiTask() {
	}
	
	public MultiTask(List<Runnable> tasks) {
		this.tasks = tasks;
	}
	
	public void addTask(Runnable task) {
		if(tasks == null) {
			tasks = new ArrayList<Runnable>();
		}
		tasks.add(task);
	}
	
	public void execute(ExecutorService es) {
		List<TaskRunnable> list = new ArrayList<MultiTask.TaskRunnable>();
		for(Runnable r : tasks) {
			TaskRunnable wrapper = new TaskRunnable(r);
			es.execute(wrapper);
			list.add(wrapper);
		}
		synchronized (this) {
			while(finished < tasks.size()) {
				try {
					this.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		for(TaskRunnable tr : list) {
			if(tr.e != null) {
				throw new RuntimeException(tr.e);
			}
		}
	}
	
	private class TaskRunnable implements Runnable {
		private Runnable actual;
		private Exception e;

		public TaskRunnable(Runnable r) {
			this.actual = r;
		}
		
		@Override
		public void run() {
			try {
				actual.run();
			} catch (Exception e) {
				e.printStackTrace();
				this.e = e;
			}
			finally {
				synchronized (MultiTask.this) {
					finished++;
					if(finished >= tasks.size()) {
						MultiTask.this.notify();
					}
				}
			}
		}
		
	}
}
