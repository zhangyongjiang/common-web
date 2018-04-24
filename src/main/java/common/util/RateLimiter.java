package common.util;

import java.util.LinkedList;
import java.util.List;

public class RateLimiter {
	private int requests;
	private long duration;
	private List<Long> queue = new LinkedList<Long>();

	public RateLimiter(int requests, long duration) {
		this.requests = requests;
		this.duration = duration;
	}
	
	private void removeExpired() {
		long now = System.currentTimeMillis();
		while(queue.size() > 0) {
			Long reqTime = queue.get(0);
			long age = now - reqTime;
			if(age > duration) {
				queue.remove(0);
			}
			else {
				break;
			}
		}
	}
	
	public int getAvailable() {
		synchronized(this) {
			removeExpired();
			return requests - queue.size();
		}
	}
	
	public boolean isAvailable() {
		return getAvailable() > 0;
	}
	
	public boolean request() {
		synchronized(this) {
			removeExpired();
			if(queue.size()>=requests)
				return false;
			queue.add(System.currentTimeMillis());
			return true;
		}
	}
}
