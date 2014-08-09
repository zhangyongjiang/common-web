package common.db.shard;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class SharedShardTaskExecutor {
	private int poolSize;
	private ExecutorService executorService;
	
	@Value("${db.threadPool.size}")
	public void setThreadPoolSize(int size) {
		this.poolSize = size;
		executorService = Executors.newFixedThreadPool(size);
	}

	public ExecutorService getExecutorService() {
		return executorService;
	}

}
