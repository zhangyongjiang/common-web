package common.db.shard;

public class ShardTask implements Runnable {
	private int shardId;
	private ShardRunnable runnable;
	
	public ShardTask(int shard, ShardRunnable runnable) {
		this.shardId = shard;
		this.runnable = runnable;
	}

	@Override
	public void run() {
		runnable.run(shardId);
	}
}
