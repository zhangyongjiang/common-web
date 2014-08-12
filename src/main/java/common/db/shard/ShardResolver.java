package common.db.shard;

public interface ShardResolver {
	int getShardId(String id);
	int size();
}
