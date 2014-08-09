package common.db.shard;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

public class DbCluster {
	private Properties settings;
	private ShardResolver shardResolver;
	private Map<Integer, DbShard> shards;
	private DataSourceManager dataSourceManager;
	
	public DbCluster(ShardResolver shardResolver) {
		shards = new HashMap<Integer, DbShard>();
		this.shardResolver = shardResolver;
	}
	
	public void init(DataSourceManager dataSourceManager, Properties settings) {
		this.settings = settings;
		this.dataSourceManager = dataSourceManager;
	}
	
	public NamedParameterJdbcTemplate getNamedParameterJdbcTemplate(int shardId) {
		return getDbShard(shardId).getNamedParameterJdbcTemplate();
	}
	
	public JdbcTemplate getJdbcTemplate(int shardId) {
		return getDbShard(shardId).getJdbcTemplate();
	}
	
	private DbShard getDbShard(int shardId) {
		synchronized (shards) {
			DbShard shard = shards.get(shardId);
			if(shard == null) {
				Properties shardSetting = new Properties(settings);
				String url = shardSetting.getProperty("url");
		        url = url.replaceAll("__VIRTUAL__", String.valueOf(shardId));
				int virtualShardPerPhysicalServer = shardResolver.virtualSize() / shardResolver.physicalSize();
		        url = url.replaceAll("__PHYSICAL__", String.valueOf((shardId / virtualShardPerPhysicalServer)));
		        shardSetting.put("url", url);
				
				shard = new DbShard(dataSourceManager, shardId, shardSetting);
				shard.open();
				shards.put(shardId, shard);
			}
			return shard;
		}
	}
	
	public void close() throws SQLException {
		synchronized (shards) {
			for(DbShard shard : shards.values()) {
				shard.close();
			}
			shards.clear();
		}
	}
}
