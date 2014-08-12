package common.db.shard;

import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

public class DbCluster {
	
	private ShardedDataSource shardedDataSource;
	private Map<Integer, JdbcTemplate> jdbcTemplates;
	private Map<Integer, NamedParameterJdbcTemplate> namedJdbcTemplates;
	
	public DbCluster(ShardedDataSource shardedDataSource) {
		this.shardedDataSource = shardedDataSource;
	}
	
	public synchronized NamedParameterJdbcTemplate getNamedParameterJdbcTemplate(int shardId) {
		NamedParameterJdbcTemplate template = namedJdbcTemplates.get(shardId);
		if(template == null) {
			template = new NamedParameterJdbcTemplate(shardedDataSource.getDataSource(shardId));
			namedJdbcTemplates.put(shardId, template);
		}
		return template;
	}
	
	public synchronized JdbcTemplate getJdbcTemplate(int shardId) {
		JdbcTemplate template = jdbcTemplates.get(shardId);
		if(template == null) {
			template = new JdbcTemplate(shardedDataSource.getDataSource(shardId));
			jdbcTemplates.put(shardId, template);
		}
		return template;
	}
	
}
