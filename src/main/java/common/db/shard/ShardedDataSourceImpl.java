package common.db.shard;

import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

public class ShardedDataSourceImpl implements ShardedDataSource {
	private Map<Integer, JdbcTemplate> jdbcTemplates;
	private Map<Integer, NamedParameterJdbcTemplate> namedJdbcTemplates;
	private List<DataSource> dataSources;

	@Override
	public DataSource getDataSource(int shard) {
		return dataSources.get(shard);
	}

	public synchronized NamedParameterJdbcTemplate getNamedParameterJdbcTemplate(int shardId) {
		NamedParameterJdbcTemplate template = namedJdbcTemplates.get(shardId);
		if(template == null) {
			template = new NamedParameterJdbcTemplate(getDataSource(shardId));
			namedJdbcTemplates.put(shardId, template);
		}
		return template;
	}
	
	public synchronized JdbcTemplate getJdbcTemplate(int shardId) {
		JdbcTemplate template = jdbcTemplates.get(shardId);
		if(template == null) {
			template = new JdbcTemplate(getDataSource(shardId));
			jdbcTemplates.put(shardId, template);
		}
		return template;
	}

	public void setDataSources(List<DataSource> dataSources) {
		this.dataSources = dataSources;
	}

}
