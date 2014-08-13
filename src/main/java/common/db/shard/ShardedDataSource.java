package common.db.shard;

import javax.sql.DataSource;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

public interface ShardedDataSource {
	DataSource getDataSource(int shard);
	NamedParameterJdbcTemplate getNamedParameterJdbcTemplate(int shardId);
	JdbcTemplate getJdbcTemplate(int shardId);
}
