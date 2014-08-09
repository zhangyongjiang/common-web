package common.db.shard;

import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import common.db.util.ORMClass;
import common.db.util.RowMapped;

public interface ClusterDataDao<T extends HasId> {
	void createSchema();
	int getShardSize();
	void replace(T data);
	void replace(List<T> data);
	T get(String id);
	List<T> getAll(String id);
	List<T> get(List<String> id);
	Set<String> existingIds(List<String> ids);
	Set<String> nonexistingIds(List<String> ids);
	void dump(ObjectDumper od);
	void dump(ObjectDumper od, int shardId);
	void dump(RowMapped<T> rows);
	void dump(String sql, RowMapped<T> rows);
	void dump(String sql, RowMapper<T> rows);
	JdbcTemplate getJdbcTemplate(int shardId);
	NamedParameterJdbcTemplate getNamedJdbcTemplate(int shardId);
	JdbcTemplate getJdbcTemplateForId(String id);
	NamedParameterJdbcTemplate getNamedJdbcTemplateForId(String id);
	void remove(String id);
	void remove(List<String> ids);
	void query(OutputStream outputStream, String sql);
	ORMClass<T> getOrmClass();
	List<T> queryAllShards(String sql, Object[] args);
	List<T> queryAllShards(String sql, Map<String, Object> params);
	void queryAllShards(String sql, RowMapper<T> rows, Object[] args);
	void queryAllShards(String sql, RowMapper<T> rows, Map<String, Object> params);
	void queryAllShards(String sql, RowMapped<T> rows, Object[] args);
	void queryAllShards(String sql, RowMapped<T> rows, Map<String, Object> params);
	void updateAllShards(String sql, Object[] args);
	void updateAllShards(String sql, Map<String, Object> params);
	void updateAllShards(String sql, List<Object[]> args);
	void updateShards(String sql, Map<Integer, Object[]> args);
}
