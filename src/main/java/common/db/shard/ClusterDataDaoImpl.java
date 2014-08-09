package common.db.shard;

import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import common.db.util.ORMClass;
import common.db.util.RowMapped;
import common.util.MultiTask;
import common.util.reflection.ReflectionUtil;

public abstract class ClusterDataDaoImpl<T> implements ClusterDataDao<T> {
	abstract public DbCluster getDbCluster();
	abstract public ShardResolver getShardResolver();
	
	@Autowired protected SharedShardTaskExecutor taskExecutor;
	@Autowired protected DataSourceManager dataSourceManager;
	
	protected ORMClass<T> orm;
	
	public ClusterDataDaoImpl() {
		orm = createOrmClass();
	}
	
	protected ORMClass createOrmClass() {
		Class cls = ReflectionUtil.getParameterizedType(this.getClass());
		return new ORMClass<T>(cls);
	}
	
	@Override
	public void replace(T data) {
		int shardId = getShardResolver().getShardId(orm.getObjectId(data));
		NamedParameterJdbcTemplate jc = getDbCluster().getNamedParameterJdbcTemplate(shardId);
		if(orm.isReplaceSupported()) {
			orm.replace(jc, data);
		}
		else {
			T db = get(orm.getObjectId(data));
			if(db == null) {
				orm.insert(jc, data, true);
			}
			else {
				orm.updateEntity(jc, data, null);
			}
		}
	}

	@Override
	public void replace(List<T> data) {
		for(T t : data) {
			try {
				replace(t);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public T get(String id) {
		List<T> all = getAll(id);
		return all.size() > 0 ? all.get(0) : null;
	}

	@Override
	public List<T> getAll(String id) {
		int shardId = getShardResolver().getShardId(id);
		NamedParameterJdbcTemplate jc = getDbCluster().getNamedParameterJdbcTemplate(shardId);
		return orm.query(jc, Collections.singletonMap("id", id), null);
	}

	@Override
	public List<T> get(List<String> ids) {
		final Map<Integer, List<String>> shardedIds = new HashMap<Integer, List<String>>();
		for(String id : ids) {
			int shardId = getShardResolver().getShardId(id);
			List<String> inshardIds = shardedIds.get(shardId);
			if(inshardIds == null) {
				inshardIds = new ArrayList<String>();
				shardedIds.put(shardId, inshardIds);
			}
			inshardIds.add(id);
		}
		
		final List<T> result = new ArrayList<T>();
		MultiTask mt = new MultiTask();
		for(Entry<Integer, List<String>> entry : shardedIds.entrySet()) {
			int shardId = entry.getKey();
			ShardTask shardTask = new ShardTask(shardId, new ShardRunnable() {
				@Override
				public void run(int shardId) {
					List<String> sameShardIds = shardedIds.get(shardId);
					NamedParameterJdbcTemplate namedjc = getDbCluster().getNamedParameterJdbcTemplate(shardId);
					List<T> data = orm.queryBySql(namedjc, "select * from " + orm.getTableName() + " where id in (:ids)", Collections.singletonMap("ids", sameShardIds));
					synchronized (result) {
						result.addAll(data);
					}
				}
			});
			mt.addTask(shardTask);
		}
		mt.execute(taskExecutor.getExecutorService());
		
		return result;
	}
	
	protected Map<Integer, List<String>> splitIdsByShard(List<String> ids) {
		Map<Integer, List<String>> shardedIds = new HashMap<Integer, List<String>>();
		for(String id : ids) {
			int shardId = getShardResolver().getShardId(id);
			List<String> inshardIds = shardedIds.get(shardId);
			if(inshardIds == null) {
				inshardIds = new ArrayList<String>();
				shardedIds.put(shardId, inshardIds);
			}
			inshardIds.add(id);
		}
		return shardedIds;
	}

	@Override
	public Set<String> existingIds(List<String> ids) {
		final Map<Integer, List<String>> shardedIds = splitIdsByShard(ids);
		final Set<String> result = new HashSet<String>();
		
		ShardRunnable runnable = new ShardRunnable() {
			@Override
			public void run(int shardId) {
				List<String> sameShardIds = shardedIds.get(shardId);
				NamedParameterJdbcTemplate namedjc = getDbCluster().getNamedParameterJdbcTemplate(shardId);
				String sql = "select id from " + orm.getTableName() + " where id in (:ids)";
				List<String> data = namedjc.queryForList(sql, Collections.singletonMap("ids", sameShardIds), String.class);
				synchronized (result) {
					result.addAll(data);
				}
			}
		};
		
		MultiTask mt = new MultiTask();
		for(Entry<Integer, List<String>> entry : shardedIds.entrySet()) {
			int shardId = entry.getKey();
			ShardTask shardTask = new ShardTask(shardId, runnable);
			mt.addTask(shardTask);
		}
		mt.execute(taskExecutor.getExecutorService());
		
		return result;
	}

	@Override
	public Set<String> nonexistingIds(List<String> ids) {
		Set<String> existings = existingIds(ids);
		Set<String> notfound = new HashSet<String>();
		for(String s : ids) {
			if(!existings.contains(s))
				notfound.add(s);
		}
		return notfound;
	}

	@Override
	public void dump(final ObjectDumper od) {
		String sql = "select * from " + orm.getTableName();
		for(int i=0; i<getShardResolver().virtualSize(); i++) {
			JdbcTemplate template = getDbCluster().getJdbcTemplate(i);
			RowMapper<T> mapper2 = orm.getStreamRow(new RowMapped<T>() {
				@Override
				public void objectFound(T obj) {
					od.dump(obj);
				}
			});
			template.query(sql, mapper2);
		}
	}
	
	@Override
	public void dump(final ObjectDumper od, int shardId) {
		String sql = "select * from " + orm.getTableName();
		JdbcTemplate template = getDbCluster().getJdbcTemplate(shardId);
		RowMapper<T> streamRow = orm.getStreamRow(new RowMapped<T>() {
			@Override
			public void objectFound(T obj) {
					od.dump(obj);
			}
		});
		template.query(sql, streamRow);
	}
	
	@Override
	public void dump(RowMapped<T> rows) {
		String sql = "select * from " + orm.getTableName();
		dump(sql, rows);
	}
	
	@Override
	public void dump(String sql, RowMapped<T> rows) {
		for(int i=0; i<getShardResolver().virtualSize(); i++) {
			JdbcTemplate template = getDbCluster().getJdbcTemplate(i);
			template.query(sql, orm.getStreamRow(rows));
		}
	}
	
	@Override
	public void dump(String sql, RowMapper<T> rows) {
		for(int i=0; i<getShardResolver().virtualSize(); i++) {
			JdbcTemplate template = getDbCluster().getJdbcTemplate(i);
			template.query(sql, rows);
		}
	}
	
	@Override
	public JdbcTemplate getJdbcTemplate(int shardId) {
		return getDbCluster().getJdbcTemplate(shardId);
	}
	
	@Override
	public NamedParameterJdbcTemplate getNamedJdbcTemplate(int shardId) {
		return getDbCluster().getNamedParameterJdbcTemplate(shardId);
	}
	
	@Override
	public JdbcTemplate getJdbcTemplateForId(String id) {
		return getDbCluster().getJdbcTemplate(getShardResolver().getShardId(id));
	}
	
	@Override
	public NamedParameterJdbcTemplate getNamedJdbcTemplateForId(String id) {
		return getDbCluster().getNamedParameterJdbcTemplate(getShardResolver().getShardId(id));
	}
	
	@Override
	public int getShardSize() {
		return getShardResolver().virtualSize();
	}
	
	@Override
	public void remove(String id) {
		remove(Collections.singletonList(id));
	}
	
	@Override
	public void remove(List<String> ids) {
		final Map<Integer, List<String>> shardedIds = splitIdsByShard(ids);
		
		ShardRunnable runnable = new ShardRunnable() {
			@Override
			public void run(int shardId) {
				List<String> sameShardIds = shardedIds.get(shardId);
				NamedParameterJdbcTemplate namedjc = getDbCluster().getNamedParameterJdbcTemplate(shardId);
				String sql = "delete from " + orm.getTableName() + " where id in (:ids)";
				namedjc.update(sql, Collections.singletonMap("ids", sameShardIds));
			}
		};
		
		MultiTask mt = new MultiTask();
		for(Entry<Integer, List<String>> entry : shardedIds.entrySet()) {
			int shardId = entry.getKey();
			ShardTask shardTask = new ShardTask(shardId, runnable);
			mt.addTask(shardTask);
		}
		mt.execute(taskExecutor.getExecutorService());
	}
	
	@Override
	public void query(final OutputStream stream, String sql) {
		final StringBuilder sb = new StringBuilder();
		for(int i=0; i<getShardResolver().virtualSize(); i++) {
			JdbcTemplate template = getDbCluster().getJdbcTemplate(i);
			template.query(sql, new RowMapper(){
				ResultSetMetaData metaData = null;
				@Override
				public Object mapRow(ResultSet rs, int rowNum)
						throws SQLException {
					if(metaData == null)
						metaData = rs.getMetaData();
					int cnt = metaData.getColumnCount();
					if(sb.length() == 0) {
						for(int i=0; i<cnt; i++) {
							sb.append(metaData.getColumnLabel(i+1));
							sb.append("\t");
						}
						sb.append("\n");
					}
					for(int i=0; i<cnt; i++) {
						sb.append(rs.getString(i+1));
						sb.append("\t");
					}
					sb.append("\n");
					return null;
				}});
		}
	}
	
	@Override
	public ORMClass<T> getOrmClass() {
		return orm;
	}
	
	@Override
	public List<T> queryAllShards(final String sql, final Object[] args) {
		final List<T> list = new ArrayList<T>();
		queryAllShards(sql, new RowMapped<T>(){
			@Override
			public void objectFound(T obj) {
				list.add(obj);
			}}, args);
		return list;
	}
	
	@Override
	public List<T> queryAllShards(final String sql, Map<String, Object> params) {
		final List<T> list = new ArrayList<T>();
		queryAllShards(sql, new RowMapped<T>(){
			@Override
			public void objectFound(T obj) {
				list.add(obj);
			}}, params);
		return list;
	}
	
	@Override
	public void queryAllShards(final String sql, final RowMapper<T> rows, final Object[] args) {
		MultiTask mt = new MultiTask();
		ShardRunnable runnable = new ShardRunnable() {
			@Override
			public void run(int shardId) {
				JdbcTemplate namedjc = getDbCluster().getJdbcTemplate(shardId);
				namedjc.query(sql, args, rows);
			}
		};
		
		for(int i=0; i<getShardSize(); i++) {
			ShardTask shardTask = new ShardTask(i, runnable);
			mt.addTask(shardTask);
		}
		mt.execute(taskExecutor.getExecutorService());
	}
	
	@Override
	public void queryAllShards(final String sql, final RowMapper<T> rows, final Map<String, Object> params) {
		MultiTask mt = new MultiTask();
		ShardRunnable runnable = new ShardRunnable() {
			@Override
			public void run(int shardId) {
				NamedParameterJdbcTemplate namedjc = getDbCluster().getNamedParameterJdbcTemplate(shardId);
				namedjc.query(sql, params, rows);
			}
		};
		
		for(int i=0; i<getShardSize(); i++) {
			ShardTask shardTask = new ShardTask(i, runnable);
			mt.addTask(shardTask);
		}
		mt.execute(taskExecutor.getExecutorService());
	}
	
	@Override
	public void queryAllShards(final String sql, final RowMapped<T> rows, final Object[] args) {
		RowMapper<T> mapper = orm.getStreamRow(rows);
		queryAllShards(sql, mapper, args);
	}
	
	@Override
	public void queryAllShards(final String sql, final RowMapped<T> rows, Map<String, Object> params) {
		RowMapper<T> mapper = orm.getStreamRow(rows);
		queryAllShards(sql, mapper, params);
	}
	
	@Override
	public void updateAllShards(final String sql, final Object[] args) {
		MultiTask mt = new MultiTask();
		ShardRunnable runnable = new ShardRunnable() {
			@Override
			public void run(int shardId) {
				JdbcTemplate namedjc = getDbCluster().getJdbcTemplate(shardId);
				namedjc.update(sql, args);
			}
		};
		
		for(int i=0; i<getShardSize(); i++) {
			ShardTask shardTask = new ShardTask(i, runnable);
			mt.addTask(shardTask);
		}
		mt.execute(taskExecutor.getExecutorService());
	}
	
	@Override
	public void updateShards(final String sql, final Map<Integer, Object[]> args) {
		MultiTask mt = new MultiTask();
		ShardRunnable runnable = new ShardRunnable() {
			@Override
			public void run(int shardId) {
				JdbcTemplate namedjc = getDbCluster().getJdbcTemplate(shardId);
				namedjc.update(sql, args.get(shardId));
			}
		};
		
		for(Entry<Integer, Object[]> entry : args.entrySet()) {
			int shardId = entry.getKey();
			ShardTask shardTask = new ShardTask(shardId, runnable);
			mt.addTask(shardTask);
		}
		mt.execute(taskExecutor.getExecutorService());
	}
	
	@Override
	public void updateAllShards(final String sql, final List<Object[]> args) {
		MultiTask mt = new MultiTask();
		ShardRunnable runnable = new ShardRunnable() {
			@Override
			public void run(int shardId) {
				JdbcTemplate namedjc = getDbCluster().getJdbcTemplate(shardId);
				namedjc.update(sql, args.get(shardId));
			}
		};
		
		for(int i=0; i<getShardSize(); i++) {
			ShardTask shardTask = new ShardTask(i, runnable);
			mt.addTask(shardTask);
		}
		mt.execute(taskExecutor.getExecutorService());
	}
	
	@Override
	public void updateAllShards(final String sql, final Map<String, Object> params) {
		MultiTask mt = new MultiTask();
		ShardRunnable runnable = new ShardRunnable() {
			@Override
			public void run(int shardId) {
				NamedParameterJdbcTemplate namedjc = getDbCluster().getNamedParameterJdbcTemplate(shardId);
				namedjc.update(sql, params);
			}
		};
		
		for(int i=0; i<getShardSize(); i++) {
			ShardTask shardTask = new ShardTask(i, runnable);
			mt.addTask(shardTask);
		}
		mt.execute(taskExecutor.getExecutorService());
	}
}
