package common.db.shard;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

public class DbCluster {
	public static class DbShard {
		private JdbcTemplate jdbcTemplate;
		private NamedParameterJdbcTemplate namedParameterJdbcTemplate;
		
		public DbShard(DataSource dataSource) {
	        jdbcTemplate = new JdbcTemplate(dataSource);
			namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(getJdbcTemplate());
		}
		
		public JdbcTemplate getJdbcTemplate() {
			return jdbcTemplate;
		}
		
		public NamedParameterJdbcTemplate getNamedParameterJdbcTemplate() {
			return namedParameterJdbcTemplate;
		}
	}
	
	private Map<String, BasicDataSource> dataSources;
	private Properties settings;
	private ShardResolver shardResolver;
	private Map<Integer, DbShard> shards;
	
	public DbCluster(ShardResolver shardResolver) {
		shards = new HashMap<Integer, DbShard>();
		dataSources = new HashMap<String, BasicDataSource>();
		this.shardResolver = shardResolver;
	}
	
	public void init(Properties settings) {
		this.settings = settings;
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
				
		    	BasicDataSource dataSource = getDataSource(shardSetting);
				shard = new DbShard(dataSource);
				shards.put(shardId, shard);
			}
			return shard;
		}
	}
	
	public void close() throws SQLException {
		synchronized (shards) {
			for(DbShard shard : shards.values()) {
			}
			shards.clear();
		}
	}
	
	synchronized public BasicDataSource getDataSource(Properties settings) {
		String url = settings.getProperty("url");
		BasicDataSource dataSource = dataSources.get(url);
		if(dataSource != null) return dataSource;
		
		dataSource = new BasicDataSource();
        dataSource.setDriverClassName(settings.getProperty("driverClassName"));
        dataSource.setUsername(settings.getProperty("userName"));
        dataSource.setPassword(settings.getProperty("password"));
        dataSource.setValidationQuery(settings.getProperty("validationQuery"));
		dataSource.setUrl(url);
		dataSource.setLogAbandoned(true);
		dataSource.setRemoveAbandonedTimeout(300);
		dataSource.setMinEvictableIdleTimeMillis(300000);
		dataSource.setTimeBetweenEvictionRunsMillis(30000);
        
        dataSource.setMaxIdle(getInt("maxIdle", settings));
        dataSource.setMaxOpenPreparedStatements(500);
        dataSource.setPoolPreparedStatements(false);
		
        dataSources.put(url, dataSource);
        
        return dataSource;
	}
	
	private Integer getInt(String key, Properties settings) {
		String property = settings.getProperty(key);
		return property == null ? null : Integer.parseInt(property);
	}
}
