package common.db.shard;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.dbcp2.BasicDataSource;
import org.springframework.stereotype.Component;

@Component
public class DataSourceManager {
	private Map<String, BasicDataSource> dataSources;
	
	public DataSourceManager() {
		dataSources = new HashMap<String, BasicDataSource>();
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
