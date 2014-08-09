package common.db.shard;

import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.dbcp2.BasicDataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

public class DbShard {
	private int id;
	private Properties settings;
	private BasicDataSource dataSource;
	private JdbcTemplate jdbcTemplate;
	private NamedParameterJdbcTemplate namedParameterJdbcTemplate;
	private DataSourceManager dataSourceManager;
	
	public DbShard(DataSourceManager dataSourceManager, int id, Properties settings) {
		this.setId(id);
		this.settings = settings;
		this.dataSourceManager = dataSourceManager;
	}
	
	public void open() {
		dataSource = dataSourceManager.getDataSource(settings);
        jdbcTemplate = new JdbcTemplate(dataSource);
		namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(getJdbcTemplate());
	}
	
	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}
	
	public void close() throws SQLException {
		dataSource.close();
	}
	
	public JdbcTemplate getJdbcTemplate() {
		return jdbcTemplate;
	}
	
	public NamedParameterJdbcTemplate getNamedParameterJdbcTemplate() {
		return namedParameterJdbcTemplate;
	}
}
