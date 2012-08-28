package dk.nsi.sdm4.core.persistence.migration;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

/**
 * Supports migrations in db/testmigrations with names on the form <YYYYMMDD>_<HHMM>__Description.sql
 * Only supports MySQL
 */
public class DbMigrator {
	protected static final String METADATA_TABLE_NAME = "db_migrations";
	private static final Logger log = Logger.getLogger(DbMigrator.class);
	private static final String SCHEMA_SQL = "CREATE TABLE `db_migrations` (\n" +
			"  `version` varchar(20) NOT NULL,\n" +
			"  `description` varchar(100) DEFAULT NULL,\n" +
			"  `installed_by` varchar(30) NOT NULL,\n" +
			"  `installed_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n" +
			"  PRIMARY KEY (`version`),\n" +
			"  UNIQUE KEY `version` (`version`)\n" +
			")  ENGINE=InnoDB DEFAULT CHARSET=latin1";

	@Autowired
	JdbcTemplate jdbcTemplate;

	@Autowired
	MigrationFinder migrationFinder;

	public void migrate() {
		if (!metadataTableExists()) {
			createMetadataTable();
		}

		doMigrations();

	}

	private void doMigrations() {
		Collection<Migration> migrations = migrationFinder.findMigrations();
		for (Migration migration : migrations) {
			jdbcTemplate.update(migration.getSql());
		}
	}

	private void createMetadataTable() {
		jdbcTemplate.update(SCHEMA_SQL);
	}

	private boolean metadataTableExists() {
		return (Boolean) jdbcTemplate.execute(new ConnectionCallback() {
			public Boolean doInConnection(Connection connection) throws SQLException, DataAccessException {
				ResultSet resultSet = connection.getMetaData().getTables(getCurrentSchema(), null, METADATA_TABLE_NAME, null);
				return resultSet.next();
			}
		});
	}

	private String getCurrentSchema() {
		return (String) jdbcTemplate.execute(new ConnectionCallback() {
			public String doInConnection(Connection connection) throws SQLException, DataAccessException {
				return connection.getCatalog();
			}
		});
	}
}
