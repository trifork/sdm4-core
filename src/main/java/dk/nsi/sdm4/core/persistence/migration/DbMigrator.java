package dk.nsi.sdm4.core.persistence.migration;

import com.googlecode.flyway.core.dbsupport.mysql.MySQLSqlScript;
import com.googlecode.flyway.core.migration.sql.PlaceholderReplacer;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * Supports migrations in db/testmigrations with names on the form <YYYYMMDD>_<HHMM>__Description.sql
 * Only supports MySQL
 */
public class DbMigrator {
	protected static final String METADATA_TABLE_NAME = "db_migrations";
	private static final Logger log = Logger.getLogger(DbMigrator.class);
	private static final String SCHEMA_SQL = "CREATE TABLE IF NOT EXISTS `db_migrations` (\n" +
			"  `version` varchar(20) NOT NULL,\n" +
			"  `description` varchar(100) DEFAULT NULL,\n" +
			"  `installed_by` varchar(30) NOT NULL,\n" +
			"  `installed_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n" +
			"  PRIMARY KEY (`version`),\n" +
			"  UNIQUE KEY `version` (`version`)\n" +
			")  ENGINE=InnoDB DEFAULT CHARSET=latin1";

	@Autowired
	JdbcTemplate jdbcTemplate;

	// not springwired to keep importer configurations simpler; we only substitute it for unit testing
	protected MigrationFinder migrationFinder = new MigrationFinder();

	public void migrate() {
		createMetadataTableIfNotExists();

		doMigrations();

	}

	private void doMigrations() {
		List<Migration> migrations = migrationFinder.findMigrations();

		for (Migration migration : migrations) {
			log.info("Migrating to " + migration);
			if (!migrationHasBeenRun(migration)) {
				MySQLSqlScript script = new MySQLSqlScript(migration.getSql(), PlaceholderReplacer.NO_PLACEHOLDERS);
				script.execute(jdbcTemplate);

				jdbcTemplate.update("INSERT INTO " + METADATA_TABLE_NAME + " (version, description, installed_by) VALUES (?, ?, SUBSTRING_INDEX(USER(),'@',1))",
						migration.getVersion(), migration.getDescription());
				if (log.isDebugEnabled()) {
					log.debug("Ran migration " + migration.toString() + ": " + migration.getSql());
				}
			} else {
				if (log.isDebugEnabled()) {
					log.debug("Migration has already been run: " + migration.toString());
				}
			}
		}
	}

	private boolean migrationHasBeenRun(Migration migration) {
		return 0 < jdbcTemplate.queryForInt("SELECT COUNT(*) FROM " + METADATA_TABLE_NAME + " WHERE version = ?", migration.getVersion());
	}

	private void createMetadataTableIfNotExists() {
		// The SQL makes sure the table is only created if it doesn't exist
		jdbcTemplate.update(SCHEMA_SQL);
	}

	private String getCurrentSchema() {
		return (String) jdbcTemplate.execute(new ConnectionCallback() {
			public String doInConnection(Connection connection) throws SQLException, DataAccessException {
				return connection.getCatalog();
			}
		});
	}

}
