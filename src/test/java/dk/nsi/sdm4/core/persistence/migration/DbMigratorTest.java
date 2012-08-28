package dk.nsi.sdm4.core.persistence.migration;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;
import org.springframework.test.context.transaction.BeforeTransaction;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.util.Date;

import static org.junit.Assert.fail;

@RunWith(SpringJUnit4ClassRunner.class)
@Transactional
@ContextConfiguration(loader = AnnotationConfigContextLoader.class)
public class DbMigratorTest {
	@Configuration
	@PropertySource("classpath:test.properties")
	@Import(DbMigrationTestDatasourceConfiguration.class)
	static class ContextConfiguration {
		@Bean
		public DbMigrator migrator() {
			DbMigrator dbMigrator = new DbMigrator();
			return dbMigrator;
		}
	}

	@Autowired
	private DbMigrator migrator;

	@Autowired
	JdbcTemplate jdbcTemplate;

	@BeforeTransaction
	public void clearSchema() {
		jdbcTemplate.update("DROP Table if exists " + DbMigrator.METADATA_TABLE_NAME);
	}

	@Test
	public void youCanCallMigrateWithoutErrors() {
		migrator.migrate();
	}

	@Test
	public void metadataTableIsCreatedAutomatically() {
		String selectSql = "SELECT max(installed_on) from " + DbMigrator.METADATA_TABLE_NAME;

		try {
			jdbcTemplate.queryForObject(selectSql, Date.class); // throws exception if table does not exist
			fail("Metadata table should not exist before running the test"); // we expect the above to throw
		} catch (BadSqlGrammarException ignored) {
			// metadata table does not exist yet, continue the test
		}

		migrator.migrate();
		jdbcTemplate.queryForObject(selectSql, Date.class); // throws exception if table does not exist
	}
}
