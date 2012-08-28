package dk.nsi.sdm4.core.persistence.migration;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;
import org.springframework.test.context.transaction.BeforeTransaction;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
			DbMigrator migrator = new DbMigrator();
			migrator.migrationFinder = migrationFinder();
			return migrator;
		}

		@Bean
		public MigrationFinder migrationFinder() {
			return mock(MigrationFinder.class);
		}
	}

	@Autowired
	private DbMigrator migrator;

	@Autowired
	JdbcTemplate jdbcTemplate;

	@Autowired
	MigrationFinder migrationFinder;

	@BeforeTransaction
	public void clearSchema() {
		jdbcTemplate.update("DROP Table if exists " + DbMigrator.METADATA_TABLE_NAME);
		jdbcTemplate.update("DROP Table if exists TestMigration1");
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

	@Test
	public void willRun1Migration() {
		migrateWith("db/migration/V20010101_0101__TestMigration1.sql");

		migrator.migrate();

		jdbcTemplate.queryForObject("SELECT max(TestColumn) from TestMigration1", Date.class); // throws exception if table does not exist
	}

	@Test
	public void willRun2MigrationsInTheOrderProvidedByTheFinder() {
		// we test elsewhere that the default migrationfinder sorts the migrations
		migrateWith("testmigrations/V20010101_0101__TestMigration1.sql",
				"testmigrations/V20010103_0101__TestMigration3.sql");

		migrator.migrate();

		jdbcTemplate.queryForObject("SELECT max(TestColumnAfterAlter) from TestMigration1", Date.class); // throws exception if table does not exist
	}

	@Test
	public void willIgnoreMigrationsItHasRunBefore() {
		migrateWith("testmigrations/V20010101_0101__TestMigration1.sql");
		migrator.migrate();

		migrateWith("testmigrations/V20010101_0101__TestMigration1.sql"); // this migration will fail if run two times
		migrator.migrate();
	}

	@Test
	public void willCreateTwoTablesInOneMigration() {
		migrateWith("testmigrations/V19990000_0000__MultipleCreateTableStatements.sql");

		migrator.migrate();

		jdbcTemplate.queryForObject("SELECT max(TestColumn) from Multiple1", Date.class); // throws exception if table does not exist
		jdbcTemplate.queryForObject("SELECT max(TestColumn) from Multiple2", Date.class); // throws exception if table does not exist

	}

	@Test
	public void willRunOlderMigrationsIfNotSeenBefore() {
		migrateWith("testmigrations/V20010101_0101__TestMigration1.sql",
				"testmigrations/V20010103_0101__TestMigration3.sql");

		migrator.migrate();
		assertEquals(2, jdbcTemplate.queryForInt("SELECT DISTINCT count(version) from " + DbMigrator.METADATA_TABLE_NAME + " WHERE version=? or version=?",
				"20010101_0101", "20010103_0101")); // make sure the two migrations are run

		migrateWith("testmigrations/V20010102_0101__TestMigration2.sql");
		migrator.migrate();


		jdbcTemplate.queryForObject("SELECT max(TestColumn) from TestMigration2", Date.class); // throws exception if table does not exist
	}

	private void migrateWith(String... migrationPaths) {
		List<Migration> migrations = new ArrayList<Migration>();

		for (String migrationPath : migrationPaths) {
			migrations.add(new Migration(new ClassPathResource(migrationPath)));
		}

		when(migrationFinder.findMigrations()).thenReturn(migrations);
	}
}
