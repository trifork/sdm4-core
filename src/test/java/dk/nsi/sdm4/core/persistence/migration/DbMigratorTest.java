package dk.nsi.sdm4.core.persistence.migration;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;
import org.springframework.test.context.transaction.BeforeTransaction;
import org.springframework.transaction.annotation.Transactional;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

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
			return new DbMigrator();
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
		List<Migration> migrations = Arrays.asList(
				new Migration(new ClassPathResource("testmigrations/V20010101_0101__TestMigration1.sql")));
		when(migrationFinder.findMigrations()).thenReturn(migrations);

		migrator.migrate();

		jdbcTemplate.queryForObject("SELECT max(TestColumn) from TestMigration1", Date.class); // throws exception if table does not exist
	}

	@Test
	public void willRun2MigrationsInCorrectOrder() {
		List<Migration> migrations = Arrays.asList(
				new Migration(new ClassPathResource("testmigrations/V20010102_0101__TestMigration2.sql")),
				new Migration(new ClassPathResource("testmigrations/V20010101_0101__TestMigration1.sql")));
		when(migrationFinder.findMigrations()).thenReturn(migrations);

		migrator.migrate();

		jdbcTemplate.queryForObject("SELECT max(TestColumnAfterAlter) from TestMigration1", Date.class); // throws exception if table does not exist
	}

}
