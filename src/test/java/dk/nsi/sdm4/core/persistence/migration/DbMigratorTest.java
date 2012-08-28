package dk.nsi.sdm4.core.persistence.migration;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;

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

	@Test
	public void youCanCallMigrateWithoutErrors() {
		migrator.migrate();
	}

	@Test
	public void metadataTableIsCreatedAutomatically() {
		migrator.migrate();
		jdbcTemplate.queryForObject("SELECT max(installed_on) from " + DbMigrator.METADATA_TABLE_NAME, Date.class); // throws exception if table does not exist
	}
}
