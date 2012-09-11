package dk.nsi.sdm4.core.config;

import dk.nsi.sdm4.core.parser.DirectoryInbox;
import dk.nsi.sdm4.core.parser.Inbox;
import dk.nsi.sdm4.core.parser.Parser;
import dk.nsi.sdm4.core.parser.ParserExecutor;
import dk.nsi.sdm4.core.persistence.migration.DbMigrator;
import dk.nsi.sdm4.core.persistence.recordpersister.RecordPersister;
import dk.nsi.sdm4.core.status.ImportStatusRepository;
import dk.nsi.sdm4.core.status.ImportStatusRepositoryJdbcImpl;
import dk.nsi.sdm4.core.status.TimeSource;
import dk.nsi.sdm4.core.status.TimeSourceRealTimeImpl;
import org.joda.time.Instant;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.CustomScopeConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.context.support.SimpleThreadScope;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jndi.JndiObjectFactoryBean;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.HashMap;

/**
 * Configuration class extendended by the concrete subclasses in the parser modules,
 * providing the common infrastructure.
 */
public abstract class StamdataConfiguration {
	@Value("${sdm.dataDir}")
	private String dataDir;
	@Value("${jdbc.JNDIName}")
	private String jdbcJNDIName;

	@Bean
	public ParserExecutor parserExecutor() {
		return new ParserExecutor();
	}

	@Bean(initMethod = "migrate")
	public DbMigrator dbMigrator() {
		return new DbMigrator();
	}

	// this is not automatically registered, see https://jira.springsource.org/browse/SPR-8539
	@Bean
	public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
		PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer = new PropertySourcesPlaceholderConfigurer();
		propertySourcesPlaceholderConfigurer.setIgnoreResourceNotFound(true);
		propertySourcesPlaceholderConfigurer.setIgnoreUnresolvablePlaceholders(false);

		propertySourcesPlaceholderConfigurer.setLocations(new Resource[]{new ClassPathResource("default-config.properties"), new ClassPathResource("config.properties")});

		return propertySourcesPlaceholderConfigurer;
	}

	@Bean
	public DataSource dataSource() throws Exception {
		JndiObjectFactoryBean factory = new JndiObjectFactoryBean();
		factory.setJndiName(jdbcJNDIName);
		factory.setExpectedType(DataSource.class);
		factory.afterPropertiesSet();
		return (DataSource) factory.getObject();
	}

	@Bean
	public PlatformTransactionManager transactionManager(DataSource ds) {
		return new DataSourceTransactionManager(ds);
	}

	@Bean
	public JdbcTemplate jdbcTemplate(DataSource dataSource) {
		return new JdbcTemplate(dataSource);
	}

	@Bean
	public Inbox inbox(Parser parser) throws Exception {
		return new DirectoryInbox(
				dataDir,
				parser.getHome());
	}

	@Bean
	public ImportStatusRepository importStatusRepository() {
		return new ImportStatusRepositoryJdbcImpl();
	}

	@Bean
	public TimeSource timeSource() {
		return new TimeSourceRealTimeImpl();
	}

	@Bean
	@Scope(value="thread", proxyMode= ScopedProxyMode.TARGET_CLASS)
	public RecordPersister recordPersister() {
		return new RecordPersister(Instant.now());
	}

	// This needs the static modifier due to https://jira.springsource.org/browse/SPR-8269. If not static, field jdbcJndiName
	// will not be set when trying to instantiate the DataSource
	@Bean
	public static CustomScopeConfigurer scopeConfigurer() {
		return new SimpleThreadScopeConfigurer();
	}
}
