package dk.nsi.sdm4.core.config;

import com.googlecode.flyway.core.Flyway;
import dk.nsi.sdm4.core.parser.DirectoryInbox;
import dk.nsi.sdm4.core.parser.Inbox;
import dk.nsi.sdm4.core.parser.Parser;
import dk.nsi.sdm4.core.parser.ParserExecutor;
import dk.nsi.sdm4.core.status.ImportStatusRepository;
import dk.nsi.sdm4.core.status.ImportStatusRepositoryJdbcImpl;
import dk.nsi.sdm4.core.status.TimeSource;
import dk.nsi.sdm4.core.status.TimeSourceRealTimeImpl;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jndi.JndiObjectFactoryBean;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.sql.DataSource;

/**
 * Configuration class extendended by the concrete subclasses in the parser modules
 */
public abstract class StamdataConfiguration {
	@Value("${sdm.dataDir}") private String dataDir;
	@Value("${jdbc.JNDIName}") private String jdbcJNDIName;

	@Bean
    public ParserExecutor parserExecutor() {
        return new ParserExecutor();
    }

    @Bean(initMethod = "migrate")
    public Flyway flyway(DataSource dataSource) {
        Flyway flyway = new Flyway();
        flyway.setDataSource(dataSource);
        return flyway;
    }

    // this is not automatically registered, see https://jira.springsource.org/browse/SPR-8539
    @Bean
    public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
        PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer = new PropertySourcesPlaceholderConfigurer();
        propertySourcesPlaceholderConfigurer.setIgnoreResourceNotFound(true);
        propertySourcesPlaceholderConfigurer.setIgnoreUnresolvablePlaceholders(false);

        propertySourcesPlaceholderConfigurer.setLocations(new Resource[]{new ClassPathResource("default-config.properties"),new ClassPathResource("config.properties")});
        
        return propertySourcesPlaceholderConfigurer;
    }

	@Bean
	public DataSource dataSource() throws Exception{
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
	public Inbox inbox() throws Exception {
		return new DirectoryInbox(
				dataDir,
				parser().getHome());
	}

	@Bean
	public ImportStatusRepository importStatusRepository() {
		return new ImportStatusRepositoryJdbcImpl();
	}


	@Bean
	public TimeSource timeSource() {
		return new TimeSourceRealTimeImpl();
	}

	public abstract Parser parser();
}
