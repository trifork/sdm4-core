package dk.nsi.sdm4.core.config;

import dk.nsi.sdm4.core.parser.Inbox;
import dk.nsi.sdm4.core.parser.ParserExecutor;
import dk.nsi.sdm4.core.persistence.migration.DbMigrator;
import dk.sdsd.nsp.slalog.api.SLALogger;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.sql.DataSource;

import static org.mockito.Mockito.mock;

@Configuration
@EnableTransactionManagement
@PropertySource("test.properties")
public class StamdataTestConfiguration extends StamdataConfiguration {
    //Make sure to override all methods on StamdataConfiguration with mock methods

    @Bean
    public DataSource dataSource() {
        return mock(DataSource.class);
    }

    @Bean
    public DbMigrator dbMigrator() {
        return null;
    }

	@Bean
	public SLALogger slaLogger() {
		return mock(SLALogger.class);
	}
}
