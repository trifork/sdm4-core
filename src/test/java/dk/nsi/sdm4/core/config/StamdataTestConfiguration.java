package dk.nsi.sdm4.core.config;

import com.googlecode.flyway.core.Flyway;
import dk.nsi.sdm4.core.parser.Inbox;
import dk.nsi.sdm4.core.parser.Parser;
import dk.nsi.sdm4.core.parser.ParserExecutor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.oxm.Unmarshaller;
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
    public ParserExecutor parserExecutor() {
        //ParserExecutor currently doesn't have a interface, and cannot be mocked
        return null;
    }

    @Bean
    public Flyway flyway(DataSource dataSource) {
        return null;
    }

    @Bean
    public Inbox inbox() {
        return mock(Inbox.class);
    }

	@Override
	public Parser parser() {
		return mock(Parser.class);
	}
}
