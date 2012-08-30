package dk.nsi.sdm4.core.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import javax.inject.Inject;

import dk.nsi.sdm4.core.parser.ParserExecutor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import dk.nsi.sdm4.core.parser.Inbox;
import dk.nsi.sdm4.core.parser.Parser;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {CommonInfrastructureConfigTest.TestConfiguration.class})
public class CommonInfrastructureConfigTest {
    @Inject
    ParserExecutor executor;

    @Inject
    Inbox inbox;

    @Configuration
    @Import({StamdataTestConfiguration.class})
    static class TestConfiguration {
        @Bean
        public Parser parser() {
	        Parser parser = mock(Parser.class);
	        when(parser.getHome()).thenReturn("testParser");
	        return parser;
        }
    }

    @Test
    public void canCreateStamdataConfiguration() throws Exception {
        assertNotNull(inbox);
	    assertNotNull(executor);
    }
}
