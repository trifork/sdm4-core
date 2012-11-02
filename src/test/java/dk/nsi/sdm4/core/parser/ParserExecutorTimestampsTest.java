package dk.nsi.sdm4.core.parser;

import dk.nsi.sdm4.core.config.SimpleThreadScopeConfigurer;
import dk.nsi.sdm4.core.persistence.recordpersister.*;
import dk.nsi.sdm4.core.status.ImportStatusRepository;
import dk.sdsd.nsp.slalog.api.SLALogger;
import dk.sdsd.nsp.slalog.impl.SLALoggerDummyImpl;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.CustomScopeConfigurer;
import org.springframework.context.annotation.*;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests that the ParserExecutor executes a parser with an injected RecordPersister in such a way that all records
 * from a single set of files (ie. the value of an inbox.top() operation) end up with the same (distinct) ModifiedDate
 * in the database
 */
@RunWith(SpringJUnit4ClassRunner.class)
@Transactional
@ContextConfiguration(loader = AnnotationConfigContextLoader.class)
public class ParserExecutorTimestampsTest {
	@Rule
	public TemporaryFolder tmpDir = new TemporaryFolder();

	@Configuration
	@PropertySource("classpath:test.properties")
	static class ContextConfiguration {
		@Bean
		public ParserExecutor executor() {
			return new ParserExecutor();
		}

		@Bean
		public JdbcTemplate template() {
			return new JdbcTemplate(datasource());
		}

		@Bean
		public DataSource datasource() {
			return new EmbeddedDatabaseBuilder()
					.addScript("ParserExecutorTimestampsTest-schema.sql")
					.build();
		}

		@Bean
		public Parser fakeParser() {
			return new Parser() {
				public final int NUMBER_OF_RECORDS_TO_CREATE = 2;
				public final RecordSpecification RECORD_SPECIFICATION = RecordSpecification.createSpecification("ParserExecutorTimestampsTest", "Column1", FieldSpecification.field("Column1", 1));

				@Override
				public void process(File dataSet) throws ParserException {
					for (int i = 0; i < NUMBER_OF_RECORDS_TO_CREATE; i++) {
						Record record = new RecordBuilder(RECORD_SPECIFICATION).field("Column1", "v").build();
						try {
							persister().persist(record, RECORD_SPECIFICATION);
						} catch (SQLException e) {
							throw new ParserException(e);
						}
					}
				}

				@Override
				public String getHome() {
					return "fakeParser";
				}
			};
		}

		@Bean
		public PlatformTransactionManager transactionManager(DataSource ds) {
			return new DataSourceTransactionManager(ds);
		}

		@Bean
		public PropertySourcesPlaceholderConfigurer properties(){
			return new PropertySourcesPlaceholderConfigurer();
		}

		// this definition needs to be identical to the one in StamdataConfiguration, as the modified date behaviour depends on this
		@Bean
		@Scope(value="thread", proxyMode= ScopedProxyMode.TARGET_CLASS)
		public RecordPersister persister() {
			return new RecordPersister(Instant.now());
		}

		@Bean
		public Inbox inbox() {
			return mock(Inbox.class);
		}

		@Bean
		ImportStatusRepository importStatusRepository() {
			return mock(ImportStatusRepository.class);
		}

		@Bean
		SLALogger slaLogger() {
			return new SLALoggerDummyImpl();
		}

		@Bean
		static CustomScopeConfigurer scopeConfigurer() {
			return new SimpleThreadScopeConfigurer();
		}
	}

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Autowired
	RecordPersister persister;

	@Autowired
	ParserExecutor executor;

	@Autowired
	Inbox inbox;

	@Test
	public void twoRecordsFromSameRunHasSameModifiedDate() throws IOException {
		whenInboxIsNotLockedAndHasSomeFileInIt();
		executor.run();
		assertEquals(2, jdbcTemplate.queryForInt("SELECT COUNT(*) from ParserExecutorTimestampsTest"));
		assertEquals(1, jdbcTemplate.queryForInt("SELECT COUNT (DISTINCT ModifiedDate) from ParserExecutorTimestampsTest"));
	}

	@Test
	public void recordsFromDifferentRunsHasDifferentModifiedDateValues() throws IOException {
		whenInboxIsNotLockedAndHasSomeFileInIt();
		executor.run();
		executor.run();
		assertEquals(4, jdbcTemplate.queryForInt("SELECT COUNT(*) from ParserExecutorTimestampsTest"));
		assertEquals(2, jdbcTemplate.queryForInt("SELECT COUNT (DISTINCT ModifiedDate) from ParserExecutorTimestampsTest"));
	}

	@Test
	public void twoRecordsFromSameRunHasSameValidFrom() throws IOException {
		whenInboxIsNotLockedAndHasSomeFileInIt();
		executor.run();
		assertEquals(2, jdbcTemplate.queryForInt("SELECT COUNT(*) from ParserExecutorTimestampsTest"));
		assertEquals(1, jdbcTemplate.queryForInt("SELECT COUNT (DISTINCT ValidFrom) from ParserExecutorTimestampsTest"));
	}

	@Test
	public void recordsFromDifferentRunsHasDifferentValidFromValues() throws IOException {
		whenInboxIsNotLockedAndHasSomeFileInIt();
		executor.run();
		executor.run();
		assertEquals(4, jdbcTemplate.queryForInt("SELECT COUNT(*) from ParserExecutorTimestampsTest"));
		assertEquals(2, jdbcTemplate.queryForInt("SELECT COUNT (DISTINCT ValidFrom) from ParserExecutorTimestampsTest"));
	}

	private void whenInboxIsNotLockedAndHasSomeFileInIt() throws IOException {
		when(inbox.isLocked()).thenReturn(false);

		File dataset = tmpDir.newFolder("datasetFilename");
		createFile(dataset, "dummyFile");

		Mockito.when(inbox.top()).thenReturn(dataset);
	}

	private File createFile(File parent, String name) throws IOException {
		File file = new File(parent, name);
		assertTrue(file.createNewFile());

		return file;
	}
}