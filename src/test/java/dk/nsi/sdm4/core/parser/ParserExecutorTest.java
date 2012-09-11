package dk.nsi.sdm4.core.parser;

import dk.nsi.sdm4.core.persistence.recordpersister.RecordPersister;
import dk.nsi.sdm4.core.status.ImportStatusRepository;
import dk.sdsd.nsp.slalog.api.SLALogger;
import dk.sdsd.nsp.slalog.impl.SLALoggerDummyImpl;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader = AnnotationConfigContextLoader.class)
public class ParserExecutorTest {
	@Configuration
	static class TestConfiguration {
		@Bean
		public ParserExecutor parserExecutor() {
			return new ParserExecutor();

		}

		@Bean
		public Inbox inbox() {
			return mock(Inbox.class);
		}


		@Bean
		public Parser parser() {
			return mock(Parser.class);
		}

		@Bean
		public JdbcTemplate template() {
			return mock(JdbcTemplate.class); // needed because the RecordPersister class is mocked, but still has its @Autowired fields
		}

		@Bean
		public RecordPersister persister() {
			return mock(RecordPersister.class);
		}

		@Bean
		ImportStatusRepository importStatusRepository() {
			ImportStatusRepository repo = mock(ImportStatusRepository.class);
			return repo;
		}

		@Bean
		public SLALogger slaLogger() {
			return new SLALoggerDummyImpl();
		}
	}

	@Autowired
	Inbox inbox;

	@Autowired
	Parser parser;

	@Autowired
	ParserExecutor executor;

	@Autowired
	ImportStatusRepository statusRepo;

	@Autowired
	RecordPersister persister;

	@Before
	public void resetMocks() {
		reset(inbox, parser, statusRepo, persister);
	}

	@Test
	public void shouldNotThrowAnExceptionWhenInboxIsLocked() throws IOException {
		whenInboxIsLocked();
		executor.run();
	}

	@Test
	public void shouldLockAParserIfItFails() throws Exception {
		whenInboxIsNotLockedAndHasSomeFileInIt();
		doThrow(new RuntimeException()).when(parser).process(any(File.class));

		try {
			executor.run();
		} catch (Exception ignore) {
			// we expect the executor to throw an exception in this case
			verify(inbox).lock();
			return;
		}

		fail("expected exception from executor");
	}

	@Test
	public void shouldNotAttemptExecutionIfTheInboxIsLocked() throws Exception {
		whenInboxIsLocked();

		executor.run();

		verify(parser, times(0)).process(any(File.class));
	}

	@Test
	public void shouldCallUpdateBeforeCheckingTheInboxForItems() throws Exception {
		whenInboxIsNotLocked();

		executor.run();

		InOrder inOrder = inOrder(inbox);

		inOrder.verify(inbox).update();
		inOrder.verify(inbox).top();
	}

	@Test
	public void shouldCallProcessOnParserBeforeAdvancingTheInbox() throws Exception {
		whenInboxIsNotLockedAndHasSomeFileInIt();

		executor.run();

		InOrder inOrder = inOrder(parser, inbox);

		inOrder.verify(parser).process(any(File.class));
		inOrder.verify(inbox).advance();
	}

	@Test
	public void shouldSetTheLatestRunTimestampIfItActuallyRan() throws Exception {
		whenInboxIsNotLockedAndHasSomeFileInIt();

		executor.run();

		verify(statusRepo).importStartedAt(any(DateTime.class));
		verify(statusRepo).importEndedWithSuccess(any(DateTime.class));
	}

	@Test
	public void shouldSetErrorStatusIfTheParserThrowsException() throws Exception {
		whenInboxIsNotLockedAndHasSomeFileInIt();
		doThrow(new RuntimeException("parser cannot parse")).when(parser).process(any(File.class));

		try {
			executor.run();
		} catch (RuntimeException ignore) {
			// we expect the parser's exception to be rethrown
		}

		verify(statusRepo).importStartedAt(any(DateTime.class));
		verify(statusRepo).importEndedWithFailure(any(DateTime.class));
	}

	@Test
	public void shouldRethrowIfTheParserThrowsException() throws Exception {
		whenInboxIsNotLockedAndHasSomeFileInIt();
		RuntimeException parserException = new RuntimeException("parser cannot parse");
		doThrow(parserException).when(parser).process(any(File.class));

		try {
			executor.run();
			fail("expected executor to rethrow parser's exception");
		} catch (RuntimeException executorException) {
			// we expect the parser's exception to be rethrown wrapped in a RuntimeException
			assertSame(parserException, executorException.getCause());
		}
	}

	@Test
	public void shouldNotSetTheLatestRunTimestampIfItDidNotRun() throws Exception {
		when(inbox.top()).thenReturn(null);

		executor.run();

		verifyZeroInteractions(statusRepo);
	}

	@Test
	public void shouldResetTheRecordPersisterBeforeRunningParser() throws IOException {
		whenInboxIsNotLockedAndHasSomeFileInIt();

		executor.run();

		verify(persister).resetTransactionTime();
		verify(parser).process(any(File.class));
	}

	private void whenInboxIsLocked() throws IOException {
		when(inbox.isLocked()).thenReturn(true);
		doThrow(new IllegalStateException("Inbox is locked")).when(inbox).top();
		doThrow(new IllegalStateException("Inbox is locked")).when(inbox).update();
	}

	private void whenInboxIsNotLocked() throws IOException {
		when(inbox.isLocked()).thenReturn(false);
	}

	private void whenInboxIsNotLockedAndHasSomeFileInIt() throws IOException {
		whenInboxIsNotLocked();
		when(inbox.top()).thenReturn(new File("dummyFile"));
	}

}
