package dk.nsi.sdm4.core.parser;

import dk.nsi.sdm4.core.persistence.recordpersister.RecordPersister;
import dk.nsi.sdm4.core.status.ImportStatusRepository;
import dk.sdsd.nsp.slalog.api.SLALogger;
import dk.sdsd.nsp.slalog.impl.SLALoggerDummyImpl;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader = AnnotationConfigContextLoader.class)
public class ParserExecutorTest {
	@Rule
	public TemporaryFolder tmpDir = new TemporaryFolder();

	@Configuration
	static class TestConfiguration {
		@Bean
		public ParserExecutor parserExecutor() {
			return new ParserExecutor();

		}

		@Bean
		public Inbox inbox() {
			return Mockito.mock(Inbox.class);
		}


		@Bean
		public Parser parser() {
			return Mockito.mock(Parser.class);
		}

		@Bean
		public JdbcTemplate template() {
			return Mockito.mock(JdbcTemplate.class); // needed because the RecordPersister class is mocked, but still has its @Autowired fields
		}

		@Bean
		public RecordPersister persister() {
			return Mockito.mock(RecordPersister.class);
		}

		@Bean
		ImportStatusRepository importStatusRepository() {
			return Mockito.mock(ImportStatusRepository.class);
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
		Mockito.reset(inbox, parser, statusRepo, persister);
	}

	@Test
	public void shouldNotThrowAnExceptionWhenInboxIsLocked() throws IOException {
		whenInboxIsLocked();
		executor.run();
	}

	@Test
	public void shouldLockAParserIfItFails() throws Exception {
		whenInboxIsNotLockedAndHasSomeFileInIt();
		Mockito.doThrow(new RuntimeException()).when(parser).process(any(File.class));

		try {
			executor.run();
		} catch (Exception ignore) {
			// we expect the executor to throw an exception in this case
			Mockito.verify(inbox).lock();
			return;
		}

		fail("expected exception from executor");
	}

	@Test
	public void shouldNotAttemptExecutionIfTheInboxIsLocked() throws Exception {
		whenInboxIsLocked();

		executor.run();

		Mockito.verify(parser, Mockito.times(0)).process(any(File.class));
	}

	@Test
	public void shouldCallUpdateBeforeCheckingTheInboxForItems() throws Exception {
		whenInboxIsNotLocked();

		executor.run();

		InOrder inOrder = Mockito.inOrder(inbox);

		inOrder.verify(inbox).update();
		inOrder.verify(inbox).top();
	}

	@Test
	public void shouldCallProcessOnParserBeforeAdvancingTheInbox() throws Exception {
		whenInboxIsNotLockedAndHasSomeFileInIt();

		executor.run();

		InOrder inOrder = Mockito.inOrder(parser, inbox);

		inOrder.verify(parser).process(any(File.class));
		inOrder.verify(inbox).advance();
	}

	@Test
	public void shouldSetTheLatestRunTimestampIfItActuallyRan() throws Exception {
		whenInboxIsNotLockedAndHasSomeFileInIt();

		executor.run();

		Mockito.verify(statusRepo).importStartedAt(any(DateTime.class));
		Mockito.verify(statusRepo).importEndedWithSuccess(any(DateTime.class));
	}

	@Test
	public void shouldSetErrorStatusIfTheParserThrowsException() throws Exception {
		whenInboxIsNotLockedAndHasSomeFileInIt();
		Mockito.doThrow(new RuntimeException("parser cannot parse")).when(parser).process(any(File.class));

		try {
			executor.run();
		} catch (RuntimeException ignore) {
			// we expect the parser's exception to be rethrown
		}

		Mockito.verify(statusRepo).importStartedAt(any(DateTime.class));
		Mockito.verify(statusRepo).importEndedWithFailure(any(DateTime.class));
	}

	@Test
	public void shouldRethrowIfTheParserThrowsException() throws Exception {
		whenInboxIsNotLockedAndHasSomeFileInIt();
		RuntimeException parserException = new RuntimeException("parser cannot parse");
		Mockito.doThrow(parserException).when(parser).process(any(File.class));

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
		Mockito.when(inbox.top()).thenReturn(null);

		executor.run();

		Mockito.verifyZeroInteractions(statusRepo);
	}

	@Test
	public void shouldResetTheRecordPersisterBeforeRunningParser() throws IOException {
		whenInboxIsNotLockedAndHasSomeFileInIt();

		executor.run();

		Mockito.verify(persister).resetTransactionTime();
		Mockito.verify(parser).process(any(File.class));
	}

	@Test
	public void shouldLogTheDatasetFilenameAndContentsBeforeHandingItToTheParser() throws Exception {
		Logger logger = Mockito.mock(Logger.class);
		ParserExecutor.logger = logger;

		whenInboxIsNotLocked();
		File dataset = tmpDir.newFolder("datasetFilename");
		File file1 = createFile(dataset, "file1");
		File file2 = createFile(dataset, "file2");

		Mockito.when(inbox.top()).thenReturn(dataset);

		executor.run();

		ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
		Mockito.verify(logger).info(captor.capture());
		String loggedMsg = captor.getValue();

		assertTrue("logs dataset path", loggedMsg.contains(dataset.getAbsolutePath()));
		assertTrue("logs file1 path", loggedMsg.contains(file1.getAbsolutePath()));
		assertTrue("logs file2 path", loggedMsg.contains(file2.getAbsolutePath()));
		assertTrue("logs md5 hash", loggedMsg.contains("md5=250fd79a4936f66847582a144baf5492")); // we know from md5 in a shell that this is the md5 sum
		// our two files has the same md5 hash, so we just assert once
	}

	/** creates af file with contents with a known hash of 250fd79a4936f66847582a144baf5492 **/
	private File createFile(File parent, String name) throws IOException {
		File file = new File(parent, name);
		assertTrue(file.createNewFile());

		FileWriter writer = new FileWriter(file);
		writer.write("thisIsJustTheFileContents");
		writer.close();

		return file;
	}

	private void whenInboxIsLocked() throws IOException {
		Mockito.when(inbox.isLocked()).thenReturn(true);
		Mockito.doThrow(new IllegalStateException("Inbox is locked")).when(inbox).top();
		Mockito.doThrow(new IllegalStateException("Inbox is locked")).when(inbox).update();
	}

	private void whenInboxIsNotLocked() throws IOException {
		Mockito.when(inbox.isLocked()).thenReturn(false);
	}

	private void whenInboxIsNotLockedAndHasSomeFileInIt() throws IOException {
		whenInboxIsNotLocked();
		Mockito.when(inbox.top()).thenReturn(new File("dummyFile"));
	}
}
