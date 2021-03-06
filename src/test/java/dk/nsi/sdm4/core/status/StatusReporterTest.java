package dk.nsi.sdm4.core.status;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import dk.nsi.sdm4.core.parser.Inbox;
import dk.nsi.sdm4.core.status.ImportStatus.Outcome;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader = AnnotationConfigContextLoader.class)
public class StatusReporterTest {
    @Configuration
    static class TestConfiguration {
        @Bean
        public Inbox inbox() {
            Inbox inbox = mock(Inbox.class);
            return inbox;
        }

        @Bean
        public StatusReporter statusReporter() {
            return new StatusReporter();
        }
        @Bean ImportStatusRepository importStatusRepository() {
            ImportStatusRepository repo = mock(ImportStatusRepository.class);
            return repo;
        }
    }

    @Autowired
    Inbox inbox;
    @Autowired
    StatusReporter reporter;
    @Autowired
    ImportStatusRepository statusRepo;

    
    @Test
    public void willReturn200underNormalCircumstances() throws Exception {
        when(inbox.isLocked()).thenReturn(false);
        when(statusRepo.isDBAlive()).thenReturn(true);
        final ResponseEntity<String> response = reporter.reportStatus();

        assertEquals(200, response.getStatusCode().value());
        assertTrue(response.getBody().startsWith("OK"));
    }

    @Test
    public void errorWithLockedInbox() throws Exception {
        when(inbox.isLocked()).thenReturn(true);
        
        final ResponseEntity<String> response = reporter.reportStatus();

        assertEquals(500, response.getStatusCode().value());
        assertTrue(response.getBody().startsWith("Inbox is locked"));
    }    
    
    @Test
    public void reportsNeverRunWhenImportStatusDoesNotExist() {
        when(statusRepo.getLatestStatus()).thenReturn(null);
        when(inbox.isLocked()).thenReturn(false);
        
        final ResponseEntity<String> response = reporter.reportStatus();
        assertEquals(200, response.getStatusCode().value());
        assertTrue(response.getBody().toLowerCase().contains("never run"));
    }
    
    @Test
    public void reportsStartTimeOfLastSuccess() {
        ImportStatus status = new ImportStatus();
        DateTime startTime = new DateTime();
        status.setStartTime(startTime);
        status.setOutcome(Outcome.SUCCESS);
        
        when(statusRepo.getLatestStatus()).thenReturn(status);
        when(inbox.isLocked()).thenReturn(false);
        
        final ResponseEntity<String> response = reporter.reportStatus();
        assertEquals(200, response.getStatusCode().value());
        
        assertTrue(response.getBody().contains(startTime.toString()));
    }

	@Test
	public void reportsEndTimeOfLastSuccess() {
		ImportStatus status = new ImportStatus();
		DateTime endTime = new DateTime();
		status.setEndTime(endTime);
		status.setOutcome(Outcome.SUCCESS);

		when(statusRepo.getLatestStatus()).thenReturn(status);
		when(inbox.isLocked()).thenReturn(false);

		final ResponseEntity<String> response = reporter.reportStatus();
		assertEquals(200, response.getStatusCode().value());

		assertTrue(response.getBody().contains(endTime.toString()));
	}

	@Test
    public void reports200OKevenWhenLastRunWasAFailureButTheTextSaysThatItFailed() {
	    // another test ensures that when inbox is locked, 500 is returned. The fact that something failed previously should
	    // not give operations warnings if they've taken care of the LOCKED file in the inbox
        ImportStatus status = new ImportStatus();
        DateTime startTime = new DateTime();
        status.setStartTime(startTime);
		DateTime endTime = new DateTime();
		status.setEndTime(endTime);
		status.setOutcome(Outcome.FAILURE);
        
        when(statusRepo.getLatestStatus()).thenReturn(status);
        when(inbox.isLocked()).thenReturn(false);
        
        final ResponseEntity<String> response = reporter.reportStatus();
        assertEquals(200, response.getStatusCode().value());
        assertTrue(response.getBody().contains(Outcome.FAILURE.toString()));
    }

	@Test
	public void reportsErrorWhenOverdueEvenWhenLastRunIsSuccess() {
		ImportStatus status = new ImportStatus();
		DateTime startTime = new DateTime();
		status.setStartTime(startTime);
		status.setOutcome(Outcome.SUCCESS);

		when(statusRepo.getLatestStatus()).thenReturn(status);
		when(inbox.isLocked()).thenReturn(false);
		when(statusRepo.isOverdue()).thenReturn(true);

		final ResponseEntity<String> response = reporter.reportStatus();
		assertEquals(500, response.getStatusCode().value());
		assertTrue("nævner starttid for sidste kørsel", response.getBody().contains(startTime.toString()));
		assertTrue("nævner at job er overdue", response.getBody().toLowerCase().contains("overdue"));
	}

	@Test
	public void reportsFailureAndMentionsBothConditionsWhenInboxIsLockedAndJobIsOverdue() {
		ImportStatus status = new ImportStatus();
		DateTime startTime = new DateTime();
		status.setStartTime(startTime);
		status.setOutcome(Outcome.SUCCESS);

		when(statusRepo.getLatestStatus()).thenReturn(status);
		when(inbox.isLocked()).thenReturn(false);
		when(statusRepo.isOverdue()).thenReturn(true);

		final ResponseEntity<String> response = reporter.reportStatus();
		assertEquals(500, response.getStatusCode().value());
		assertTrue("nævner starttid for sidste kørsel", response.getBody().contains(startTime.toString()));
		assertTrue("nævner at job er overdue", response.getBody().toLowerCase().contains("overdue"));
	}

    @Test
    public void reportsDBIsAlive() {
        when(statusRepo.isDBAlive()).thenReturn(true);
        when(inbox.isLocked()).thenReturn(false);
        when(statusRepo.isOverdue()).thenReturn(false);

        final ResponseEntity<String> response = reporter.reportStatus();
        assertEquals(200, response.getStatusCode().value());

        assertFalse(response.getBody().contains("Database is _NOT_ running correctly"));
    }

    @Test
    public void reportsDBIsNotAlive() {
        when(statusRepo.isDBAlive()).thenReturn(false);
        when(inbox.isLocked()).thenReturn(false);

        final ResponseEntity<String> response = reporter.reportStatus();
        assertEquals(500, response.getStatusCode().value());

        assertTrue(response.getBody().contains("Database is _NOT_ running correctly"));
    }
}
