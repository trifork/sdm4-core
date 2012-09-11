package dk.nsi.sdm4.core.status;

import org.joda.time.DateTime;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Represents the import status and deadline information for a single parser
 */
public interface ImportStatusRepository {
	void importStartedAt(DateTime startTime);

	ImportStatus getLatestStatus();

	boolean isOverdue();

	boolean isDBAlive();

	void importEndedWithSuccess(DateTime endTime);

	void importEndedWithFailure(DateTime endTime);
}
