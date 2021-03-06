package dk.nsi.sdm4.core.status;

import dk.nsi.sdm4.core.parser.Parser;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

@Repository
public class ImportStatusRepositoryJdbcImpl implements ImportStatusRepository {
	private static final Logger logger = Logger.getLogger(ImportStatusRepositoryJdbcImpl.class);

	@Value("${spooler.max.days.between.runs}")
	private int maxDaysBetweenRuns;

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Autowired
	private Parser parser;

	@Autowired
	private TimeSource timeSource;

	private String statusTableName;

	@PostConstruct
	private void constructTableNameFromParser() {
		statusTableName = parser.getHome() + "ImportStatus";
	}

	@Override
	@Transactional(propagation = Propagation.REQUIRES_NEW)
	public void importStartedAt(DateTime startTime) {
		jdbcTemplate.update("INSERT INTO " + statusTableName + " (StartTime) values (?)", startTime.toDate());
	}

	@Override
	@Transactional(propagation = Propagation.MANDATORY)
	public void importEndedWithSuccess(DateTime endTime) {
		importEndedAt(endTime, ImportStatus.Outcome.SUCCESS);
	}

	@Override
	@Transactional(propagation = Propagation.REQUIRES_NEW)
	public void importEndedWithFailure(DateTime endTime) {
		importEndedAt(endTime, ImportStatus.Outcome.FAILURE);
	}

	private void importEndedAt(DateTime endTime, ImportStatus.Outcome outcome) {
		Long newestOpenId;
		try {
			newestOpenId = jdbcTemplate.queryForLong("SELECT Id from " + statusTableName + " ORDER BY StartTime DESC LIMIT 1");
		} catch (EmptyResultDataAccessException e) {
			// it seems we do not have any open statuses, let's not update
			return;
		}

		jdbcTemplate.update("UPDATE " + statusTableName + " SET EndTime=?, Outcome=? WHERE Id=?", endTime.toDate(), outcome.toString(), newestOpenId);
	}

	@Override
	public ImportStatus getLatestStatus() {
		try {
			return jdbcTemplate.queryForObject("SELECT * from " + statusTableName + " ORDER BY StartTime DESC LIMIT 1", new ImportStatusRowMapper());
		} catch (EmptyResultDataAccessException ignored) {
			// that's not a problem, we just don't have any statuses
			return null;
		}
	}

	@Override
	/**
	 *        {maxDaysBetweenRuns}
	 *     <-----------------------------
	 *
	 * ____'________|________'___________|___________
	 *           lastRun                now
	 *     ^                 ^
	 *   !overdue         overdue
	 */
	public boolean isOverdue() {
		ImportStatus latestStatus = getLatestStatus();
		if (latestStatus == null) {
			// we're not overdue if we have never run
			return false;
		}


		DateTime lastRun = latestStatus.getStartTime();
		DateTime now = timeSource.now();
		return (now.minusDays(maxDaysBetweenRuns).isAfter(lastRun));
	}


	private class ImportStatusRowMapper implements RowMapper<ImportStatus> {

		@Override
		public ImportStatus mapRow(ResultSet rs, int rowNum) throws SQLException {
			ImportStatus status = new ImportStatus();
			status.setStartTime(new DateTime(rs.getTimestamp("StartTime")));
			Timestamp endTime = rs.getTimestamp("EndTime");
			if (endTime != null) {
				status.setEndTime(new DateTime(endTime));
			}
			String dbOutcome = rs.getString("Outcome");
			if (dbOutcome != null) {
				status.setOutcome(ImportStatus.Outcome.valueOf(dbOutcome));
			}

			return status;
		}
	}

	@Override
	public boolean isDBAlive() {
		try {
			jdbcTemplate.queryForObject("Select max(id) from " + statusTableName, Long.class);
		} catch (Exception someError) {
			logger.debug("Exception when trying to check database", someError);
			return false;
		}
		return true;
	}
}
