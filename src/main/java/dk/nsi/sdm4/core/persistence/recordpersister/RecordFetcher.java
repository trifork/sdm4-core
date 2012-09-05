/**
 * The contents of this file are subject to the Mozilla Public
 * License Version 1.1 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at http://www.mozilla.org/MPL/
 *
 * Software distributed under the License is distributed on an "AS
 * IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * rights and limitations under the License.
 *
 * Contributor(s): Contributors are attributed in the source code
 * where applicable.
 *
 * The Original Code is "Stamdata".
 *
 * The Initial Developer of the Original Code is Trifork Public A/S.
 *
 * Portions created for the Original Code are Copyright 2011,
 * Lægemiddelstyrelsen. All Rights Reserved.
 *
 * Portions created for the FMKi Project are Copyright 2011,
 * National Board of e-Health (NSI). All Rights Reserved.
 */
package dk.nsi.sdm4.core.persistence.recordpersister;

import org.joda.time.Instant;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static dk.nsi.sdm4.core.persistence.recordpersister.FieldSpecification.RecordFieldType.ALPHANUMERICAL;
import static dk.nsi.sdm4.core.persistence.recordpersister.FieldSpecification.RecordFieldType.NUMERICAL;


public class RecordFetcher {
	@Autowired
	private JdbcTemplate jdbcTemplate;

	public Record fetchCurrent(String key, RecordSpecification recordSpecification, String lookupColumn) throws SQLException {
		SqlRowSet resultSet = jdbcTemplate.queryForRowSet(String.format("SELECT * FROM %s WHERE %s = ? AND validTo IS NULL", recordSpecification.getTable(), lookupColumn), key);
		if (resultSet.next()) {
			return createRecordFromResultSet(recordSpecification, resultSet);
		} else {
			return null;
		}
	}

	public Record fetchCurrent(String key, RecordSpecification recordSpecification) throws SQLException {
		return fetchCurrent(key, recordSpecification, recordSpecification.getKeyColumn());
	}

	public List<RecordMetadata> fetchSince(RecordSpecification recordSpecification, long fromPID, Instant fromModifiedDate, int limit) throws SQLException {
		String queryString = String.format("SELECT * FROM %s WHERE " +
				"(PID > ? AND ModifiedDate = ?) OR " +
				"PID > ? OR " +
				"(PID = ? AND ModifiedDate > ?) " +
				"ORDER BY ModifiedDate, PID LIMIT %d", recordSpecification.getTable(), limit);


		Timestamp fromModifiedDateAsTimestamp = new Timestamp(fromModifiedDate.getMillis());

		SqlRowSet resultSet = jdbcTemplate.queryForRowSet(queryString,
				fromPID,
				fromModifiedDateAsTimestamp,
				fromPID,
				fromPID,
				fromModifiedDateAsTimestamp);


		List<RecordMetadata> result = new ArrayList<RecordMetadata>();
		while (resultSet.next()) {
			Instant validFrom = new Instant(resultSet.getTimestamp("ValidFrom"));
			Instant validTo = new Instant(resultSet.getTimestamp("ValidTo"));
			Instant modifiedDate = new Instant(resultSet.getTimestamp("ModifiedDate"));
			Long pid = (Long) resultSet.getObject("PID");
			Record record = createRecordFromResultSet(recordSpecification, resultSet);
			RecordMetadata recordMetadata = new RecordMetadata(validFrom, validTo, modifiedDate, pid, record);
			result.add(recordMetadata);
		}

		return result;
	}

	private Record createRecordFromResultSet(RecordSpecification recordSpecification, SqlRowSet resultSet) throws SQLException {
		RecordBuilder builder = new RecordBuilder(recordSpecification);

		for (FieldSpecification fieldSpec : recordSpecification.getFieldSpecs()) {
			if (fieldSpec.persistField) {
				String fieldName = fieldSpec.name;

				if (fieldSpec.type == NUMERICAL) {
					builder.field(fieldName, resultSet.getInt(fieldName));
				} else if (fieldSpec.type == ALPHANUMERICAL) {
					builder.field(fieldName, resultSet.getString(fieldName));
				} else {
					throw new AssertionError("Invalid field specifier used");
				}
			}
		}

		return builder.build();
	}
}
