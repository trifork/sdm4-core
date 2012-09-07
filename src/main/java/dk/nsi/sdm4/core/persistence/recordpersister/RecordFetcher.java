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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import java.sql.SQLException;

import static dk.nsi.sdm4.core.persistence.recordpersister.FieldSpecification.RecordFieldType.*;

public class RecordFetcher {
	@Autowired
	private JdbcTemplate jdbcTemplate;

	public Record fetchCurrent(String key, RecordSpecification recordSpecification, String lookupColumn) {
		Record record = null;
		SqlRowSet resultSet = jdbcTemplate.queryForRowSet(String.format("SELECT * FROM %s WHERE %s = ? AND validTo IS NULL", recordSpecification.getTable(), lookupColumn), key);
		if (resultSet.next()) {
			record = createRecordFromResultSet(recordSpecification, resultSet);
		} // else we will return null, which is what we want

		if (!resultSet.isLast()) {
			throw new IncorrectResultSizeDataAccessException("More than one record with validTo NULL was found", 1);
		}

		return record;
	}

	public Record fetchCurrent(String key, RecordSpecification recordSpecification) throws SQLException {
		return fetchCurrent(key, recordSpecification, recordSpecification.getKeyColumn());
	}


    // Indtil 7/7-2012 var der her en fetchSince-metode. Den er fjernet, da dens semantik var uklar, og ingen parsere
	// brugte den. Den antog at databasetabellen for en Record havde en kolonne PID, hvilket kun gælder for én parser
	// blandt de nuværende.
	// Når/hvis NSP-modulerne skal over på det nye core, må vi genindføre metoden med en mere veldefineret semantik.

    private Record createRecordFromResultSet(RecordSpecification recordSpecification, SqlRowSet resultSet) {
		RecordBuilder builder = new RecordBuilder(recordSpecification);

		for (FieldSpecification fieldSpec : recordSpecification.getFieldSpecs()) {
			if (fieldSpec.persistField) {
				String fieldName = fieldSpec.name;

				if (fieldSpec.type == NUMERICAL) {
					builder.field(fieldName, resultSet.getLong(fieldName));
				} else if (fieldSpec.type == DECIMAL10_3) {
					builder.field(fieldName, resultSet.getDouble(fieldName));
				} else if (fieldSpec.type == ALPHANUMERICAL) {
					builder.field(fieldName, resultSet.getString(fieldName));
				} else {
					throw new AssertionError("Invalid field specifier " + fieldSpec.type + " used");
				}
			}
		}

		return builder.build();
	}
}
