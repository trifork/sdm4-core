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

import org.apache.log4j.Logger;
import org.joda.time.Instant;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import static dk.nsi.sdm4.core.persistence.recordpersister.FieldSpecification.RecordFieldType.*;

public class RecordFetcher {
	private static final Logger log = Logger.getLogger(RecordFetcher.class);
    private Instant transactionTime;

	@Autowired
	private JdbcTemplate jdbcTemplate;

    @Autowired
    public RecordFetcher(Instant transactionTime) {
        this.transactionTime = transactionTime;
    }

    public RecordFetcher() {
        resetTransactionTime();
    }

    public void resetTransactionTime() {
        this.transactionTime = new Instant();
    }

	public Record fetchCurrent(String key, RecordSpecification recordSpecification,
                               String lookupColumn) {
        // KPN ValidTo does not have to be null as long as it is in the future it is ok
        //             futhermore ValidFrom should be checked here aswell
		String sql = String.format("SELECT * FROM %s WHERE %s = ? AND (validTo IS NULL OR validTo > ?) AND (validFrom <= ?)" ,
                recordSpecification.getTable(), lookupColumn);

		Record record;
		try {
            Timestamp validAtStamp = new Timestamp(transactionTime.getMillis());
            record = jdbcTemplate.queryForObject(sql,
                    new RecordRowsetMapper(recordSpecification), key, validAtStamp, validAtStamp);
		} catch (EmptyResultDataAccessException e) {
			// når der ingen åben record er, forventer klienter bare at vi returnerer null
			return null;
		}

		if (log.isDebugEnabled()) {
			log.debug("Fetch current for " + recordSpecification.getTable() + " " + lookupColumn + "=" + key + " returning " + record);
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

    // TODO: KPN Not sure I agree fetchsince is used by the batch copy service, so it should be recreated if the service
    // should run on sdm4 at some point...

	private class RecordRowsetMapper implements RowMapper<Record> {
		private RecordSpecification recordSpecification;

		private RecordRowsetMapper(RecordSpecification recordSpecification) {
			this.recordSpecification = recordSpecification;
		}

		@Override
		public Record mapRow(ResultSet resultSet, int rowNum) throws SQLException {
			RecordBuilder builder = new RecordBuilder(recordSpecification);

			for (FieldSpecification fieldSpec : recordSpecification.getFieldSpecs()) {
				if (log.isDebugEnabled()) {
					log.debug("Processing field " + fieldSpec.name);
				}

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
}
