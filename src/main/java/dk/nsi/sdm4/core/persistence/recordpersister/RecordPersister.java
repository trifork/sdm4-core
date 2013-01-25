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

import com.google.common.base.Preconditions;
import dk.nsi.sdm4.core.persistence.recordpersister.statements.RecordInsertSetter;
import dk.nsi.sdm4.core.persistence.recordpersister.statements.RecordUpdateSetter;
import org.joda.time.Instant;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.SQLException;

/**
 * Handles persisting of Records
 */
public class RecordPersister {
	private Instant transactionTime;

	@Autowired
	protected JdbcTemplate jdbcTemplate;

	@Autowired
	public RecordPersister(Instant transactionTime) {
		this.transactionTime = transactionTime;
	}

	public RecordPersister() {
		resetTransactionTime();
	}

	public void resetTransactionTime() {
		this.transactionTime = new Instant();
	}

	public Instant getTransactionTime() {
		return transactionTime;
	}

    /**
     * Persist a record to database, automatically handles setting ModifiedDate and ValidFrom
     * @param record
     * @param specification
     * @throws SQLException
     */
	public void persist(Record record, RecordSpecification specification) throws SQLException {
		Preconditions.checkNotNull(record);
		Preconditions.checkNotNull(specification);
		Preconditions.checkArgument(specification.conformsToSpecifications(record));

		// Data dumps from Yderregister and "Sikrede" contains history information and are therefore handled
		// differently from all other register types. The data contained in each input record is appended directly
		// to the database instead of updating existing records.
		jdbcTemplate.update(RecordInsertSetter.createInsertStatementSql(specification),
                new RecordInsertSetter(record, specification, transactionTime, null, null));
	}

    /**
     * Persist a records to database, ValidTo and ValidFrom are set from metadata.
     * ModifiedDate is always set to transaction time.
     * @param recordWithMeta
     * @param specification
     * @return a new recordmeta with PID and ModifiedDate updated.
     */
    public RecordWithMetadata persist(RecordWithMetadata recordWithMeta, RecordSpecification specification)
            throws SQLException {
        Preconditions.checkNotNull(recordWithMeta);
        Preconditions.checkNotNull(recordWithMeta.getRecord());
        Preconditions.checkNotNull(specification);
        Preconditions.checkArgument(specification.conformsToSpecifications(recordWithMeta.getRecord()));

        jdbcTemplate.update(RecordInsertSetter.createInsertStatementSql(specification),
                new RecordInsertSetter(recordWithMeta.getRecord(), specification, transactionTime,
                        recordWithMeta.getValidFrom(), recordWithMeta.getValidTo()));
        Long pid = jdbcTemplate.queryForLong( "select last_insert_id()" );
         return new RecordWithMetadata(recordWithMeta.getValidFrom(), recordWithMeta.getValidTo(), transactionTime,
                 pid, recordWithMeta.getRecord());
    }

    /**
     * Updates a record including ValidTo and ValidFrom.
     * ModifiedDate is always set to transaction time regardsless of meta
     * @param recordWithMeta
     * @param specification
     */
    public void update(RecordWithMetadata recordWithMeta, RecordSpecification specification) {
        Preconditions.checkNotNull(recordWithMeta);
        Preconditions.checkNotNull(recordWithMeta.getRecord());
        Preconditions.checkNotNull(specification);
        Preconditions.checkArgument(specification.conformsToSpecifications(recordWithMeta.getRecord()));

        jdbcTemplate.update(RecordUpdateSetter.createUpdateStatementSql(specification),
                new RecordUpdateSetter(recordWithMeta, specification, transactionTime));
    }

}
