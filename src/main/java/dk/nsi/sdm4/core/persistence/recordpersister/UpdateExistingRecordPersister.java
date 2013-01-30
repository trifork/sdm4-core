package dk.nsi.sdm4.core.persistence.recordpersister;

import org.joda.time.Instant;
import org.springframework.beans.factory.annotation.Autowired;

import java.sql.SQLException;

/**
 *
 */
public class UpdateExistingRecordPersister extends RecordPersister {

    @Autowired
    private RecordFetcher fetcher;

    public UpdateExistingRecordPersister() {
    }

    public UpdateExistingRecordPersister(Instant transactionTime) {
        super(transactionTime);
    }

    /**
     * This method will persist the record parsed from file into the database, using the following algorithm:
     * <ol>
     *     <li>Attempt to find an existing current db-record with the same key used in the file-version.
     *     <ul>
     *         <li>If such a record exists:
     *         <ol>
     *             <li>Compare the file-based record with the database-based record</li>
     *             <li>Identical: Don't persist - we already have the data</li>
     *             <li>Not identical: Update the existing db-records ValidTo=now and create a new record - effectively replacing the existing db-record as the current record</li>
     *         </ol>
     *         </li>
     *         <li>If no record exists: Create a new record</li>
     *     </ul>
     *     </li>
     * </ol>Before doing so it will check the database to see if we already
     * have a record with the same key. If the record exists in the database, we move on to check if the field values in
     * the two records are the same - if they are identical we skip the persist request since the data in the record
     * and in the database-record are
     *
     * @param record
     * @param specification
     * @throws SQLException
     */
    @Override
    public void persist(Record record, RecordSpecification specification) throws SQLException {
        RecordWithMetadata existingRecord = fetcher.fetchCurrentWithMeta(String.valueOf(record.get(specification.getKeyColumn())), specification);
        if (existingRecord == null) {
            super.persist(record, specification);
        } else {
            if (!existingRecord.getRecord().equals(record)) {
                existingRecord.setValidTo(getTransactionTime());
                update(existingRecord, specification); //update existing record
                super.persist(record, specification); //create the new record
            } // else - ignore persist record request as existing record is identical
        }
    }
}
