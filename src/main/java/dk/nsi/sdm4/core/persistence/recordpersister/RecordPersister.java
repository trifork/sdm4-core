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
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.joda.time.Instant;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;

import static dk.nsi.sdm4.core.persistence.recordpersister.FieldSpecification.RecordFieldType.ALPHANUMERICAL;

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
		jdbcTemplate.update(createInsertStatementSql(specification),
                new InsertStatementSetter(record, specification));
	}

    /**
     * Terminate a records by setting ValidTo to current transaction time.
     * This applied only if record ValidTo IS NULL or ValidTo >= now
     * @param record the records to terminate
     * @param specification
     */
    public void terminate(Record record, RecordSpecification specification) {
        terminateAt(record, specification, new Date(transactionTime.getMillis()));
    }

    /**
     * Terminate a records at a specefic time in the future
     * This applied only if record ValidTo IS NULL or ValidTo >= when
     * @param record the records to terminate
     * @param specification
     * @param when at what time should the record be terminated
     */
    public void terminateAt(Record record, RecordSpecification specification, Date when) {
        Preconditions.checkNotNull(record);
        Preconditions.checkNotNull(specification);
        Preconditions.checkArgument(specification.conformsToSpecifications(record));

        jdbcTemplate.update(createTerminateRecordsSql(specification),
                new TerminateStatementSetter(record, specification, new Timestamp(when.getTime())));
    }

    /**
     * Base class for PreparedStatementSetters that work on Record´s
     */
    private abstract class RecordStatementSetter implements PreparedStatementSetter {
        protected Record record;
        protected RecordSpecification recordSpec;

        public RecordStatementSetter(Record record, RecordSpecification recordSpec) {
            this.record = record;
            this.recordSpec = recordSpec;
            Preconditions.checkArgument(recordSpec.conformsToSpecifications(record), "The record does not conform to it's spec.");
        }

        /**
         * Sets a field on a prepared statement according to its field specification
         * @param statement
         * @param fieldSpecification the field specification
         * @param index index to set in the preparedStatement
         * @throws SQLException
         */
        protected void setStatementFieldAtIndex(PreparedStatement statement, FieldSpecification fieldSpecification,
                                                int index) throws SQLException {
            Object fieldVal = record.get(fieldSpecification.name);
            if (fieldSpecification.type == ALPHANUMERICAL) {
                statement.setString(index, (String) fieldVal);
            } else if (fieldSpecification.type == FieldSpecification.RecordFieldType.NUMERICAL) {
                statement.setLong(index, (Long) fieldVal);
            } else if (fieldSpecification.type == FieldSpecification.RecordFieldType.DECIMAL10_3) {
                statement.setDouble(index, (Double) fieldVal);
            } else {
                throw new AssertionError("RecordType was not set correctly in the specification");
            }
        }
    }

    /**
     * Populates a insert records statement
     */
	private class InsertStatementSetter extends RecordStatementSetter {

		public InsertStatementSetter(Record record, RecordSpecification recordSpec) {
            super(record, recordSpec);
		}

		@Override
		public void setValues(PreparedStatement preparedStatement) throws SQLException {
			int index = 1;

			for (FieldSpecification fieldSpecification : recordSpec.getFieldSpecs()) {
				if (fieldSpecification.persistField) {
                    setStatementFieldAtIndex(preparedStatement, fieldSpecification, index++);
				}
			}
			preparedStatement.setTimestamp(index++, new Timestamp(transactionTime.getMillis()));
			preparedStatement.setTimestamp(index++, new Timestamp(transactionTime.getMillis()));
		}
	}

    /**
     * Populates a update statement with ValidTo
     */
    private class TerminateStatementSetter extends RecordStatementSetter {

        private final Timestamp terminateAt;

        public TerminateStatementSetter(Record record, RecordSpecification recordSpec, Timestamp when) {
            super(record, recordSpec);
            terminateAt = when;
        }

        @Override
        public void setValues(PreparedStatement preparedStatement) throws SQLException {
            int index = 1;
            preparedStatement.setTimestamp(index++, terminateAt);
            preparedStatement.setTimestamp(index++, new Timestamp(transactionTime.getMillis()));

            String keyColumn = recordSpec.getKeyColumn();
            for (FieldSpecification fieldSpecification : recordSpec.getFieldSpecs()) {
                if (fieldSpecification.name.equals(keyColumn)) {
                    setStatementFieldAtIndex(preparedStatement, fieldSpecification, index++);
                    break;
                }
            }
            preparedStatement.setTimestamp(index++, terminateAt);
        }
    }


	public String createInsertStatementSql(RecordSpecification specification) {
		StringBuilder builder = new StringBuilder();

		builder.append("INSERT INTO ").append(specification.getTable()).append(" (");

		List<String> fieldNames = Lists.newArrayList();
		List<String> questionMarks = Lists.newArrayList();

		for (FieldSpecification fieldSpecification : specification.getFieldSpecs()) {
			if (fieldSpecification.persistField) {
				fieldNames.add(fieldSpecification.name);
				questionMarks.add("?");
			}
		}

		fieldNames.add("ValidFrom");
		questionMarks.add("?");

		fieldNames.add("ModifiedDate");
		questionMarks.add("?");

		builder.append(StringUtils.join(fieldNames, ", "));
		builder.append(") VALUES (");
		builder.append(StringUtils.join(questionMarks, ", "));
		builder.append(")");

		return builder.toString();
	}

    private String createTerminateRecordsSql(RecordSpecification specification) {
        StringBuilder builder = new StringBuilder();
        builder.append("UPDATE ").append(specification.getTable()).append(" ");
        builder.append("SET ValidTo=?, ModifiedDate=? ");
        builder.append("WHERE ").append(specification.getKeyColumn()).append("=? ");
        builder.append("AND (ValidTo IS NULL OR ValidTo>=?)");
        return builder.toString();
    }
}
