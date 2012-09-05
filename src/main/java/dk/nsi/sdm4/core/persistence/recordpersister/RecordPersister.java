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
import java.util.List;

import static dk.nsi.sdm4.core.persistence.recordpersister.FieldSpecification.RecordFieldType.ALPHANUMERICAL;

public class RecordPersister {
	private Instant transactionTime;

	@Autowired
	protected JdbcTemplate jdbcTemplate;

	@Autowired
	public RecordPersister(Instant transactionTime) {
		this.transactionTime = transactionTime;
	}


	public void persist(Record record, RecordSpecification specification) throws SQLException {
		Preconditions.checkNotNull(record);
		Preconditions.checkNotNull(specification);
		Preconditions.checkArgument(specification.conformsToSpecifications(record));

		// Data dumps from Yderregister and "Sikrede" contains history information and are therefore handled
		// differently from all other register types. The data contained in each input record is appended directly
		// to the database instead of updating existing records.
		jdbcTemplate.update(createInsertStatementSql(specification), new InsertStatementSetter(record, specification));
	}

	private class InsertStatementSetter implements PreparedStatementSetter {
		private Record record;
		private RecordSpecification recordSpec;

		public InsertStatementSetter(Record record, RecordSpecification recordSpec) {
			this.record = record;
			this.recordSpec = recordSpec;
			Preconditions.checkArgument(recordSpec.conformsToSpecifications(record), "The record does not conform to it's spec.");
		}

		@Override
		public void setValues(PreparedStatement preparedStatement) throws SQLException {
			int index = 1;

			for (FieldSpecification fieldSpecification : recordSpec.getFieldSpecs()) {
				if (fieldSpecification.persistField) {
					Object fieldVal = record.get(fieldSpecification.name);
					if (fieldSpecification.type == ALPHANUMERICAL) {
						preparedStatement.setString(index, (String) fieldVal);
					} else if (fieldSpecification.type == FieldSpecification.RecordFieldType.NUMERICAL) {
						preparedStatement.setLong(index, (Long) fieldVal);
					} else if (fieldSpecification.type == FieldSpecification.RecordFieldType.DECIMAL10_3) {
						preparedStatement.setDouble(index, (Double) fieldVal);
					} else {
						throw new AssertionError("RecordType was not set correctly in the specification");
					}

					index++;
				}
			}

			preparedStatement.setTimestamp(index++, new Timestamp(transactionTime.getMillis()));
			preparedStatement.setTimestamp(index++, new Timestamp(transactionTime.getMillis()));
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
}
