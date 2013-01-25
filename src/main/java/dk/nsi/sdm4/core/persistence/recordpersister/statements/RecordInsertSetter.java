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
package dk.nsi.sdm4.core.persistence.recordpersister.statements;

import com.google.common.collect.Lists;
import dk.nsi.sdm4.core.persistence.recordpersister.FieldSpecification;
import dk.nsi.sdm4.core.persistence.recordpersister.Record;
import dk.nsi.sdm4.core.persistence.recordpersister.RecordSpecification;
import org.apache.commons.lang.StringUtils;
import org.joda.time.Instant;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;

public class RecordInsertSetter extends RecordSetter {
    Instant transactionTime;
    private final Instant validFrom;
    private final Instant validTo;

    public RecordInsertSetter(Record record, RecordSpecification recordSpec, Instant transactionTime,
                              Instant validFrom, Instant validTo) {
        super(record, recordSpec);
        this.transactionTime = transactionTime;
        this.validFrom = validFrom;
        this.validTo = validTo;
    }

    @Override
    public void setValues(PreparedStatement preparedStatement) throws SQLException {
        int index = 1;

        for (FieldSpecification fieldSpecification : recordSpec.getFieldSpecs()) {
            if (fieldSpecification.persistField) {
                setStatementFieldAtIndex(preparedStatement, fieldSpecification, index++);
            }
        }
        // Set ValidFrom
        if (validFrom == null) {
            // ValidFrom are not suppose to be null update with transactiontime if it is
            preparedStatement.setTimestamp(index++, new Timestamp(transactionTime.getMillis()));
        } else {
            preparedStatement.setTimestamp(index++, new Timestamp(validFrom.getMillis()));
        }
        // Set ValidTo
        if (validTo == null) {
            preparedStatement.setNull(index++, Types.TIMESTAMP);
        } else {
            preparedStatement.setTimestamp(index++, new Timestamp(validTo.getMillis()));
        }
        // Set ModifiedDate to transaction time always
        preparedStatement.setTimestamp(index++, new Timestamp(transactionTime.getMillis()));
    }

    public static String createInsertStatementSql(RecordSpecification specification) {
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

        fieldNames.add("ValidTo");
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
