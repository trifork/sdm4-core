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

import dk.nsi.sdm4.core.persistence.recordpersister.FieldSpecification;
import dk.nsi.sdm4.core.persistence.recordpersister.RecordWithMetadata;
import dk.nsi.sdm4.core.persistence.recordpersister.RecordSpecification;
import org.joda.time.Instant;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Iterator;

/**
 * Creates a setter that can set values on an update statement
 * Input is RecordWithMetadata because a bare Recore does not contain PID need for updating
 */
public class RecordUpdateSetter extends RecordSetter {

    private RecordWithMetadata recordWithMeta;
    private final Instant transactionTime;

    public RecordUpdateSetter(RecordWithMetadata recordWithMeta, RecordSpecification recordSpec,
                              Instant transactionTime) {
        super(recordWithMeta.getRecord(), recordSpec);
        this.recordWithMeta = recordWithMeta;
        this.transactionTime = transactionTime;
    }

    @Override
    public void setValues(PreparedStatement preparedStatement) throws SQLException {
        int index = 1;

        // Set all record fields
        for (FieldSpecification fieldSpecification : recordSpec.getFieldSpecs()) {
            if (fieldSpecification.persistField) {
                setStatementFieldAtIndex(preparedStatement, fieldSpecification, index++);
            }
        }
        // Set ValidFrom
        if (recordWithMeta.getValidFrom() != null) {
            Timestamp validFrom = new Timestamp(recordWithMeta.getValidFrom().getMillis());
            preparedStatement.setTimestamp(index++, validFrom);
        } else {
            // ValidFrom cannot be null insert transaction time if it is
            preparedStatement.setTimestamp(index++, new Timestamp(transactionTime.getMillis()));
        }

        // Set ValidTo
        if (recordWithMeta.getValidTo() != null) {
            Timestamp validTo = new Timestamp(recordWithMeta.getValidTo().getMillis());
            preparedStatement.setTimestamp(index++, validTo);
        } else {
            preparedStatement.setNull(index++, Types.TIMESTAMP);
        }
        // Set modified to transaction time no matter what
        preparedStatement.setTimestamp(index++, new Timestamp(transactionTime.getMillis()));

        // Insert record pid
        preparedStatement.setLong(index++, recordWithMeta.getPid());
    }

    public static String createUpdateStatementSql(RecordSpecification specification) {
        StringBuilder builder = new StringBuilder();

        builder.append("UPDATE ").append(specification.getTable()).append(" SET ");

        Iterator<FieldSpecification> fieldIterator = specification.getFieldSpecs().iterator();
        while (fieldIterator.hasNext()) {
            FieldSpecification fieldSpecification = fieldIterator.next();
            if (fieldSpecification.persistField) {
                builder.append(fieldSpecification.name);
                builder.append("=?, ");
            }
        }
        builder.append("ValidFrom=?, ValidTo=?, ModifiedDate=? ");
        builder.append("WHERE PID=?");

        return builder.toString();
    }
}
