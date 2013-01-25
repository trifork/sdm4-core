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

import com.google.common.base.Preconditions;
import dk.nsi.sdm4.core.persistence.recordpersister.FieldSpecification;
import dk.nsi.sdm4.core.persistence.recordpersister.Record;
import dk.nsi.sdm4.core.persistence.recordpersister.RecordSpecification;
import org.springframework.jdbc.core.PreparedStatementSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static dk.nsi.sdm4.core.persistence.recordpersister.FieldSpecification.RecordFieldType.ALPHANUMERICAL;

/**
 * Base class for PreparedStatementSetters that work on Record´s
 */
public abstract class RecordSetter implements PreparedStatementSetter {
    protected Record record;
    protected RecordSpecification recordSpec;

    public RecordSetter(Record record, RecordSpecification recordSpec) {
        this.record = record;
        this.recordSpec = recordSpec;
        Preconditions.checkArgument(recordSpec.conformsToSpecifications(record), "The record does not conform to it's spec.");
    }

    /**
     * Sets a field on a prepared statement according to its field specification
     * @param statement
     * @param fieldSpecification the field specification
     * @param index index to set in the preparedStatement
     * @throws java.sql.SQLException
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
