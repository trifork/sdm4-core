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
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static dk.nsi.sdm4.core.persistence.recordpersister.FieldSpecification.RecordFieldType.*;

public class RecordSpecification {
	private final String table;
	private final String keyColumn;

	public String getTable() {
		return table;
	}

	public String getKeyColumn() {
		return keyColumn;
	}

	private List<FieldSpecification> fields;

	private RecordSpecification(String table, String keyColumn) {
		this.table = table;
		this.keyColumn = keyColumn;

		fields = new ArrayList<FieldSpecification>();
	}

	public static RecordSpecification createSpecification(String tableName, String keyColumnName, FieldSpecification... fieldSpecifications) {
		RecordSpecification recordSpecification = new RecordSpecification(tableName, keyColumnName);

		recordSpecification.fields = Arrays.asList(fieldSpecifications);

		return recordSpecification;
	}

	public Iterable<FieldSpecification> getFieldSpecs() {
		return ImmutableList.copyOf(fields);
	}

	public int acceptedTotalLineLength() {
		int totalLength = 0;
		for (FieldSpecification fieldSpecification : fields) {
            if (!fieldSpecification.calculatedField) {
			    totalLength += fieldSpecification.length;
            }
		}
		return totalLength;
	}

	public boolean conformsToSpecifications(Record record) {
		Preconditions.checkNotNull(record, "record");

		for (FieldSpecification fieldsSpecification : fields) {
			if (fieldsSpecification.persistField) {
				if (!record.containsKey(fieldsSpecification.name) && !fieldsSpecification.isOptional()) {
					return false;
				} else {
					Object value = record.get(fieldsSpecification.name);

					if (fieldsSpecification.type == NUMERICAL || fieldsSpecification.type == DECIMAL10_3) {
						if(value == null && fieldsSpecification.isOptional()) {
							// ok, field is optional
						} else if (value != null && !(value instanceof Number)) {
							return false;
						}
					} else if (fieldsSpecification.type == ALPHANUMERICAL) {
						if(value == null && fieldsSpecification.isOptional()) {
							// ok, field is optional
						} else {
							if (value != null && !(value instanceof String)) {
								return false;
							} else if (value != null) {
								String valueAsString = String.valueOf(value);

								if (valueAsString.length() > fieldsSpecification.length) {
									return false;
								}
							}
						}
                    } else if (fieldsSpecification.type == DATETIME) {
                        if(value == null && fieldsSpecification.isOptional()) {
                            // ok, field is optional
                        } else if (value != null && !(value instanceof java.util.Date)) {
                            return false;
                        }
					} else {
						throw new AssertionError("Field specification is in illegal state. Type must be set.");
					}
				}
			}
		}

		return true;
	}

	@Override
	public String toString() {
		return new ReflectionToStringBuilder(this).toString();
	}
}
