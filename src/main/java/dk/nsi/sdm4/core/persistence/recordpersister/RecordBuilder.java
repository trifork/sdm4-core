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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static dk.nsi.sdm4.core.persistence.recordpersister.FieldSpecification.RecordFieldType.ALPHANUMERICAL;
import static dk.nsi.sdm4.core.persistence.recordpersister.FieldSpecification.RecordFieldType.DECIMAL10_3;
import static dk.nsi.sdm4.core.persistence.recordpersister.FieldSpecification.RecordFieldType.NUMERICAL;

public class RecordBuilder {
	private RecordSpecification recordSpecification;
	private Record record;

	public RecordBuilder(RecordSpecification recordSpecification) {
		this.recordSpecification = recordSpecification;
		record = new Record();
	}

	public RecordBuilder field(String fieldName, long value) {
		return field(fieldName, value, NUMERICAL);
	}

	public RecordBuilder field(String fieldName, double value) {
		return field(fieldName, value, DECIMAL10_3);
	}

	public RecordBuilder field(String fieldName, String value) {
		return field(fieldName, value, ALPHANUMERICAL);
	}

	private RecordBuilder field(String fieldName, Object value, FieldSpecification.RecordFieldType recordFieldType) {
		checkNotNull(fieldName);
		checkArgument(value == null || getFieldType(fieldName) == recordFieldType, "Field " + fieldName + " is not " + recordFieldType);

		record = record.put(fieldName, value);

		return this;
	}

	public Record build() {
		if (recordSpecification.conformsToSpecifications(record)) {
			return record;
		} else {
			throw new IllegalStateException("Mandatory fields not set");
		}
	}

	public Record addDummyFieldsAndBuild() {
		for (FieldSpecification fieldSpecification : recordSpecification.getFieldSpecs()) {
			if (!record.containsKey(fieldSpecification.name)) {
				if (fieldSpecification.type == ALPHANUMERICAL) {
					record = record.put(fieldSpecification.name, "D");
				} else if (fieldSpecification.type == NUMERICAL) {
					record = record.put(fieldSpecification.name, 0);
				} else {
					throw new AssertionError("");
				}
			}
		}
		return build();
	}

	private FieldSpecification.RecordFieldType getFieldType(String fieldName) {
		for (FieldSpecification fieldSpecification : recordSpecification.getFieldSpecs()) {
			if (fieldSpecification.name.equals(fieldName)) {
				return fieldSpecification.type;
			}
		}

		return null;
	}
}
