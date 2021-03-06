package dk.nsi.sdm4.core.persistence.recordpersister;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;

public class FieldSpecification {
	public final String name;
	public final RecordFieldType type;
	public final int length;
	public final boolean persistField;
	public final boolean ignored;
	public final boolean optional;
    public final boolean calculatedField;

	public static enum RecordFieldType {
		ALPHANUMERICAL,
		NUMERICAL,
		DECIMAL10_3,
        DATETIME
	}

	private FieldSpecification(String name, RecordFieldType type, int length, boolean persistField, boolean ignored,
                               boolean optional, boolean calculatedField) {
		this.name = name;
		this.type = type;
		this.length = length;
		this.persistField = persistField;
		this.ignored = ignored;
		this.optional = optional;
        this.calculatedField = calculatedField;
	}

	/**
	 * Returns a copy of the field with a numerical entry
	 */
	public FieldSpecification numerical() {
		return new FieldSpecification(
                name, RecordFieldType.NUMERICAL, length, persistField, ignored, optional, calculatedField);
	}

    public FieldSpecification datetime() {
        return new FieldSpecification(
                name, RecordFieldType.DATETIME, length, persistField, ignored, optional, calculatedField);
    }

	/**
	 * Returns a copy of the field that will not be persisted
	 */
	public FieldSpecification doNotPersist() {
		return new FieldSpecification(name, type, length, false, ignored, false, calculatedField);
	}

    public FieldSpecification calculated() {
        return new FieldSpecification(name, type, length, persistField, ignored, persistField, true);
    }

	/**
	 * Returns a copy of the field with a decimal(10,3) entry
	 */
	public FieldSpecification decimal10_3() {
		return new FieldSpecification(
                name, RecordFieldType.DECIMAL10_3, length, persistField, ignored, optional, calculatedField);
	}

	/**
	 * Returns a copy of the field that will be ignored by parsers, making the field not end up in the produced records.
	 * Used for areas of fixed width lines which are by specification blank
	 */
	public FieldSpecification ignored() {
		return new FieldSpecification(name, RecordFieldType.DECIMAL10_3, length, false, true, false, calculatedField);
	}

	/**
	 * Creates an alphanumerical field that will be persisted
	 */
	public static FieldSpecification field(String name, int length, boolean optional) {
		return new FieldSpecification(name, RecordFieldType.ALPHANUMERICAL, length, true, false, optional, false);
	}

	@Override
	public String toString() {
		return new ReflectionToStringBuilder(this).toString();
	}

	public boolean isOptional() {
		return optional;
	}

}
