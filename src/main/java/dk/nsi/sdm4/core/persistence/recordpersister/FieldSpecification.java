package dk.nsi.sdm4.core.persistence.recordpersister;

public class FieldSpecification {
	public final String name;
	public final RecordFieldType type;
	public final int length;
	public final boolean persistField;


	public static enum RecordFieldType {
		ALPHANUMERICAL,
		NUMERICAL
	}

	public FieldSpecification(String name, RecordFieldType type, int length, boolean persistField) {
		this.name = name;
		this.type = type;
		this.length = length;
		this.persistField = persistField;
	}

	/**
	 * Returns a copy of the field with a numerical entry
	 */
	public FieldSpecification numerical() {
		return new FieldSpecification(name, RecordFieldType.NUMERICAL, length, persistField);
	}

	/**
	 * Returns a copy of the field that will not be persisted
	 */
	public FieldSpecification doNotPersist() {
		return new FieldSpecification(name, type, length, false);
	}


	/**
	 * Creates an alphanumerical field that will be persisted
	 */
	public static FieldSpecification field(String name, int length) {
		return new FieldSpecification(name, RecordFieldType.ALPHANUMERICAL, length, true);
	}
}
