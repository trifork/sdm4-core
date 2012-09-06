package dk.nsi.sdm4.core.parser;

import dk.nsi.sdm4.core.persistence.recordpersister.FieldSpecification;
import dk.nsi.sdm4.core.persistence.recordpersister.Record;
import dk.nsi.sdm4.core.persistence.recordpersister.RecordSpecification;
import org.junit.Test;

import static dk.nsi.sdm4.core.persistence.recordpersister.FieldSpecification.RecordFieldType.NUMERICAL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class SingleLineRecordParserTest {
	@Test(expected = IllegalArgumentException.class)
	public void complainsWhenLineIsTooLong() {
		SingleLineRecordParser parser = makeParser(FieldSpecification.field("testField", 2));
		parser.parseLine("123");
	}

	@Test(expected = IllegalArgumentException.class)
	public void complainsWhenLineIsTooShort() {
		SingleLineRecordParser parser = makeParser(FieldSpecification.field("testField", 2));
		parser.parseLine("1");
	}

	@Test
	public void buildsARecordWithOneField() {
		SingleLineRecordParser parser = makeParser(FieldSpecification.field("testField", 12));
		Record record = parser.parseLine("testFieldVal");
		assertEquals("testFieldVal", record.get("testField"));
	}

	@Test
	public void buildsARecordWithNumericField() {
		SingleLineRecordParser parser = makeParser(FieldSpecification.field("testField", 2).numerical());
		Record record = parser.parseLine("31");
		assertEquals(31L, record.get("testField"));
	}

	@Test
	public void parsesSecondFieldOfRecord() {
		SingleLineRecordParser parser = makeParser(FieldSpecification.field("testField", 5), FieldSpecification.field("testField2", 12));
		Record record = parser.parseLine("12345testFieldVal");
		assertEquals("testFieldVal", record.get("testField2"));
	}

	@Test
	public void ignoresIgnoredField() {
		SingleLineRecordParser parser = makeParser(FieldSpecification.field("testField", 5).ignored());
		Record record = parser.parseLine("12345");
		assertNull(record.get("testField"));
	}

	private SingleLineRecordParser makeParser(FieldSpecification... fields) {
		return new SingleLineRecordParser(RecordSpecification.createSpecification("testTable", "testKeyColumn", fields));
	}
}
