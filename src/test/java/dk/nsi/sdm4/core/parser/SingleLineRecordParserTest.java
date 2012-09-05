package dk.nsi.sdm4.core.parser;

import dk.nsi.sdm4.core.persistence.recordpersister.Record;
import dk.nsi.sdm4.core.persistence.recordpersister.RecordSpecification;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SingleLineRecordParserTest {
	@Test(expected = IllegalArgumentException.class)
	public void complainsWhenLineIsTooLong() {
		SingleLineRecordParser parser = makeParser(RecordSpecification.field("testField", 2));
		parser.parseLine("123");
	}

	@Test(expected = IllegalArgumentException.class)
	public void complainsWhenLineIsTooShort() {
		SingleLineRecordParser parser = makeParser(RecordSpecification.field("testField", 2));
		parser.parseLine("1");
	}

	@Test
	public void buildsARecordWithOneField() {
		SingleLineRecordParser parser = makeParser(RecordSpecification.field("testField", 12));
		Record record = parser.parseLine("testFieldVal");
		assertEquals("testFieldVal", record.get("testField"));
	}

	@Test
	public void buildsARecordWithAnIntegerField() {
		SingleLineRecordParser parser = makeParser(new RecordSpecification.FieldSpecification("testField", RecordSpecification.RecordFieldType.NUMERICAL, 2, true));
		Record record = parser.parseLine("31");
		assertEquals(31, record.get("testField"));
	}

	@Test
	public void parsesSecondFieldOfRecord() {
		SingleLineRecordParser parser = makeParser(RecordSpecification.field("testField", 5), RecordSpecification.field("testField2", 12));
		Record record = parser.parseLine("12345testFieldVal");
		assertEquals("testFieldVal", record.get("testField2"));
	}

	private SingleLineRecordParser makeParser(RecordSpecification.FieldSpecification... fields) {
		return new SingleLineRecordParser(RecordSpecification.createSpecification("testTable", "testKeyColumn", fields));
	}
}