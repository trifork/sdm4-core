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

import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;
import org.springframework.transaction.annotation.Transactional;

import java.sql.SQLException;
import java.sql.Timestamp;

import static dk.nsi.sdm4.core.persistence.recordpersister.FieldSpecification.field;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

@RunWith(SpringJUnit4ClassRunner.class)
@Transactional
@ContextConfiguration(loader = AnnotationConfigContextLoader.class)
public class RecordPersisterTest
{
	private RecordSpecification decimalRecordSpec;

	@Configuration
		@PropertySource("classpath:test.properties")
		@Import(RecordPersisterTestDatasourceConfiguration.class)
		static class ContextConfiguration {
			@Bean
			public RecordFetcher recordFetcher(Instant transactionTime) {
				return new RecordFetcher(transactionTime);
			}

			@Bean
			public RecordPersister persister(Instant transactionTime) {
				return new RecordPersister(transactionTime);
			}

            @Bean
            public Instant transactionTime() {
                return new DateTime(2011, 5, 29, 0, 0, 0).toInstant();
            }
		}

    private RecordSpecification recordSpecification;

	@Autowired
    private RecordFetcher fetcher;

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Autowired
	private RecordPersister persister;

    @Before
    public void setUp() throws SQLException
    {
        recordSpecification = RecordSpecification.createSpecification("SikredeTest", "Moo",
                field("Foo", 2, false).numerical(),
                field("Moo", 5, false)
        );

	    decimalRecordSpec = RecordSpecification.createSpecification("SikredeTestDecimal", "Moo",
			    field("Foo", 2, false).decimal10_3(),
			    field("Moo", 5, false)
	    );

	    createSikredeFieldsTableOnDatabase(recordSpecification);
    }

    @Test
    public void testAddingTheSameRecordTwiceButWithNeverTimestamp() throws SQLException 
    {
        RecordBuilder builder = new RecordBuilder(recordSpecification);
        Record recordA = builder.field("Foo", 42).field("Moo", "Bar").build();
        Record recordB = builder.field("Foo", 23).field("Moo", "Bar").build();

        DateTime theYear2000 = new DateTime(2000, 1, 1, 0, 0);
        RecordPersister persisterIn2000 = new RecordPersister(theYear2000.toInstant());
	    persisterIn2000.jdbcTemplate = jdbcTemplate;
        persisterIn2000.persist(recordA, recordSpecification);

        DateTime theYear2010 = theYear2000.plusYears(10);
        RecordPersister persisterIn2010 = new RecordPersister(theYear2010.toInstant());
	    persisterIn2010.jdbcTemplate = jdbcTemplate;
	    persisterIn2010.persist(recordB, recordSpecification);

	    Long fooFromDb = jdbcTemplate.queryForLong("SELECT FOO FROM " + recordSpecification.getTable() + " WHERE ValidFrom > ?", theYear2010.minusDays(1).toDate());
	    assertThat(fooFromDb, is(23L));
    }

    @Test
    public void testAddingTwoDifferentRecordsDontEffectEachOther() throws SQLException 
    {
        Record recordA = new RecordBuilder(recordSpecification).field("Foo", 42).field("Moo", "Far").build();
        Record recordB = new RecordBuilder(recordSpecification).field("Foo", 23).field("Moo", "Bar").build();
        
        persister.persist(recordA, recordSpecification);
        persister.persist(recordB, recordSpecification);

	    assertEquals(2, jdbcTemplate.queryForInt("SELECT COUNT(*) FROM " + recordSpecification.getTable() + " WHERE validTo IS NULL"));

        Record recordAExpected = fetcher.fetchCurrent("Far", recordSpecification);
        Record recordBExpected = fetcher.fetchCurrent("Bar", recordSpecification);

        assertEquals(recordB, recordBExpected);
        assertEquals(recordA, recordAExpected);
    }

	@Test
	public void testSaveDecimal10_3() throws SQLException
	{
		Record recordA = new RecordBuilder(decimalRecordSpec).field("Foo", 42.2).field("Moo", "Far").build();

		persister.persist(recordA, decimalRecordSpec);

		assertEquals(new Double(42.2), jdbcTemplate.queryForObject("SELECT Foo FROM " + decimalRecordSpec.getTable(), Double.class));
	}

    @Test
    public void testTerminateRecord() throws SQLException {
        Record record = new RecordBuilder(recordSpecification).field("Foo", 42).field("Moo", "Far").build();
        persister.persist(record, recordSpecification);

        Record recordFetced = fetcher.fetchCurrent("Far", recordSpecification);
        // Make sure ValidTo is set to null
        Timestamp timestamp = extractValidToFromRecord(record, recordSpecification);
        assertNull("ValidTo should be null at this point", timestamp);

        persister.terminate(recordFetced, recordSpecification);
        timestamp = extractValidToFromRecord(record, recordSpecification);
        assertNotNull("ValidTo should be set, since the record has been terminated", timestamp);
    }

    @Test
    public void testTerminateRecordAt() throws SQLException {
        Record record = new RecordBuilder(recordSpecification).field("Foo", 42).field("Moo", "Far").build();
        persister.persist(record, recordSpecification);

        Record recordFetced = fetcher.fetchCurrent("Far", recordSpecification);
        DateTime futureTime = new DateTime(2050, 1, 1, 0, 0, 0);
        persister.terminateAt(recordFetced, recordSpecification, futureTime.toDate());

        recordFetced = fetcher.fetchCurrent("Far", recordSpecification);
        Timestamp futureStamp = extractValidToFromRecord(recordFetced, recordSpecification);
        assertTrue(futureStamp.compareTo(futureTime.toDate()) == 0);

        DateTime closerFutureTime = new DateTime(2040, 1, 1, 0, 0, 0);
        persister.terminateAt(recordFetced, recordSpecification, closerFutureTime.toDate());
        recordFetced = fetcher.fetchCurrent("Far", recordSpecification);
        Timestamp closerFutureStamp = extractValidToFromRecord(recordFetced, recordSpecification);
        assertTrue(closerFutureStamp.compareTo(closerFutureTime.toDate()) == 0);
    }

    private Timestamp extractValidToFromRecord(Record record, RecordSpecification specification) {
        String keyColumn = specification.getKeyColumn();
        String sql = "SELECT ValidTo FROM " + specification.getTable() + " WHERE " + keyColumn + "=?";
        Object keyValue = record.get(keyColumn);
        return jdbcTemplate.queryForObject(sql, Timestamp.class, (String)keyValue);
    }

	private void createSikredeFieldsTableOnDatabase(RecordSpecification recordSpecification) throws SQLException
    {
        jdbcTemplate.update("DROP TABLE IF EXISTS " + recordSpecification.getTable());
	    jdbcTemplate.update(RecordMySQLTableGenerator.createSqlSchema(recordSpecification));

	    jdbcTemplate.update("DROP TABLE IF EXISTS " + decimalRecordSpec.getTable());
	    jdbcTemplate.update(RecordMySQLTableGenerator.createSqlSchema(decimalRecordSpec));
    }
}
