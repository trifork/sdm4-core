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
    public void testPersistWithMeta() throws SQLException {
        Record record = new RecordBuilder(decimalRecordSpec).field("Foo", 42.2).field("Moo", "Far").build();
        // public RecordWithMetadata(Instant validFrom, Instant validTo, Instant modifiedDate, Long pid, Record record) {
        Instant validFrom = new DateTime(2012, 1, 1, 1, 1, 1).toInstant();
        Instant validTo = new DateTime(2013, 1, 1, 1, 1, 1).toInstant();
        RecordWithMetadata recordWithMeta = new RecordWithMetadata(validFrom, validTo, null, null, record);
        recordWithMeta = persister.persist(recordWithMeta, decimalRecordSpec);
        assertNotNull(recordWithMeta.getPid());
        assertNotNull(recordWithMeta.getModifiedDate());
        assertEquals(recordWithMeta.getValidTo(), validTo);
        assertEquals(recordWithMeta.getValidFrom(), validFrom);
    }

    @Test
    public void testUpdateRecord() throws SQLException {
        Record recordA = new RecordBuilder(decimalRecordSpec).field("Foo", 42.2).field("Moo", "Far").build();
        persister.persist(recordA, decimalRecordSpec);
        assertEquals(new Double(42.2), jdbcTemplate.queryForObject("SELECT Foo FROM " + decimalRecordSpec.getTable(), Double.class));

        RecordWithMetadata recordWithMetadata = fetcher.fetchCurrentWithMeta("Far", decimalRecordSpec);
        recordWithMetadata.getRecord().put("Foo", 88.8);
        persister.update(recordWithMetadata, decimalRecordSpec);
        assertEquals(new Double(88.8), jdbcTemplate.queryForObject("SELECT Foo FROM " + decimalRecordSpec.getTable(), Double.class));
    }

	private void createSikredeFieldsTableOnDatabase(RecordSpecification recordSpecification) throws SQLException
    {
        jdbcTemplate.update("DROP TABLE IF EXISTS " + recordSpecification.getTable());
	    jdbcTemplate.update(RecordMySQLTableGenerator.createSqlSchema(recordSpecification));

	    jdbcTemplate.update("DROP TABLE IF EXISTS " + decimalRecordSpec.getTable());
	    jdbcTemplate.update(RecordMySQLTableGenerator.createSqlSchema(decimalRecordSpec));
    }
}
