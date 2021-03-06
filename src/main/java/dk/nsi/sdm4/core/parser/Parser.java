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
package dk.nsi.sdm4.core.parser;

import java.io.File;

/**
 * A parser that imports files and stores the data in a database.
 * <p/>
 * Generally parsers should never log anything other than on DEBUG level.
 * If something goes wrong the parser must throw an exception and let the
 * caller do the logging.
 *
 * @author Thomas Børlum <thb@trifork.com>
 */
public interface Parser {

    public final static String SLA_RECORDS_PROCESSED_MAME = "processed_records";
    public final static String SLA_INPUT_NAME = "input";

	/**
	 * Processes a data set and persists the data.
	 * <p/>
	 * Processing consists of four steps:
	 * <p/>
	 * <ol>
	 * <li>Check that all required files are present.</li>
	 * <li>Check that the import sequence is in order.</li>
	 * <li>Parse the data set and persisting it accordingly.</li>
	 * <li>Update the version number, in the key value store.</li>
	 * </ol>
	 * <p/>
	 * You should only log on DEBUG level. See {@linkplain Parser parser}.
	 *
	 * @param dataSet the root directory of the file set. Data files are contained within the directory.
     * @param identifier identifer for this "parse run"
	 * @throws OutOfSequenceException if the data set is out of sequence in the expected order.
	 * @throws ParserException        if anything parser specific error happens or unexpected happens.
	 */
	void process(File dataSet, String identifier) throws ParserException;

	String getHome();
}
