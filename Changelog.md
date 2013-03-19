## sdm-core 4.2
*  Bruger nsp-util 1.0.9, så konfiguration af SLA-logging kan foretages fra classpath på JBoss 7

## sdm-core 4.3
*  NSPSUPPORT-126: ParserExecutor logger filers absolutte stier og md5-summer inden parser behandler dem

## sdm-core 4.4
*  Flyttet RecordSettere til deres egen pakke
*  Udvidet RecordPersister så records kan opdateres
*  Udvidet RecordFetcher så den kan hente Records med metadata
Use 4.5 instead

## sdm-core 4.5
*  NSPSUPPORT-188: Importere der benytter sdm4-core 4.4 får fejl når en record forsøges opdateret - 'expected 1 found 2'
    -FIX:  Make sure persister and fetcher always have the exact same transaction time
*  Allow insertion of null values in fields
!! Make sure to test properly if upgrading to this version, fetcher fetchcurrent has more conditions added
   however it should make no different in the components using it is inserting correct validFrom.

## sdm-core 4.6
* NSPSUPPORT-189 Tillad filer større end 4gb's at blive importeret.
* Tillad nøgle værdier kan være null når man henter data op (i forbindelse med NSPSUPPORT-182)
* Map altid database NULLs til java NULLs tidligere mappede den til 0 hvis det var decimal eller numeric
  (i forbindelse med NSPSUPPORT-182)

## sdm-core 4.7
* Added @Primary annotation to datasource to allow multiple datasources in importers

## sdm-core 4.8
* Fields in records specs can now be of calculated type, which will not be counted when parsing input files.
* Added MD5Generator as it is needed in multiple importers
* Fix typo in fetchCurrent