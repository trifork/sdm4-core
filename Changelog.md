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
