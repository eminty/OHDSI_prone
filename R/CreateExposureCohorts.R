#' Create the exposure cohorts
#' Create Procedure Exclusion Cohort
#' @details
#' This function will create the exposure cohorts following the definitions included in this package.
#'
#' @param connectionDetails      An object of type \code{connectionDetails} as created using the
#'                               \code{\link[DatabaseConnector]{createConnectionDetails}} function in
#'                               the DatabaseConnector package.
#' @param cdmDatabaseSchema      Schema name where your patient-level data in OMOP CDM format resides.
#'                               Note that for SQL Server, this should include both the database and
#'                               schema name, for example 'cdm_data.dbo'.
#' @param vocabularyDatabaseSchema   Schema name where your vocabulary tables in OMOP CDM format resides.
#'                               Note that for SQL Server, this should include both the database and
#'                               schema name, for example 'cdm_data.dbo'.
#' @param cohortDatabaseSchema   Schema name where intermediate data can be stored. You will need to
#'                               have write priviliges in this schema. Note that for SQL Server, this
#'                               should include both the database and schema name, for example
#'                               'cdm_data.dbo'.
#' @param tablePrefix            A prefix to be used for all table names created for this study.
#' @param indicationId           A string denoting the indicationId for which the exposure cohorts
#'                               should be created.
#' @param oracleTempSchema       Should be used in Oracle to specify a schema where the user has write
#'                               priviliges for storing temporary tables.
#' @param outputFolder           Name of local folder to place results; make sure to use forward
#'                               slashes (/)
#' @param databaseId             A short string for identifying the database (e.g. 'Synpuf').
#' @param filterExposureCohorts  Optional subset of exposure cohorts to use; \code{NULL} implies all.
#' @param imputeExposureLengthWhenMissing  For PanTher: impute length of drug exposures when the length is missing?
#'
#' @export
createNoProcCohort <- function(connectionDetails,
                                  cdmDatabaseSchema,
                                  vocabularyDatabaseSchema = cdmDatabaseSchema,
                                  cohortDatabaseSchema,
                                  tablePrefix = "prone_nlp",
                                  indicationId = "covid_hosp_proc_excl",
                                  oracleTempSchema,
                                  outputFolder,
                                  databaseId,
                                  filterExposureCohorts = NULL,
                                  imputeExposureLengthWhenMissing = FALSE) {
  
  indicationFolder <- file.path(outputFolder, indicationId)
  if (!file.exists(indicationFolder)) {
    dir.create(indicationFolder, recursive = TRUE)
  }
  
  ParallelLogger::logInfo("Creating ", indicationId, " exposure cohort")
  
  cohortTable <- paste(tablePrefix, indicationId, "cohort", sep = "_")
  
  # Note: using connection when calling createCohortTable and instantiateCohortSet is pereferred, but requires this
  # fix in CohortDiagnostics to be released: https://github.com/OHDSI/CohortDiagnostics/commit/f4c920bc4feb5d701f1149ddd9cf7ca968be6a71
  # connection <- DatabaseConnector::connect(connectionDetails)
  # on.exit(DatabaseConnector::disconnect(connection))
  
  # CohortDiagnostics:::createCohortTable(connectionDetails = connectionDetails,
  #                                      cohortDatabaseSchema = cohortDatabaseSchema,
  #                                      cohortTable = cohortTable)
  
  ParallelLogger::logInfo("- Populating table ", cohortTable)
  
  CohortDiagnostics::instantiateCohortSet(connectionDetails = connectionDetails,
                                          cdmDatabaseSchema = cdmDatabaseSchema,
                                          vocabularyDatabaseSchema = vocabularyDatabaseSchema,
                                          cohortDatabaseSchema = cohortDatabaseSchema,
                                          oracleTempSchema = oracleTempSchema,
                                          cohortTable = cohortTable,
                                          packageName = "OHDSI_prone_eminty",
                                          createCohortTable = TRUE,
                                          cohortToCreateFile = paste0("settings/", indicationId, "CovidNoProcCohort.csv"),
                                          generateInclusionStats = FALSE,
                                          inclusionStatisticsFolder = indicationFolder)
  

#' Create the Procedure Exclusion, no Dex cohort
#'
#' @details
#' This function will create the outcome cohorts following the definitions included in this package.
#'
#' @param connectionDetails      An object of type \code{connectionDetails} as created using the
#'                               \code{\link[DatabaseConnector]{createConnectionDetails}} function in
#'                               the DatabaseConnector package.
#' @param cdmDatabaseSchema      Schema name where your patient-level data in OMOP CDM format resides.
#'                               Note that for SQL Server, this should include both the database and
#'                               schema name, for example 'cdm_data.dbo'.
#' @param vocabularyDatabaseSchema   Schema name where your vocabulary tables in OMOP CDM format resides.
#'                               Note that for SQL Server, this should include both the database and
#'                               schema name, for example 'cdm_data.dbo'.
#' @param cohortDatabaseSchema   Schema name where intermediate data can be stored. You will need to
#'                               have write priviliges in this schema. Note that for SQL Server, this
#'                               should include both the database and schema name, for example
#'                               'cdm_data.dbo'.
#' @param indicationId           A string denoting the indicationId for which the exposure cohorts
#'                               should be created.
#' @param tablePrefix            A prefix to be used for all table names created for this study.
#' @param oracleTempSchema       Should be used in Oracle to specify a schema where the user has write
#'                               priviliges for storing temporary tables.
#' @param outputFolder           Name of local folder to place results; make sure to use forward
#'                               slashes (/)
#' @param databaseId             A short string for identifying the database (e.g. 'Synpuf').
#' @param filterOutcomeCohorts  Optional subset of exposure cohorts to use; \code{NULL} implies all.
#'
#' @export
createNoProcOnDexCohort <- function(connectionDetails,
                                 cdmDatabaseSchema,
                                 vocabularyDatabaseSchema = cdmDatabaseSchema,
                                 cohortDatabaseSchema,
                                 tablePrefix = "prone_nlp",
                                 indicationId = "covid_hosp_proc_excl_on_dex",
                                 oracleTempSchema,
                                 outputFolder,
                                 databaseId,
                                 filterOutcomeCohorts = NULL) {
  
  outcomeFolder <- file.path(outputFolder, "NoProc_OnDex")
  if (!file.exists(outcomeFolder)) {
    dir.create(outcomeFolder, recursive = TRUE)
  }
  
  ParallelLogger::logInfo("Creating ", indicationId, " exposure cohort")
  
  cohortTable <- paste(tablePrefix, indicationId, "cohort", sep = "_")
  
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  
  # CohortDiagnostics:::createCohortTable(connection = connection,
  #                                      cohortDatabaseSchema = cohortDatabaseSchema,
  #                                      cohortTable = cohortTable)
  
  ParallelLogger::logInfo("- Populating table ", cohortTable)
  
  CohortDiagnostics::instantiateCohortSet(connection = connection,
                                          cdmDatabaseSchema = cdmDatabaseSchema,
                                          vocabularyDatabaseSchema = vocabularyDatabaseSchema,
                                          cohortDatabaseSchema = cohortDatabaseSchema,
                                          oracleTempSchema = oracleTempSchema,
                                          cohortTable = cohortTable,
                                          packageName = "LegendT2dm",
                                          cohortToCreateFile = "settings/OutcomesOfInterest.csv",
                                          generateInclusionStats = FALSE,
                                          createCohortTable = TRUE,
                                          inclusionStatisticsFolder = outcomeFolder)
  
