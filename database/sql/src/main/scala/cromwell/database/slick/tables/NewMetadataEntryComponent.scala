package cromwell.database.slick.tables

import java.sql.Timestamp

import cromwell.database.sql.tables.MetadataEntry
import javax.sql.rowset.serial.SerialClob

trait NewMetadataEntryComponent {

  this: DriverComponent with WorkflowMetadataSummaryEntryComponent =>

  import driver.api._

  class NewMetadataEntries(tag: Tag) extends Table[MetadataEntry](tag, "NEW_METADATA_ENTRY") {

    // TODO: Should this be started at 0 or from where the old metadata table stopped?
    def metadataEntryId = column[Long]("METADATA_JOURNAL_ID", O.PrimaryKey, O.AutoInc)

    def workflowExecutionUuid = column[String]("WORKFLOW_EXECUTION_UUID", O.PrimaryKey, O.Length(255))

    def callFullyQualifiedName = column[Option[String]]("CALL_FQN", O.Length(255))

    def jobIndex = column[Option[Int]]("JOB_SCATTER_INDEX")

    def jobAttempt = column[Option[Int]]("JOB_RETRY_ATTEMPT")

    def metadataKey = column[String]("METADATA_KEY", O.Length(255))

    def metadataValue = column[Option[SerialClob]]("METADATA_VALUE")

    def metadataValueType = column[Option[String]]("METADATA_VALUE_TYPE", O.Length(10))

    def metadataTimestamp = column[Timestamp]("METADATA_TIMESTAMP")

    override def * = (workflowExecutionUuid, callFullyQualifiedName, jobIndex, jobAttempt, metadataKey, metadataValue,
      metadataValueType, metadataTimestamp, metadataEntryId.?) <> (MetadataEntry.tupled, MetadataEntry.unapply)

    // TODO: Add necessary indexes
  }

  val newMetadataEntries = TableQuery[NewMetadataEntries]

  val newMetadataEntriesExists = Compiled(newMetadataEntries.take(1).exists)

  val newMetadataEntryIdsAutoInc = newMetadataEntries returning newMetadataEntries.map(_.metadataEntryId)

  val newMetadataEntriesForWorkflowExecutionUuid = Compiled(
    (workflowExecutionUuid: Rep[String]) => (for {
      metadataEntry <- newMetadataEntries
      if metadataEntry.workflowExecutionUuid === workflowExecutionUuid
    } yield metadataEntry).sortBy(_.metadataTimestamp)
  )

  val newMetadataEntryExistsForWorkflowExecutionUuid = Compiled(
    (workflowExecutionUuid: Rep[String]) => (for {
      metadataEntry <- newMetadataEntries
      if metadataEntry.workflowExecutionUuid === workflowExecutionUuid
    } yield metadataEntry).exists
  )

  val countNewMetadataEntriesForWorkflowExecutionUuid = Compiled(
    (rootWorkflowId: Rep[String], expandSubWorkflows: Rep[Boolean]) => {
      val targetWorkflowIds = for {
        summary <- workflowMetadataSummaryEntries
        // Uses `IX_WORKFLOW_METADATA_SUMMARY_ENTRY_RWEU`, `UC_WORKFLOW_METADATA_SUMMARY_ENTRY_WEU`
        if summary.workflowExecutionUuid === rootWorkflowId || ((summary.rootWorkflowExecutionUuid === rootWorkflowId) && expandSubWorkflows)
      } yield summary.workflowExecutionUuid

      for {
        metadata <- newMetadataEntries
        if metadata.workflowExecutionUuid in targetWorkflowIds // Uses `METADATA_WORKFLOW_IDX`
      } yield metadata
    }.size
  )

  val newMetadataEntriesForWorkflowExecutionUuidAndMetadataKey = Compiled(
    (workflowExecutionUuid: Rep[String], metadataKey: Rep[String]) => (for {
      metadataEntry <- newMetadataEntries
      if metadataEntry.workflowExecutionUuid === workflowExecutionUuid
      if metadataEntry.metadataKey === metadataKey
      if metadataEntry.callFullyQualifiedName.isEmpty
      if metadataEntry.jobIndex.isEmpty
      if metadataEntry.jobAttempt.isEmpty
    } yield metadataEntry).sortBy(_.metadataTimestamp)
  )

  val countNewMetadataEntriesForWorkflowExecutionUuidAndMetadataKey = Compiled(
    (rootWorkflowId: Rep[String], metadataKey: Rep[String], expandSubWorkflows: Rep[Boolean]) => {
      val targetWorkflowIds = for {
        summary <- workflowMetadataSummaryEntries
        // Uses `IX_WORKFLOW_METADATA_SUMMARY_ENTRY_RWEU`, `UC_WORKFLOW_METADATA_SUMMARY_ENTRY_WEU`
        if summary.workflowExecutionUuid === rootWorkflowId || ((summary.rootWorkflowExecutionUuid === rootWorkflowId) && expandSubWorkflows)
      } yield summary.workflowExecutionUuid

      for {
        metadata <- newMetadataEntries
        if metadata.workflowExecutionUuid in targetWorkflowIds // Uses `METADATA_WORKFLOW_IDX`
        if metadata.metadataKey === metadataKey
        if metadata.callFullyQualifiedName.isEmpty
        if metadata.jobIndex.isEmpty
        if metadata.jobAttempt.isEmpty
      } yield metadata
    }.size
  )

  val newMetadataEntriesForJobKey = Compiled(
    (workflowExecutionUuid: Rep[String], callFullyQualifiedName: Rep[String], jobIndex: Rep[Option[Int]],
     jobAttempt: Rep[Option[Int]]) => (for {
      metadataEntry <- newMetadataEntries
      if metadataEntry.workflowExecutionUuid === workflowExecutionUuid
      if metadataEntry.callFullyQualifiedName === callFullyQualifiedName
      if hasSameIndex(metadataEntry, jobIndex)
      if hasSameAttempt(metadataEntry, jobAttempt)
    } yield metadataEntry).sortBy(_.metadataTimestamp)
  )

  val countNewMetadataEntriesForJobKey = Compiled(
    (rootWorkflowId: Rep[String], callFullyQualifiedName: Rep[String], jobIndex: Rep[Option[Int]],
     jobAttempt: Rep[Option[Int]], expandSubWorkflows: Rep[Boolean]) => {
      val targetWorkflowIds = for {
        summary <- workflowMetadataSummaryEntries
        // Uses `IX_WORKFLOW_METADATA_SUMMARY_ENTRY_RWEU`, `UC_WORKFLOW_METADATA_SUMMARY_ENTRY_WEU`
        if summary.workflowExecutionUuid === rootWorkflowId || ((summary.rootWorkflowExecutionUuid === rootWorkflowId) && expandSubWorkflows)
      } yield summary.workflowExecutionUuid

      for {
        metadata <- newMetadataEntries
        if metadata.workflowExecutionUuid in targetWorkflowIds // Uses `METADATA_WORKFLOW_IDX`
        if metadata.callFullyQualifiedName === callFullyQualifiedName
        if hasSameIndex(metadata, jobIndex)
        if hasSameAttempt(metadata, jobAttempt)
      } yield metadata
    }.size
  )

  val newMetadataEntriesForJobKeyAndMetadataKey = Compiled(
    (workflowExecutionUuid: Rep[String], metadataKey: Rep[String], callFullyQualifiedName: Rep[String],
     jobIndex: Rep[Option[Int]], jobAttempt: Rep[Option[Int]]) => (for {
      metadataEntry <- newMetadataEntries
      if metadataEntry.workflowExecutionUuid === workflowExecutionUuid
      if metadataEntry.metadataKey === metadataKey
      if metadataEntry.callFullyQualifiedName === callFullyQualifiedName
      if hasSameIndex(metadataEntry, jobIndex)
      if hasSameAttempt(metadataEntry, jobAttempt)
    } yield metadataEntry).sortBy(_.metadataTimestamp)
  )

  val countNewMetadataEntriesForJobKeyAndMetadataKey = Compiled(
    (rootWorkflowId: Rep[String], metadataKey: Rep[String], callFullyQualifiedName: Rep[String],
     jobIndex: Rep[Option[Int]], jobAttempt: Rep[Option[Int]], expandSubWorkflows: Rep[Boolean]) => {
      val targetWorkflowIds = for {
        summary <- workflowMetadataSummaryEntries
        // Uses `IX_WORKFLOW_METADATA_SUMMARY_ENTRY_RWEU`, `UC_WORKFLOW_METADATA_SUMMARY_ENTRY_WEU`
        if summary.workflowExecutionUuid === rootWorkflowId || ((summary.rootWorkflowExecutionUuid === rootWorkflowId) && expandSubWorkflows)
      } yield summary.workflowExecutionUuid

      for {
        metadata <- newMetadataEntries
        if metadata.workflowExecutionUuid in targetWorkflowIds // Uses `METADATA_WORKFLOW_IDX`
        if metadata.metadataKey === metadataKey
        if metadata.callFullyQualifiedName === callFullyQualifiedName
        if hasSameIndex(metadata, jobIndex)
        if hasSameAttempt(metadata, jobAttempt)
      } yield metadata
    }.size
  )

  def newMetadataEntriesForJobWithKeyConstraints(workflowExecutionUuid: String,
                                                metadataKeysToFilterFor: List[String],
                                                metadataKeysToFilterOut: List[String],
                                                callFqn: String,
                                                jobIndex: Option[Int],
                                                jobAttempt: Option[Int]) = {
    (for {
      metadataEntry <- newMetadataEntries
      if metadataEntry.workflowExecutionUuid === workflowExecutionUuid
      if metadataEntryHasMetadataKeysLike(metadataEntry, metadataKeysToFilterFor, metadataKeysToFilterOut)
      if metadataEntry.callFullyQualifiedName === callFqn
      if hasSameIndex(metadataEntry, jobIndex)
      // Assume that every metadata entry for a call should have a non null attempt value
      // Because of that, if the jobAttempt parameter is Some(_), make sure it matches, otherwise take all entries
      // regardless of the attempt
      if (metadataEntry.jobAttempt === jobAttempt) || jobAttempt.isEmpty
    } yield metadataEntry).sortBy(_.metadataTimestamp)
  }

  def newMetadataEntriesWithKeyConstraints(workflowExecutionUuid: String,
                                          metadataKeysToFilterFor: List[String],
                                          metadataKeysToFilterOut: List[String],
                                          requireEmptyJobKey: Boolean) = {
    (for {
      metadataEntry <- newMetadataEntries
      if metadataEntry.workflowExecutionUuid === workflowExecutionUuid
      if metadataEntryHasMetadataKeysLike(metadataEntry, metadataKeysToFilterFor, metadataKeysToFilterOut)
      if metadataEntryHasEmptyJobKey(metadataEntry, requireEmptyJobKey)
    } yield metadataEntry).sortBy(_.metadataTimestamp)
  }

  def countNewMetadataEntriesForJobWithKeyConstraints(rootWorkflowId: String,
                                                   metadataKeysToFilterFor: List[String],
                                                   metadataKeysToFilterOut: List[String],
                                                   callFqn: String,
                                                   jobIndex: Option[Int],
                                                   jobAttempt: Option[Int],
                                                   expandSubWorkflows: Boolean) = {

    val targetWorkflowIds = for {
      summary <- workflowMetadataSummaryEntries
      // Uses `IX_WORKFLOW_METADATA_SUMMARY_ENTRY_RWEU`, `UC_WORKFLOW_METADATA_SUMMARY_ENTRY_WEU`
      if summary.workflowExecutionUuid === rootWorkflowId || ((summary.rootWorkflowExecutionUuid === rootWorkflowId) && expandSubWorkflows)
    } yield summary.workflowExecutionUuid

    (for {
      metadataEntry <- newMetadataEntries
      if metadataEntry.workflowExecutionUuid in targetWorkflowIds
      if metadataEntryHasMetadataKeysLike(metadataEntry, metadataKeysToFilterFor, metadataKeysToFilterOut)
      if metadataEntry.callFullyQualifiedName === callFqn
      if hasSameIndex(metadataEntry, jobIndex)
      // Assume that every metadata entry for a call should have a non null attempt value
      // Because of that, if the jobAttempt parameter is Some(_), make sure it matches, otherwise take all entries
      // regardless of the attempt
      if (metadataEntry.jobAttempt === jobAttempt) || jobAttempt.isEmpty
    } yield metadataEntry).size
  }

  def countNewMetadataEntriesWithKeyConstraints(rootWorkflowId: String,
                                             metadataKeysToFilterFor: List[String],
                                             metadataKeysToFilterOut: List[String],
                                             requireEmptyJobKey: Boolean,
                                             expandSubWorkflows: Boolean) = {

    val targetWorkflowIds = for {
      summary <- workflowMetadataSummaryEntries
      // Uses `IX_WORKFLOW_METADATA_SUMMARY_ENTRY_RWEU`, `UC_WORKFLOW_METADATA_SUMMARY_ENTRY_WEU`
      if summary.workflowExecutionUuid === rootWorkflowId || ((summary.rootWorkflowExecutionUuid === rootWorkflowId) && expandSubWorkflows)
    } yield summary.workflowExecutionUuid

    (for {
      metadataEntry <- newMetadataEntries
      if metadataEntry.workflowExecutionUuid in targetWorkflowIds
      if metadataEntryHasMetadataKeysLike(metadataEntry, metadataKeysToFilterFor, metadataKeysToFilterOut)
      if metadataEntryHasEmptyJobKey(metadataEntry, requireEmptyJobKey)
    } yield metadataEntry).size
  }


  private[this] def metadataEntryHasMetadataKeysLike(metadataEntry: NewMetadataEntries,
                                                     metadataKeysToFilterFor: List[String],
                                                     metadataKeysToFilterOut: List[String]): Rep[Boolean] = {

    def containsKey(key: String): Rep[Boolean] = metadataEntry.metadataKey like key

    val positiveFilter: Option[Rep[Boolean]] = metadataKeysToFilterFor.map(containsKey).reduceOption(_ || _)
    val negativeFilter: Option[Rep[Boolean]] = metadataKeysToFilterOut.map(containsKey).reduceOption(_ || _)

    (positiveFilter, negativeFilter) match {
      case (Some(pf), Some(nf)) => pf && !nf
      case (Some(pf), None) => pf
      case (None, Some(nf)) => !nf

      // We should never get here, but there's no reason not to handle it:
      // ps: is there a better literal "true" in slick?
      case (None, None) => true: Rep[Boolean]
    }
  }

  private[this] def hasSameIndex(metadataEntry: NewMetadataEntries, jobIndex: Rep[Option[Int]]) = {
    (metadataEntry.jobIndex.isEmpty && jobIndex.isEmpty) || (metadataEntry.jobIndex === jobIndex)
  }

  private[this] def hasSameAttempt(metadataEntry: NewMetadataEntries, jobAttempt: Rep[Option[Int]]) = {
    (metadataEntry.jobAttempt.isEmpty && jobAttempt.isEmpty) || (metadataEntry.jobAttempt === jobAttempt)
  }

  private[this] def metadataEntryHasEmptyJobKey(metadataEntry: NewMetadataEntries,
                                                requireEmptyJobKey: Rep[Boolean]): Rep[Boolean] = {
    !requireEmptyJobKey ||
      (metadataEntry.callFullyQualifiedName.isEmpty &&
        metadataEntry.jobIndex.isEmpty &&
        metadataEntry.jobAttempt.isEmpty)
  }
}
