from collections import namedtuple
from logger import Logger
from transaction import Transaction
from romv_transaction import ROMVTransaction, UMVTransaction


VariableVersion = namedtuple('VariableVersion', ['variable', 'ts'])


# The basic intuition is to evict the versions that no one would potentially need in
# the future. An update transaction may need only the latest version of any variable.
# But a read-only transaction may need the latest version since the transaction has
# born. We have to somehow keep track (efficiently) of old versions that may be needed in
# the future, so we could conservatively detect versions that no-one may need and evict
# it. Here we explain how the `MultiVersionGC` does it.
# Each ongoing read-only transaction holds a set of old versions of variables. For the rest
# of this explanation we are going to name this set by "the responsibility set" of that read-only
# transaction. This naming would make sense soon.
# Note that a version becomes potential for removal only when a new version exists for the same
# variable. Hence, we are going to track only such versions. We will not track latest versions.
# When an update-transaction commits a new version, it checks whether a previous version exists
# for that variable. This check is efficient because the `MultiVersionDataManager` stores a
# cache (in RAM) with the latest timestamp of each variable in the system. So no disk operation
# is needed here. We will see later on that no disk operation is needed at all for the versions
# tracking and "responsibility-passing" mechanism.
# If there exists a previous version for that variable, from this moment and on, someone has to
# be responsible for this previous version. So, the just-committed-updater had now just become
# responsible for that old version, but this transaction has just committed and going to be
# removed from the system, so it is going to pass the responsibility into someone else's hands.
# So the just-committed-updater searches for the youngest read-only transaction that has been
# born after the timestamp of the previous transaction. If such RO transaction exists, any
# transaction that may read from this previous version must be that RO transaction, or an older
# one. That is because we took the *younger* transaction of the ones that can read this version.
# In that case, the just-committed-updater assigns this reader to be responsible for the previous
# version. We will see below how that reader would handle the eviction of that version.
# If no such reader exists, it means that currently there is no ongoing reader in the system with
# timestamp between the timestamp of the previous version and the timestamp of the new version.
# RO transactions that are older from the previous version are not able to read from it and RO
# transactions that are younger from the just-committed-version will read its value. So, no reader
# would need the previous version. In that case, the just-committed-updater would mark this version
# for eviction (we will later see how it is done and when the actual eviction happens).
# When a read-only transaction commits, it has to handle the versions under its responsibility.
# To do so, it goes over the versions that has been assigned under its "responsibility set" one-
# -by-one and checks whether the youngest older read-only transaction may need it. If so, it passes
# the responsibility for that version to that youngest older reader. If not, it marks this version
# for eviction.
# The GC is informed each time a transaction is committed. When the GC informed about a
# committed transaction, it checks about versions that can be evicted, as described above.
# When the GC encounters a version that has to be evicted, it does not actually perform
# the eviction. The eviction details are stored as a GC "job", in a dedicated gc-jobs-
# queue. Then, when the scheduler decides, it can offline call to `run_waiting_gc_jobs()`
# to perform the waiting previously registered evictions. The actual eviction application
# performs disk accesses. The idea is to avoid blocking the scheduler in favor of
# accessing the disk to perform GC evictions. Notice that all of the described operations
# that take place whenever a transaction commits are efficient because of the data
# structures we maintain in the system: (1) list of RO transactions sorted by timestamps
# in `ROMVScheduler`, and (2) the timestamps of latest versions per variable in
# `MultiVersionDataManager` and in `UMVTransaction`.
class MultiVersionGC:

    def __init__(self):
        # List of `VariableVersion` instances. Eviction jobs waiting to be performed.
        # The actual eviction is performed by calling `run_waiting_gc_jobs()`, but
        # the versions to evict are registered during the scheduler iterations.
        self._gc_jobs_queue = []

    # Called by the scheduler each time a new read-only transaction is born.
    def new_read_only_transaction(self, transaction: ROMVTransaction):
        pass  # We actually have nothing else to do here.

    def _mark_version_for_eviction(self, version_to_be_marked_for_eviction: VariableVersion):
        self._gc_jobs_queue.append(version_to_be_marked_for_eviction)

    def _read_only_transaction_committed(self, committed_read_transaction: ROMVTransaction, scheduler):
        # Hierarchic order of mentioned items:
        # ######### | -------------------------------- | ########## |
        #  Youngest |  Time interval between readers   |  Current   |
        #  Older    |  The responsibility area of the  |  Committed |
        #  Reader   |  current committed reader        |  Reader    |
        #  Birth    | { responsibility variables set } |  Birth     |
        # ######### | -------------------------------- | ########## |

        older_reader = scheduler.get_ongoing_read_only_transaction_older_than(committed_read_transaction)
        for old_version in committed_read_transaction.old_versions_under_my_responsibility:
            if older_reader is None or older_reader.timestamp < old_version.ts:
                # Former readers won't be able to read this old version.
                # The just-committed-reader is the oldest reader that born before the
                # commit time of the new version of this variable.
                # Hence, this old version is un-nessanery and we mark it for eviction.
                self._mark_version_for_eviction(old_version)

                reason = 'there is no older reader'
                if older_reader is not None:
                    reason = 'the older reader (tid={older_reader_tid} ts={older_reader_ts}) has born before the ts of the old version ({old_version_ts})'.format(
                        older_reader_tid=older_reader.transaction_id,
                        older_reader_ts=older_reader.timestamp,
                        old_version_ts=old_version.ts
                    )
                Logger().log(
                    '     GC: The just-committed-reader marks an old version {old_version} under its responsibility for eviction because {reason}.'.format(
                        old_version=old_version,
                        reason=reason),
                    log_type_name='gc')
            else:
                # The older reader might need this version in the rest of his life-time.
                # Pass the responsibility for eviction to that older reader.
                # This older reader will decide what to do with it later when it commits (like here).
                assert old_version.ts < older_reader.timestamp  # the reader could not make this version.
                older_reader.old_versions_under_my_responsibility.add(old_version)

                Logger().log(
                    '     GC: The just-committed-reader passes old version {old_version} under its responsibility to its left older reader (tid={older_reader_tid} ts={older_reader_ts}).'.format(
                        old_version=old_version,
                        older_reader_tid=older_reader.transaction_id,
                        older_reader_ts=older_reader.timestamp),
                    log_type_name='gc')

    def _update_transaction_committed(self, committed_update_transaction: UMVTransaction, scheduler):
        previous_versions = committed_update_transaction.committed_variables_latest_versions_before_update
        for variable, prev_version_ts in previous_versions.items():
            prev_version = VariableVersion(variable=variable, ts=prev_version_ts)
            older_reader = scheduler.get_ongoing_youngest_read_only_transaction()
            assert older_reader is None or isinstance(older_reader, ROMVTransaction)
            assert older_reader is None or older_reader.timestamp < committed_update_transaction.timestamp
            if older_reader is not None and older_reader.timestamp < prev_version.ts:
                older_reader = None

            if older_reader is not None:
                # There exists a read-only transaction older than the new committed version, but younger
                # then the previous version (of the current examined variable). This is the oldest reader
                # that may use the previous version. Hence, We should pass the responsibility of the
                # previous version to that reader.
                older_reader.old_versions_under_my_responsibility.add(prev_version)

                Logger().log(
                    '     GC: The just-committed-updater passes the previous version {prev_version} of the just-committed-variable to the left older reader (tid={older_reader_tid} ts={older_reader_ts}).'.format(
                        prev_version=prev_version,
                        older_reader_tid=older_reader.transaction_id,
                        older_reader_ts=older_reader.timestamp),
                    log_type_name='gc')
            else:
                # We revealed that there is no reader that might read from the previous version of that variable.
                # New readers would read from the new-just-created version. So the previous version can be
                # evicted. Hence, we mark this version for eviction.
                self._mark_version_for_eviction(prev_version)

                Logger().log('     GC: Add GC job because of updater committed variable `{variable}` with version ({new_version}) and there is no active reader since previous version ({prev_version}) of {variable}.'.format(
                    variable=variable,
                    new_version=committed_update_transaction.timestamp,
                    prev_version=prev_version.ts), log_type_name='gc')

    # Called by the scheduler each time a transaction is committed.
    def transaction_committed(self, transaction: Transaction, scheduler):
        if transaction.is_read_only:
            assert isinstance(transaction, ROMVTransaction)
            self._read_only_transaction_committed(transaction, scheduler)
        else:
            assert isinstance(transaction, UMVTransaction)
            self._update_transaction_committed(transaction, scheduler)

    def run_waiting_gc_jobs(self, scheduler):
        nr_gc_jobs = len(self._gc_jobs_queue)
        for job_nr in range(nr_gc_jobs):
            version_to_evict = self._gc_jobs_queue.pop(0)
            # Actually evict the marked-to-evict version.
            scheduler.mv_data_manager.delete_old_version(version_to_evict.variable, version_to_evict.ts)
