from collections import namedtuple
from logger import Logger
from transaction import Transaction
from romv_transaction import ROMVTransaction, UMVTransaction


# FIXME: maybe should be placed somewhere else?
VariableVersion = namedtuple('VariableVersion', ['variable', 'ts'])


# TODO: re-doc!
# The basic intuition is to evict the versions that no one would potentially need in
# the future. An update transaction may need only the latest version of any variable.
# But a read-only transaction may need the latest version since the transaction has
# born. We have to somehow keep track (efficiently) of versions and may need it in
# the future, so we could conservatively detect versions that no-one may need and evict
# it. Here we explain how the `MultiVersionGC` does it. Dealing wit updaters is simpler,
# so we explain it afterwards.
# Each ongoing read-only transaction holds a set of variables that has been committed
# (by an update transaction) before that reader has born, but after the youngest older
# reader has been born. So that, in each interval of time between the birth of each pair
# of consecutive read-only transactions, the youngest one holds a set of all the variables
# that has been committed in that interval of time. This makes that reader "responsible"
# for these versions in some sense. When a reader commits (and effectively ends its life),
# it should check whether the versions it is responsible for can be evicted. If so, it
# makes sure to delete these no-more-necessary versions. If some of these versions cannot
# get evicted (because another younger reader might need it or it is the latest version),
# the just-committed-reader should pass the responsibility for these versions to the oldest
# younger reader, as the just-committed-reader had never existed at all. Notice that a
# reader may access variables that has been committed in each one of the time-intervals
# that are older than the birth of that reader. Including its own interval, but also
# older ones.
# Practically, when an updater commits, it is still unknown which is the younger reader
# that is older than the just-committed-updater (because it would be born in the future).
# So we don't know in which set to store this variable. In order to deal with it, we
# keep a "global" set of variables, stored under the GC instance. When a new reader is
# born, this "global" set is assigned to that reader, and a new empty set it created
# and stored under this GC "global" pointer. It semantically means - variables that has
# been committed after the currently-youngest reader has been born.
# About updaters: When an update transaction commits, it checks whether there exists
# a reader that has born before the commit, but after the former version (of the same
# variable) has been committed. If so, this reader is responsible for the old version
# and we should do nothing. Otherwise, there exists no reader that is responsible for
# the former version. In that case, we should make sure to evict it when the updater
# commits. There exists no reader that needs this version, and there will be no such,
# because new readers would read the just-committed-version or newer one.
# GC-eviction-jobs:
# The GC is informed each time a transaction is committed. When the GC informed about a
# committed transaction, it checks about versions that can be evicted, as described below.
# When the GC encounters a version that has to be evicted, it does not actually perform
# the eviction. The eviction details are stored as a GC "job", in a dedicated gc-jobs-
# queue. Then, when the scheduler decides, it can offline call to `run_waiting_gc_jobs()`
# to perform the waiting previously registered evictions. The actual eviction application
# performs disk accesses. The idea is to avoid blocking the scheduler in favor of
# accessing the disk to perform GC evictions.
class MultiVersionGC:

    def __init__(self):
        # List of GCJob instances. Eviction jobs waiting to be performed.
        # The actual eviction is perform by `run_waiting_gc_jobs()`, but
        # the versions to evict are registered during the scheduler iterations.
        self._gc_jobs_queue = []

    # Called by the scheduler each time a new read-only transaction is born.
    def new_read_only_transaction(self, transaction: ROMVTransaction):
        pass
        # TODO: should we do anything else here?

    def _mark_version_for_eviction(self, version_to_be_marked_for_eviction: VariableVersion):
        self._gc_jobs_queue.append(version_to_be_marked_for_eviction)

    def _read_only_transaction_committed(self, committed_read_transaction: ROMVTransaction, scheduler):
        # Hierarchic order of mentioned items:
        # ######### | -------------------------------- | ########## |
        #  Youngest |  Time interval between readers   |  Current   |
        #  Older    |  The responsibility area of the  |  Committed |
        #  Reader   |  current committed reader        |  Reader    |
        #  Birth    | { responsibility variables set } |  Birth     |

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
                Logger().log('     GC: Add GC job because of updater committed variable `{variable}` with version ({new_version}) and there is no active reader since previous version ({prev_version}) of {variable}.'.format(variable=variable, new_version=committed_update_transaction.timestamp, prev_version=prev_version.ts), log_type_name='gc')
                self._mark_version_for_eviction(prev_version)

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
