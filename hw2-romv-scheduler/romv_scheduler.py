from collections import namedtuple
from scheduler_base_modules import Scheduler, Transaction, Timestamp, TimestampsManager, NoFalseNegativeVariablesSet


class DeadlockDetector:
    def __init__(self):
        self._wait_for_graph = None  # TODO: create a new directed graph (using networx lib).
        pass  # TODO: impl

    # Returns whether a dependency cycle has been created because of this new waiting.
    # If not, add the constrain to (add the matching edge to the graph).
    def wait_for(self, waiting_transaction_id, waiting_for_transaction_id):
        pass  # TODO: impl

    def transaction_ended(self, ended_transaction_id):
        pass  # TODO: impl. delete this transaction and the relevant edges.

    def is_deadlock(self):
        pass  # TODO: impl


class LocksManager:
    def __init__(self):
        # TODO: decide - whether do we want to check for deadlocks here or in the romv-scheduler.
        self._deadlock_detector = DeadlockDetector()
        pass  # TODO: impl - locks table fields (read locks multiset + write locks set)

    # Returns:
    #   "GOT_LOCK" the lock has been acquired successfully (or the transaction also has the lock).
    #   "WAIT" is the lock could not be acquired now (someone else is holding it, but there is no dependency cycle).
    #   "DEADLOCK" if a deadlock will be created if the transaction would wait for this wanted lock.
    # TODO: decide- do we really want to identify deadlocks here, or in the `ROMVScheduler` itself?
    def try_acquire_lock(self, transaction_id, variable, read_or_write):
        assert read_or_write in {'read', 'write'}
        pass  # TODO: impl

    def release_all_locks(self, transaction_id):
        # TODO: call self._deadlock_detector.transaction_ended(transaction_id)
        pass  # TODO: impl

    def is_deadlock(self):
        return self._deadlock_detector.is_deadlock()


class MultiVersionDataManager:
    def __init__(self):
        # TODO: make sure to explicitly separate the disk member from the "RAM" data-structures!
        pass  # TODO: impl

    def read_older_version_than(self, variable, max_ts: Timestamp):
        pass  # TODO: impl

    def write_new_version(self, variable, new_value, ts: Timestamp):
        pass  # TODO: impl

    def get_latest_version_number(self, variable):
        pass  # TODO: impl

    def read_latest_version(self, variable):
        pass  # TODO: impl

    def delete_old_versions_in_interval(self, variable, from_ts: Timestamp, to_ts: Timestamp):
        pass  # TODO: impl


class ROMVTransaction(Scheduler.ROTransaction):
    def __init__(self, *args, **kargs):
        kargs['is_read_only'] = True
        super().__init__(*args, **kargs)
        # Each ongoing read-only transaction holds a set of variables that has been committed
        # (by an update transaction) before that reader has born, but after the youngest older
        # reader has been born. So that, in each interval of time between the birth of each pair
        # of consecutive read-only transactions, the youngest one holds a set of all the variables
        # that has been committed in that interval of time. This makes that reader "responsible"
        # for these versions in some sense. More about it in the GC mechanism explanation.
        self._committed_variables_set_since_last_reader_born = NoFalseNegativeVariablesSet()

    @property
    def committed_variables_set_since_last_reader_born(self):
        return self._committed_variables_set_since_last_reader_born

    @committed_variables_set_since_last_reader_born.setter
    def committed_variables_set_since_last_reader_born(self, new_set):
        assert isinstance(new_set, NoFalseNegativeVariablesSet)
        self._committed_variables_set_since_last_reader_born = new_set


class UMVTransaction(Scheduler.UTransaction):
    def __init__(self, *args, **kargs):
        kargs['is_read_only'] = False
        super().__init__(*args, **kargs)
        self._local_written_values = dict()  # TODO: is it ok to store them on memory only?
        # For each variable that the transaction updates, we store the previous version
        # of that variable. This data is needed for the GC mechanism. In order to know
        # which versions to evict, in a case where there is no reader that is "responsible"
        # for the previous version.
        self._committed_variables_latest_versions_before_update = None

    @property
    def committed_variables_latest_versions_before_update(self):
        assert self._committed_variables_latest_versions_before_update is not None
        return self._committed_variables_latest_versions_before_update

    # Called by the scheduler after the transaction commits, in order to store to
    # disk the updates that have been made by this transaction, as new versions.
    def complete_writes(self, mv_data_manager: MultiVersionDataManager):
        self._committed_variables_latest_versions_before_update = {
            variable: mv_data_manager.get_latest_version_number(variable)
            for variable in self._local_written_values.keys()
        }
        for variable, value in self._local_written_values.items():
            mv_data_manager.write_new_version(variable, value, self.timestamp)

    # Updates that the transaction makes are accessible only for this transaction.
    # Hence, we store them locally on the transacting session instance.
    def local_write(self, variable, value):
        self._local_written_values[variable] = value

    def local_read(self, variable):
        if variable not in self._local_written_values:
            return None
        return self._local_written_values[variable]


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
# been committed after the corrently-youngest reader has been born.
# About updaters: When an update transaction commits, it checks whether there exists
# a reader that has born before the commit, but after the former version (of the same
# variable) has been committed. If so, this reader is responsible for the old version
# and we should do nothing. Otherwise, there exists no reader that is responsible for
# the former version. In that case, we should make sure to evict it when the updater
# commits. There exists no reader that needs this version, and there will be no such,
# because new readers would read the just-committed-version or newer one.
# GC jobs:
# The GC is informed each time a transaction is committed. When the GC informed about a
# committed transaction, it checks about versions that can be evicted, as described below.
# When the GC encounters a version that has to be evicted, it does not actually perform
# the eviction. The eviction details are stored as a GC "job", in a dedicated gc-jobs-
# queue. Then, when the scheduler decides, it can offline call to `run_waiting_gc_jobs()`
# to perform the waiting previously registered evictions. The actual eviction application
# performs disk accesses. The idea is to avoid blocking the scheduler in favor of
# accessing the disk to perform GC evictions.
class MultiVersionGC:
    GCJob = namedtuple('GCJob', ['variables_to_check', 'from_ts', 'to_ts'])

    def __init__(self):
        # List of GCJob instances. Eviction jobs waiting to be performed.
        # The actual eviction is perform by `run_waiting_gc_jobs()`, but
        # the versions to evict are registered during the scheduler iterations.
        self._gc_jobs_queue = []
        # This set stores all of the variables that has been committed by an update
        # transaction, since the younger read-only-transaction has born. When the
        # next reader will be born, this set will be assigned to that reader, and
        # a new empty set will be assigned to this pointer instead of the former one.
        self._committed_variables_set_since_younger_reader_born = NoFalseNegativeVariablesSet()

    # Called by the scheduler each time a new read-only transaction is born.
    def new_read_only_transaction(self, transaction: ROMVTransaction):
        transaction.committed_variables_set_since_last_reader_born = self._committed_variables_set_since_younger_reader_born
        self._committed_variables_set_since_younger_reader_born = NoFalseNegativeVariablesSet()
        # TODO: should we do anything else here?

    def _read_only_transaction_committed(self, committed_read_transaction: ROMVTransaction, scheduler):
        # ####################### | ------------------------------- | ##########################
        #  Youngest-Older-Reader  |  Time interval between readers  |  Current-Committed-Reader
        #                         |  The responsibility area of the |
        #                         |  current committed reader       |
        #                         |     { left variables set }      |
        older_reader = scheduler.get_read_transaction_older_than(committed_read_transaction)
        left_variables_set = committed_read_transaction.committed_variables_set_since_last_reader_born
        right_variables_set = self._get_committed_variables_set_after_reader_and_before_next(committed_read_transaction,
                                                                                             scheduler)

        # This is the time-span of the responsibility of the just-committed-reader.
        # Reminder: The responsibility time is from the birth of youngest-older reader,
        #           and until the birth of the just-committed-reader.
        from_ts = older_reader.timestamp if older_reader is not None else 0  # FIXME: is that correct?
        to_ts = committed_read_transaction.timestamp

        # This is the set of variables that has been committed in the responsibility
        # time-span of the just-committed-reader and also committed in the responsibility
        # time-span of the next younger reader. It means that now no reader needs them.
        intersection = left_variables_set.intersection(right_variables_set)

        # Register the relevant versions (from_ts, to_ts) of the variables in the intersection for eviction.
        # We don't evict it now, but we add it as a GC eviction job, to the GC eviction jobs queue.
        gc_job = MultiVersionGC.GCJob(variables_to_check=intersection, from_ts=from_ts, to_ts=to_ts)
        self._gc_jobs_queue.append(gc_job)

        # For the old versions we could not remove now, pass the responsibility to the next younger reader.
        # These versions would be eventually in the responsibility time-span of a reader that will evict
        # them when it commits.
        unhandled = left_variables_set.difference(intersection)
        right_variables_set.add_variables(unhandled)

    def _update_transaction_committed(self, committed_update_transaction: UMVTransaction, scheduler):
        previous_versions = committed_update_transaction.committed_variables_latest_versions_before_update
        for variable, prev_version in previous_versions.items():
            if scheduler.are_there_read_transactions_after_ts_and_before_transaction(prev_version,
                                                                                     committed_update_transaction):
                # If there exists a read-only transaction younger than the new committed version, but older
                # then the previous version (of the current examined variable), than that reader is responsible
                # for that previous version, and we should do nothing here.
                continue
            # We revealed that there is no reader that is responsible for the previous version of that variable.
            # New readers would be responsible for the new-just-created version. So the previous version can
            # be evicted. Hence, we add it as a GC eviction job.
            gc_job = MultiVersionGC.GCJob(variables_to_check={variable}, from_ts=prev_version, to_ts=prev_version)
            self._gc_jobs_queue.append(gc_job)

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
            gc_job = self._gc_jobs_queue.pop(index=0)
            # remove the relevant versions (from_ts, to_ts) of the variables to check!
            for variable in gc_job.variables_to_check:
                scheduler.mv_data_manager.delete_old_versions_in_interval(variable, gc_job.from_ts, gc_job.to_ts)

    def _get_committed_variables_set_after_reader_and_before_next(self, after_reader: ROMVTransaction, scheduler):
        older_transaction = scheduler.get_read_transaction_younger_than(after_reader)
        if older_transaction is None:
            return self._committed_variables_set_since_younger_reader_born
        assert isinstance(older_transaction, ROMVTransaction)
        return older_transaction.committed_variables_set_since_last_reader_born


# TODO: fully doc it!
# When the `Scheduler` encounters a deadlock, it chooses a victim transaction and aborts it.
# It means that the scheduler would have to store the operations in a redo-log. (is that correct?)
class ROMVScheduler(Scheduler):
    ROTransaction = ROMVTransaction
    UTransaction = UMVTransaction
    SchedulingSchemes = {'RR', 'serial'}

    def __init__(self, scheduling_scheme):
        super().__init__(scheduling_scheme)
        assert scheduling_scheme in ROMVScheduler.SchedulingSchemes
        self._scheduling_scheme = scheduling_scheme
        self._locks_manager = LocksManager()
        self._mv_data_manager = MultiVersionDataManager()
        self._mv_gc = MultiVersionGC()
        self._timestamps_manager = TimestampsManager()

    @property
    def mv_data_manager(self):
        return self._mv_data_manager

    def on_add_transaction(self, transaction: Transaction):
        # Assign new timestamp for RO transactions.
        if transaction.is_read_only:
            assert isinstance(transaction, ROMVTransaction)
            transaction.timestamp = self._timestamps_manager.get_next_ts()
            self._mv_gc.new_read_only_transaction(transaction)
        else:
            assert isinstance(transaction, UMVTransaction)
            # TODO: should we do here something for update transaction?

    def run(self):
        for transaction in self.iterate_over_transactions_by_tid_and_safely_remove_marked_to_remove_transactions():
            # Try execute next operation
            transaction.try_perform_next_operation(self)
            # Invariant: At any point in time, there is no cycle in the "wait-for" graph.
            #            Each time a transaction attempts to lock a variable, the conflict graph
            #            is updated. When a conflict cycle is firstly created it is immediately
            #            detected and then immediately broken by the scheduler.
            assert not self._locks_manager.is_deadlock()
            if transaction.is_aborted:
                # Note: the transaction already has been marked to remove by the scheduler.
                continue
            if transaction.is_completed:
                self.mark_transaction_to_remove(transaction)  # TODO: does it have to be after handling the completed transaction?
                if not transaction.is_read_only:
                    assert isinstance(transaction, UMVTransaction)
                    transaction.timestamp = self._timestamps_manager.get_next_ts()
                    transaction.complete_writes(self._mv_data_manager)
                self._locks_manager.release_all_locks(transaction.transaction_id)
                self._mv_gc.transaction_committed(transaction, self)  # TODO: should it be before releasing locks?

    # Use the `_locks_manager`.
    def try_write(self, transaction_id, variable, value):
        transaction = self.get_transaction_by_id(transaction_id)
        assert transaction is not None
        assert not transaction.is_read_only
        assert isinstance(transaction, UMVTransaction)

        got_lock = self._locks_manager.try_acquire_lock(transaction_id, variable, 'write')
        if got_lock == 'DEADLOCK':
            # Invariant: At any point in time, there is no cycle in the "wait-for" graph.
            #            Each time a transaction attempts to lock a variable, the conflict graph
            #            is updated. When a conflict cycle is firstly created it is immediately
            #            detected and then immediately broken by the scheduler.
            # TODO: maybe abort another transaction?
            self.abort_transaction(transaction)  # FIXME: is it ok to call it here?
            assert not self._locks_manager.is_deadlock()
            return False  # failed
        elif got_lock == 'WAIT':
            return False  # failed

        assert got_lock == 'GOT_LOCK'
        # FIXME: is this "local-storing" mechanism ok?
        transaction.local_write(variable, value)
        return True  # succeed writing the new value

    # Use the `_locks_manager` only for update transaction.
    def try_read(self, transaction_id, variable):
        transaction = self.get_transaction_by_id(transaction_id)
        assert transaction is not None
        if transaction.is_read_only:
            assert isinstance(transaction, ROMVTransaction)
            return self._mv_data_manager.read_older_version_than(variable, transaction.timestamp)

        assert isinstance(transaction, UMVTransaction)
        got_lock = self._locks_manager.try_acquire_lock(transaction_id, variable, 'read')
        if got_lock == 'DEADLOCK':
            # Invariant: At any point in time, there is no cycle in the "wait-for" graph.
            #            Each time a transaction attempts to lock a variable, the conflict graph
            #            is updated. When a conflict cycle is firstly created it is immediately
            #            detected and then immediately broken by the scheduler.
            # TODO: maybe abort another transaction?
            self.abort_transaction(transaction)  # FIXME: is it ok to call it here?
            assert not self._locks_manager.is_deadlock()
            return False  # failed
        elif got_lock == 'WAIT':
            return False  # failed

        assert got_lock == 'GOT_LOCK'
        # FIXME: is this "local-storing" mechanism ok?
        value = transaction.local_read(variable)
        if value is not None:
            return value
        return self._mv_data_manager.read_latest_version(variable)

    def abort_transaction(self, transaction: Transaction):
        self._locks_manager.release_all_locks(transaction.transaction_id)
        self.mark_transaction_to_remove(transaction)  # TODO: is it ok?
        # Notice: The call to `transaction.abort(..)` invokes the user-callback that might
        # add a new transaction with the same transaction id as of the aborted transaction.
        # It is ok because we already marked the aborted transaction to remove, and by side
        # effect it has been removed from the transaction_id_to_transaction_mapping.
        # Additionally, note that the potentially new added transaction (with the same
        # transaction id) has been added before the aborted transaction in the transactions
        # list sorted by transaction_id. So that the iteration in `run(..)` won't encounter
        # this transaction again until next loop (in RR scheduling scheme).
        transaction.abort(self)
        # TODO: Undo the transaction. We currently storing the updates locally on the transaction itself only.

    def get_read_transaction_younger_than(self, transaction: Transaction):
        if transaction.is_read_only:
            current_node = transaction.ro_transactions_by_arrival_list_node.prev_node
        else:
            current_node = transaction.transactions_by_tid_list_node.prev_node
        while current_node is not None and (not current_node.data.is_read_only or current_node.data.is_finished):
            current_node = current_node.prev_node
        return None if current_node is None else current_node.data

    def get_read_transaction_older_than(self, transaction: Transaction):
        if transaction.is_read_only:
            current_node = transaction.ro_transactions_by_arrival_list_node.next_node
        else:
            current_node = transaction.transactions_by_tid_list_node.next_node
        while current_node is not None and (not current_node.data.is_read_only or current_node.data.is_finished):
            current_node = current_node.next_node
        return None if current_node is None else current_node.data

    def are_there_read_transactions_after_ts_and_before_transaction(self, after_ts: Timestamp, before_transaction: Transaction):
        reader = self.get_read_transaction_younger_than(before_transaction)
        if reader is None:
            return False
        return reader.timestamp > after_ts  # FIXME: should be `>=` inequality or just `>`?

