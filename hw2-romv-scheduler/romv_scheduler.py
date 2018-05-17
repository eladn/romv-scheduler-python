from collections import namedtuple
from scheduler_base_modules import Scheduler, Transaction, Timestamp, TimestampsManager, NoFalseNegativeVariablesSet


# Invariant: at any point in time, there is no cycle in the "wait-for" graph.
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
        self._committed_variables_latest_versions_before_update = None

    @property
    def committed_variables_latest_versions_before_update(self):
        assert self._committed_variables_latest_versions_before_update is not None
        return self._committed_variables_latest_versions_before_update

    def complete_writes(self, mv_data_manager: MultiVersionDataManager):
        self._committed_variables_latest_versions_before_update = {
            variable: mv_data_manager.get_latest_version_number(variable)
            for variable in self._local_written_values.keys()
        }
        for variable, value in self._local_written_values.items():
            mv_data_manager.write_new_version(variable, value, self.timestamp)

    def local_write(self, variable, value):
        self._local_written_values[variable] = value

    def local_read(self, variable):
        if variable not in self._local_written_values:
            return None
        return self._local_written_values[variable]


class MultiVersionGC:
    GCJob = namedtuple('GCJob', ['variables_to_check', 'from_ts', 'to_ts'])

    def __init__(self):
        self._gc_jobs_queue = []  # list of GCJob instances
        self._committed_variables_set_since_younger_reader_born = NoFalseNegativeVariablesSet()

    def new_read_only_transaction(self, transaction: ROMVTransaction):
        transaction.committed_variables_set_since_last_reader_born = self._committed_variables_set_since_younger_reader_born
        self._committed_variables_set_since_younger_reader_born = NoFalseNegativeVariablesSet()
        pass  # TODO: should we do anything else here?

    def _read_only_transaction_committed(self, committed_read_transaction: ROMVTransaction, scheduler):
        older_reader = scheduler.get_read_transaction_older_than(committed_read_transaction)
        left_variables_set = committed_read_transaction.committed_variables_set_since_last_reader_born
        right_variables_set = self._get_committed_variables_set_after_reader_and_before_next(committed_read_transaction,
                                                                                             scheduler)
        intersection = left_variables_set.intersection(right_variables_set)

        # Remove the relevant versions (from_ts, to_ts) of the variables in the intersection!
        # We don't remove it now, but we add it as a remove job, to the GC jobs queue.
        from_ts = older_reader.timestamp if older_reader is not None else 0  # FIXME: is that correct?
        to_ts = committed_read_transaction.timestamp
        gc_job = MultiVersionGC.GCJob(variables_to_check=intersection, from_ts=from_ts, to_ts=to_ts)
        self._gc_jobs_queue.append(gc_job)

        # For the old versions we could not remove now, pass the responsibility to the next younger reader.
        unhandled = left_variables_set.difference(intersection)
        right_variables_set.add_variables(unhandled)

    def _update_transaction_committed(self, committed_update_transaction: UMVTransaction, scheduler):
        previous_versions = committed_update_transaction.committed_variables_latest_versions_before_update
        for variable, prev_version in previous_versions.items():
            if scheduler.are_there_read_transactions_after_ts_and_before_transaction(prev_version, committed_update_transaction):
                continue
            gc_job = MultiVersionGC.GCJob(variables_to_check={variable}, from_ts=prev_version, to_ts=prev_version)
            self._gc_jobs_queue.append(gc_job)

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
            assert not self._locks_manager.is_deadlock()  # invariant
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
            return self._mv_data_manager.read_older_version_than(variable, transaction.timestamp)

        assert isinstance(transaction, UMVTransaction)
        got_lock = self._locks_manager.try_acquire_lock(transaction_id, variable, 'read')
        if got_lock == 'DEADLOCK':
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

