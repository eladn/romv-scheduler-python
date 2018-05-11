from collections import namedtuple
from scheduler_base_modules import Scheduler, Transaction, Timestamp, TimestampsManager, VariablesSet


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

    def read_older_version_than(self, variable, max_ts):
        pass  # TODO: impl

    def write_new_version(self, variable, new_value, ts):
        pass  # TODO: impl

    def get_latest_version_number(self, variable):
        pass  # TODO: impl

    def read_latest_version(self, variable):
        pass  # TODO: impl


class ROMVTransaction(Transaction):
    def __init__(self, *args, **kargs):
        kargs['is_read_only'] = True
        super().__init__(*args, **kargs)
        self._committed_variables_set_since_last_reader_born = VariablesSet()

    @property
    def committed_variables_set_since_last_reader_born(self):
        return self._committed_variables_set_since_last_reader_born

    @committed_variables_set_since_last_reader_born.setter
    def committed_variables_set_since_last_reader_born(self, new_set):
        assert isinstance(new_set, VariablesSet)
        self._committed_variables_set_since_last_reader_born = new_set


class UMVTransaction(Transaction):
    def __init__(self, *args, **kargs):
        kargs['is_read_only'] = False
        super().__init__(*args, **kargs)
        self._local_written_values = dict()  # TODO: is it ok to store them on memory only?

    def complete_writes(self, mv_data_manager: MultiVersionDataManager):
        for variable, value in self._local_written_values.items():
            mv_data_manager.write_new_version(variable, value, self.timestamp)

    def local_write(self, variable, value):
        self._local_written_values[variable] = value

    def local_read(self, variable):
        if variable not in self._local_written_values:
            return None
        return self._local_written_values[variable]


class MultiVersionGC:
    GCJob = namedtuple('GCJob', ['variable', 'min_ts', 'max_ts'])

    def __init__(self, mv_data_manager: MultiVersionDataManager):
        self._mv_data_manager = mv_data_manager
        self._gc_jobs_queue = []  # list of GCJob instances
        self._committed_variables_set_since_younger_reader_born = VariablesSet()
        pass  # TODO: impl

    def new_read_only_transaction(self, transaction: ROMVTransaction):
        transaction.committed_variables_set_since_last_reader_born = self._committed_variables_set_since_younger_reader_born
        self._committed_variables_set_since_younger_reader_born = VariablesSet()
        pass  # TODO: impl

    def _read_only_transaction_committed(self, transaction: ROMVTransaction):
        pass  # TODO: impl

    # TODO: we may need the readers list to see whether there is a read transaction between the writes. how do we get it?
    def _update_transaction_committed(self, transaction: UMVTransaction):
        pass  # TODO: impl

    def transaction_committed(self, transaction: Transaction):
        if transaction.is_read_only:
            assert isinstance(transaction, ROMVTransaction)
            self._read_only_transaction_committed(transaction)
        else:
            assert isinstance(transaction, UMVTransaction)
            self._update_transaction_committed(transaction)

    def run_waiting_gc_jobs(self):
        pass  # TODO: impl


# TODO: fully doc it!
# When the `Scheduler` encounters a deadlock, it chooses a victim transaction and aborts it.
# It means that the scheduler would have to store the operations in a redo-log. (is that correct?)
class ROMVScheduler(Scheduler):
    ROTransaction = ROMVTransaction
    UTransaction = UMVTransaction
    SchedulingSchemes = {'RR', 'serial'}

    def __init__(self, scheduling_scheme):
        super().__init__()
        assert scheduling_scheme in ROMVScheduler.SchedulingSchemes
        self._scheduling_scheme = scheduling_scheme
        self._locks_manager = LocksManager()
        self._mv_data_manager = MultiVersionDataManager()
        self._mv_gc = MultiVersionGC(self._mv_data_manager)
        self._timestamps_manager = TimestampsManager()

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
        # TODO: allow using `serial` scheduling-scheme iteration. Currently using only `RR`.

        while len(self._ongoing_transactions) > 0:
            for transaction in self._ongoing_transactions:
                # Try execute next operation
                transaction.try_perform_next_operation(self)
                if transaction.is_aborted:
                    continue
                if transaction.is_completed:
                    if not transaction.is_read_only:
                        assert isinstance(transaction, UMVTransaction)
                        transaction.timestamp = self._timestamps_manager.get_next_ts()
                        transaction.complete_writes(self._mv_data_manager)
                    self._locks_manager.release_all_locks(transaction.transaction_id)
                    self._mv_gc.transaction_committed(transaction)  # TODO: should it be before releasing locks?
            assert not self._locks_manager.is_deadlock()
            self.remove_completed_transactions()  # Cannot be performed inside the loop. # TODO: remove also aborted!

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
        self.remove_transaction(transaction)  # problem: removed while iterating on the transactions list..
        transaction.abort(self)
        # TODO: undo the transaction.
