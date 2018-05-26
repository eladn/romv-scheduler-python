from scheduler_interface import SchedulerInterface
from transaction import Transaction
from timestamps_manager import Timestamp, TimestampsManager
from romv_transaction import ROMVTransaction, UMVTransaction
from locks_manager import LocksManager
from multi_version_data_manager import MultiVersionDataManager
from multi_version_gc import MultiVersionGC
from logger import Logger
import copy  # for deep-coping the disk in order to add the yet-committed variables so they could be printed to run-log.


class DeadlockCycleAbortReason:
    def __init__(self, deadlock_cycle):
        self.deadlock_cycle = deadlock_cycle

    def __str__(self):
        return 'Deadlock cycle found: ' + str(self.deadlock_cycle)


# TODO: fully doc it!
# When the `SchedulerInterface` encounters a deadlock, it chooses a victim transaction and aborts it.
# It means that the scheduler would have to store the operations in a redo-log. (is that correct?)
class ROMVScheduler(SchedulerInterface):
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
        if transaction.is_read_only:
            assert isinstance(transaction, ROMVTransaction)
            # Behaviour changed: We now assign timestamp only when the first read operation is being tried to be performed.
            ## Assign new timestamp for RO transactions.
            ## self.assign_timestamp_to_transaction(transaction)
            ## self._mv_gc.new_read_only_transaction(transaction)
        else:
            assert isinstance(transaction, UMVTransaction)
            # TODO: should we do here something for update transaction?

    def get_current_ts(self):
        return self._timestamps_manager.peek_next_ts()

    # TODO: doc!
    def run(self):
        for transaction in self.iterate_over_transactions_by_tid_and_safely_remove_marked_to_remove_transactions():
            serialization_point_flag = False

            # The user haven't yet not assigned the next operation to perform for that transaction.
            if not transaction.has_waiting_operation_to_perform(self):
                continue

            # Assign new timestamp for RO transactions when trying to perform the first read.
            if transaction.is_read_only and not transaction.has_timestamp:
                assert isinstance(transaction, ROMVTransaction)
                self.assign_timestamp_to_transaction(transaction)
                serialization_point_flag = True
                self._mv_gc.new_read_only_transaction(transaction)

            # Try execute next operation
            transaction.try_perform_next_operation(self)
            # Invariant: At any point in time, there is no cycle in the "wait-for" graph.
            #            Each time a transaction attempts to lock a variable, the conflict graph
            #            is updated. When a conflict cycle is firstly created it is immediately
            #            detected and then immediately broken by the scheduler.
            assert not self._locks_manager.is_deadlock()
            if transaction.is_aborted:
                # Note: the transaction already has been marked to remove by the scheduler.
                assert isinstance(transaction, UMVTransaction)
                continue
            if transaction.is_completed:
                self.mark_transaction_to_remove(transaction)  # TODO: does it have to be after handling the completed transaction?
                if not transaction.is_read_only:
                    assert isinstance(transaction, UMVTransaction)
                    self.assign_timestamp_to_transaction(transaction)
                    serialization_point_flag = True
                    transaction.complete_writes(self._mv_data_manager)
                    self._locks_manager.release_all_locks(transaction.transaction_id)
                self._mv_gc.transaction_committed(transaction, self)  # TODO: should it be before releasing locks?
                self._mv_gc.run_waiting_gc_jobs(self)

            if serialization_point_flag:
                Logger().log('     Serialization point. Timestamp: {ts}'.format(ts=transaction.timestamp),
                             log_type_name='serialization_point')

    def assign_timestamp_to_transaction(self, transaction: Transaction):
        transaction.timestamp = self._timestamps_manager.get_next_ts()
        self.serialization_point(transaction.transaction_id)

    # TODO: doc!
    def try_write(self, transaction_id, variable, value):
        transaction = self.get_transaction_by_id(transaction_id)
        assert transaction is not None
        assert not transaction.is_read_only
        assert isinstance(transaction, UMVTransaction)

        got_lock, collides_with__or__deadlock_cycle = self._locks_manager.try_acquire_lock(transaction_id, variable, 'write')
        if got_lock == 'DEADLOCK':
            # Invariant: At any point in time, there is no cycle in the "wait-for" graph.
            #            Each time a transaction attempts to lock a variable, the conflict graph
            #            is updated. When a conflict cycle is firstly created it is immediately
            #            detected and then immediately broken by the scheduler.
            # TODO: maybe abort another transaction?
            self.abort_transaction(transaction, DeadlockCycleAbortReason(collides_with__or__deadlock_cycle))  # FIXME: is it ok to call it here?
            assert not self._locks_manager.is_deadlock()
            return False  # failed
        elif got_lock == 'WAIT':
            transaction.waits_for = collides_with__or__deadlock_cycle
            return False  # failed

        assert got_lock == 'GOT_LOCK'
        # FIXME: is this "local-storing" mechanism ok?
        transaction.local_write(variable, value)
        return True  # succeed writing the new value

    # TODO: doc!
    # Use the `_locks_manager` only for update transaction.
    def try_read(self, transaction_id, variable):
        transaction = self.get_transaction_by_id(transaction_id)
        assert transaction is not None
        if transaction.is_read_only:
            assert isinstance(transaction, ROMVTransaction)
            return self._mv_data_manager.read_older_version_than(variable, transaction.timestamp)

        assert isinstance(transaction, UMVTransaction)
        got_lock, collides_with__or__deadlock_cycle = self._locks_manager.try_acquire_lock(transaction_id, variable, 'read')
        if got_lock == 'DEADLOCK':
            # Invariant: At any point in time, there is no cycle in the "wait-for" graph.
            #            Each time a transaction attempts to lock a variable, the conflict graph
            #            is updated. When a conflict cycle is firstly created it is immediately
            #            detected and then immediately broken by the scheduler.
            # TODO: maybe abort another transaction?
            self.abort_transaction(transaction, DeadlockCycleAbortReason(collides_with__or__deadlock_cycle))  # FIXME: is it ok to call it here?
            assert not self._locks_manager.is_deadlock()
            return None  # failed
        elif got_lock == 'WAIT':
            assert isinstance(collides_with__or__deadlock_cycle, set)
            transaction.waits_for = collides_with__or__deadlock_cycle
            return None  # failed

        assert got_lock == 'GOT_LOCK'
        # FIXME: is this "local-storing" mechanism ok?
        value = transaction.local_read(variable)
        if value is not None:
            return value
        return self._mv_data_manager.read_latest_version(variable)

    # TODO: doc!
    def abort_transaction(self, transaction: Transaction, reason):
        assert not transaction.is_read_only
        assert isinstance(transaction, UMVTransaction)
        self._locks_manager.release_all_locks(transaction.transaction_id)
        self.mark_transaction_to_remove(transaction)  # TODO: is it ok?
        # Notice: The call to `transaction.abort(..)` invokes the user-callback that might
        # add a new transaction with the same transaction id as of the aborted transaction.
        # It is ok because we already marked the aborted transaction to remove, and by side
        # effect it has been removed from the ongoing_transactions_mapping.
        # Additionally, note that the potentially new added transaction (with the same
        # transaction id) has been added before the aborted transaction in the transactions
        # list sorted by transaction_id. So that the iteration in `run(..)` won't encounter
        # this transaction again until next loop (in RR scheduling scheme).
        transaction.abort(self, reason)
        # TODO: Undo the transaction. We currently storing the updates locally on the transaction itself only.

    # For the GC mechanism, when a read-only transaction commits, we need to find the
    # intersection of variables that the just-committed reader is responsible for, with
    # the variables that the oldest younger read-only transaction is responsible for.
    # This method helps us finding this read-only transaction.
    def get_read_transaction_younger_than(self, transaction: Transaction):
        if transaction.is_read_only:
            current_node = transaction.ro_transactions_by_arrival_list_node.next_node
        else:
            current_node = transaction.transactions_by_arrival_list_node.next_node
        while current_node is not None and (not current_node.data.is_read_only or current_node.data.is_finished
                                            or not current_node.data.has_timestamp):
            current_node = current_node.next_node
        return None if current_node is None else current_node.data

    # For the GC mechanism, when a read-only transaction commits, we need to find the
    # responsibility time-span of the just-committed reader. This time-span starts
    # when the youngest older reader was born.
    # This method helps us finding this read-only transaction.
    def get_read_transaction_older_than(self, transaction: Transaction):
        if transaction.is_read_only:
            current_node = transaction.ro_transactions_by_arrival_list_node.prev_node
        else:
            current_node = transaction.transactions_by_arrival_list_node.prev_node
        while current_node is not None and (not current_node.data.is_read_only or current_node.data.is_finished
                                            or not current_node.data.has_timestamp):
            current_node = current_node.prev_node
        return None if current_node is None else current_node.data

    # For the GC mechanism, when an updater transaction commits, we need to decide
    # whether to evict the previous version of the updated variables. If there exists
    # a read-only transaction that is responsible for the previous version, we should
    # do nothing there. But it there is no such reader, the update transaction should
    # mark the previous version for eviction.
    # This method helps us finding whether this read-only transaction exists.
    def are_there_read_transactions_after_ts_and_before_transaction(self, after_ts: Timestamp, before_transaction: Transaction):
        reader = self.get_read_transaction_younger_than(before_transaction)
        if reader is None:
            return False
        return reader.timestamp > after_ts  # FIXME: should be `>=` inequality or just `>`?

    # Used for printing all of the variables to the run-log for debugging purposes.
    def get_variables(self):
        # TODO: if we use inner disk storage of a write transaction - do not do this!
        variables = copy.deepcopy(dict(self._mv_data_manager.get_variables()))
        for transaction in self._ongoing_u_transactions_by_arrival:
            assert isinstance(transaction, UMVTransaction)
            if transaction.is_aborted:
                continue
            for var, new_uncommitted_version in transaction._local_written_values.items():
                if var not in variables:
                    variables[var] = []
                variables[var].append((new_uncommitted_version, 'uncommitted'))
        return variables.items()

