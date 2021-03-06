from scheduler_interface import SchedulerInterface
from transaction import Transaction
from timestamps_manager import TimestampsManager
from romv_transaction import ROMVTransaction, UMVTransaction
from locks_manager import LocksManager
from multi_version_data_manager import MultiVersionDataManager
from multi_version_gc import MultiVersionGC
from logger import Logger
from doubly_linked_list import DoublyLinkedList
import copy  # for deep-coping the disk in order to add the yet-committed variables so they could be printed to run-log.


# Used for passing to the user callback when a transaction is aborted, as a reason for abortion.
class DeadlockCycleAbortReason:
    def __init__(self, deadlock_cycle):
        self.deadlock_cycle = deadlock_cycle

    def __str__(self):
        return 'Deadlock cycle found: ' + str(self.deadlock_cycle)


# The `ROMVScheduler` inherits from the basic `SchedulerInterface` and implements the ROMV protocol.
# It mainly implements the methods: `run(..)`, `try_read(..)`, and `try_write(..)`.
# We use the `LocksManager` for managing locks over the variables (which uses the `DeadlockDetector`).
# When the `ROMVScheduler` encounters a deadlock, it chooses a victim transaction and aborts it.
# For simplicity, the chosen victim is the first that closed the deadlock-cycle.
# The updates made during the update-transaction are stored inside of a local mapping inside of the
# update transaction. Find additional details about it under `UMVTransaction` class in `romv_transaction.py`.
# We use the `MultiVersionDataManager` for managing the versions and accessing the disk.
# We use the `MultiVersionGC` for evicting unnecessary old versions.
# We use the `TimestampsManager` for assigning timestamps for transactions.
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

        # This list is used by the garbage-collection mechanism.
        # The GC needs it in order to efficiently find the youngest RO-transaction,
        # and to find the youngest older RO-transaction of another given RO-transaction.
        # Used by: `get_ongoing_youngest_read_only_transaction(..)`
        #          `get_ongoing_read_only_transaction_older_than(..)`
        # Maintained by `assign_timestamp_to_transaction(..)` and `on_transaction_removed(..)`.
        # We never traverse this list in O(n).
        self._ongoing_ro_transactions_sorted_by_timestamp = DoublyLinkedList()

    @property
    def locks_manager(self):
        return self._locks_manager

    @property
    def ongoing_ro_transactions_sorted_by_timestamp(self):
        return iter(self._ongoing_ro_transactions_sorted_by_timestamp)

    @property
    def mv_data_manager(self):
        return self._mv_data_manager

    def on_add_transaction(self, transaction: Transaction):
        if transaction.is_read_only:
            assert isinstance(transaction, ROMVTransaction)
            # We assign timestamp to RO transactions only when the first read
            # operation is being tried to be performed. We do so mainly in
            # order to easily support of sustain method for our user-simulator.
        else:
            assert isinstance(transaction, UMVTransaction)

    def on_transaction_removed(self, transaction: Transaction):
        if isinstance(transaction, ROMVTransaction):
            transaction_node = transaction.ro_transactions_sorted_by_timestamp_list_node
            assert transaction_node is not None
            self._ongoing_ro_transactions_sorted_by_timestamp.remove_node(transaction_node)

    def get_current_ts(self):
        return self._timestamps_manager.peek_next_ts()

    # Run over the transaction via the chosen scheduling scheme (serial / RR).
    # For each transaction encountered by that order, try to perform its next operation.
    # The call to `transaction.try_perform_next_operation()` invokes `next_operation.try_perform(..)`
    # which might invoke `scheduler.try_read(..)` or `scheduler.try_write(..)`.
    # The last mentioned methods are implemented below (in that class).
    # The main logics of the ROMV protocol is coded in these methods.
    # We use the `LocksManager` for managing locks over the variables (which uses the deadlock detector).
    def run(self, forced_run_order=None):
        for transaction in self.iterate_over_ongoing_transactions_and_safely_remove_marked_to_remove_transactions(
                forced_run_order):

            # Flag that indicates whether the current transaction had been serialized in the current iteration.
            # Used for printing the serialization log in the correct timing.
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

            # If the current transaction had been serialized in the current iteration - log it.
            if serialization_point_flag:
                Logger().log('     Serialization point. Timestamp: {ts}'.format(ts=transaction.timestamp),
                             log_type_name='serialization_point')

    def assign_timestamp_to_transaction(self, transaction: Transaction):
        transaction.timestamp = self._timestamps_manager.get_next_ts()
        self.serialization_point(transaction.transaction_id)
        if isinstance(transaction, ROMVTransaction):
            # Now we add this RO transaction to the tail of the RO-transactions list sorted by timestamps.
            # We also keep a pointer to the created node in this list, in order to efficiently traverse
            # the list starting from a given transaction, when needed.
            # the method `get_ongoing_read_only_transaction_older_than(..)` uses this.
            node = self._ongoing_ro_transactions_sorted_by_timestamp.push_back(transaction)
            transaction.ro_transactions_sorted_by_timestamp_list_node = node

    # Called by a write operation of a transaction, when `next_operation.try_perform(..)` is called
    # by its transaction (when the scheduler invokes `transaction.try_perform_next_operation(..)`.
    # Only update-transactions can invoke this method.
    # It actually enforces the SS2PL as the ROMV protocol describes, using the lock manager.
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
            # For simplicity, the chosen victim is the first that closed the deadlock-cycle.
            self.abort_transaction(transaction, DeadlockCycleAbortReason(collides_with__or__deadlock_cycle))
            assert not self._locks_manager.is_deadlock()
            return False  # failed
        elif got_lock == 'WAIT':
            transaction.waits_for = collides_with__or__deadlock_cycle
            return False  # failed

        assert got_lock == 'GOT_LOCK'
        transaction.local_write(variable, value)
        return True  # succeed writing the new value

    # Called by a read operation of a transaction, when `next_operation.try_perform(..)` is called
    # by its transaction (when the scheduler invokes `transaction.try_perform_next_operation(..)`.
    # For update-transactions, this method enforces the SS2PL as the ROMV protocol describes,
    # using the lock manager.
    # For read-only transaction, just ask the multi-version data manager for the correct version
    # of the given variable.
    def try_read(self, transaction_id, variable):
        transaction = self.get_transaction_by_id(transaction_id)
        assert transaction is not None
        if transaction.is_read_only:
            assert isinstance(transaction, ROMVTransaction)
            value = self._mv_data_manager.read_older_version_than(variable, transaction.timestamp)
            assert value is not None
            return value

        assert isinstance(transaction, UMVTransaction)
        got_lock, collides_with__or__deadlock_cycle = self._locks_manager.try_acquire_lock(transaction_id, variable, 'read')
        if got_lock == 'DEADLOCK':
            # Invariant: At any point in time, there is no cycle in the "wait-for" graph.
            #            Each time a transaction attempts to lock a variable, the conflict graph
            #            is updated. When a conflict cycle is firstly created it is immediately
            #            detected and then immediately broken by the scheduler.
            # For simplicity, the chosen victim is the first that closed the deadlock-cycle.
            self.abort_transaction(transaction, DeadlockCycleAbortReason(collides_with__or__deadlock_cycle))
            assert not self._locks_manager.is_deadlock()
            return None  # failed
        elif got_lock == 'WAIT':
            assert isinstance(collides_with__or__deadlock_cycle, set)
            transaction.waits_for = collides_with__or__deadlock_cycle
            return None  # failed

        assert got_lock == 'GOT_LOCK'
        value = transaction.local_read(variable)
        if value is not None:
            return value
        value = self._mv_data_manager.read_latest_version(variable)
        assert value is not None
        return value

    # Called by `try_read(..)` or `try_write(..)`.
    # Notice: Keep in mind that it has been called while iterating over the transactions
    # from inside of the `run(..)` method (of this class).
    def abort_transaction(self, transaction: Transaction, reason):
        assert not transaction.is_read_only
        assert isinstance(transaction, UMVTransaction)
        self._locks_manager.release_all_locks(transaction.transaction_id)
        self.mark_transaction_to_remove(transaction)
        # Notice: The call to `transaction.abort(..)` invokes the user-callback that might
        # add a new transaction with the same transaction id as of the aborted transaction.
        # It is ok because we already marked the aborted transaction to remove, and by side
        # effect it has been removed from the ongoing_transactions_mapping.
        # Additionally, note that the potentially new added transaction (with the same
        # transaction id) has been added before the aborted transaction in the transactions
        # list sorted by transaction_id. So that the iteration in `run(..)` won't encounter
        # this transaction again until next loop (in RR scheduling scheme).
        transaction.abort(self, reason)
        # Notice: We currently storing the updates locally on the transaction itself only.
        #         If we choose to write to disk, we'll have to undo the transaction here.

    # For the GC mechanism, when a read-only transaction commits, we need to find the
    # youngest older reader of the just-committed reader, in order to check whether
    # someone may need the versions that the just-committed reader is responsible for.
    # This method helps us finding this read-only transaction.
    # For more details, see under the implementation of the `MultiVersionGC`.
    def get_ongoing_read_only_transaction_older_than(self, ro_transaction: ROMVTransaction):
        assert ro_transaction.is_read_only and isinstance(ro_transaction, ROMVTransaction)
        assert ro_transaction.ro_transactions_sorted_by_timestamp_list_node is not None
        current_node = ro_transaction.ro_transactions_sorted_by_timestamp_list_node.prev_node
        while current_node is not None and current_node.data.is_finished:
            assert current_node.data.is_read_only
            assert current_node.data.has_timestamp
            current_node = current_node.prev_node
        assert current_node is None or current_node.data.is_read_only
        assert current_node is None or current_node.data.has_timestamp
        return None if current_node is None else current_node.data

    # For the GC mechanism, when an update transaction commits, we need to find the
    # youngest older reader of that updater, in order to check whether someone may
    # need the previous version of the same variable that the just-committed updater
    # committed. This method helps us finding this read-only transaction.
    # For more details, see under the implementation of the `MultiVersionGC`.
    def get_ongoing_youngest_read_only_transaction(self):
        for ro_transaction in reversed(self._ongoing_ro_transactions_sorted_by_timestamp):
            if not ro_transaction.is_finished:
                assert ro_transaction.is_read_only
                assert ro_transaction.has_timestamp
                return ro_transaction
        return None

    # Used for printing all of the variables to the run-log for debugging purposes.
    def get_variables(self):
        # Notice: We currently storing the updates locally on the transaction itself only.
        #         If we choose to write to disk, the below operations are not relevant.
        variables = copy.deepcopy(dict(self._mv_data_manager.get_variables()))
        for transaction in self._ongoing_transactions_by_tid:
            if isinstance(transaction, ROMVTransaction):
                continue
            assert isinstance(transaction, UMVTransaction)
            if transaction.is_aborted or transaction.is_completed:  # TODO: check if it is ok.
                continue
            for var, new_uncommitted_version in transaction._local_written_values.items():
                if var not in variables:
                    variables[var] = []
                variables[var].append((new_uncommitted_version, 'uncommitted'))
        return variables.items()
