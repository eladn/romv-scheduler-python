from collections import defaultdict
from deadlock_detector import DeadlockDetector
from logger import Logger


class LocksManager:

    class TransactionLocksSet:
        def __init__(self):
            self.read_locks = set()
            self.write_locks = set()

    def __init__(self):
        self._deadlock_detector = DeadlockDetector()

        # Mapping from transaction-id to the locked variables this transaction holds.
        # { transaction_id: TransactionLocksSet }.
        self._transactions_locks_sets = defaultdict(LocksManager.TransactionLocksSet)

        # Mapping from variable to a set of transactions that
        # hold a read lock for this variable.
        # { variable: set(transaction_id) }
        self._read_locks_table = defaultdict(set)

        # Mapping from variable to the transaction-id of the (single)
        # transaction that holds a write lock for this variable.
        # { variable: transaction_id }
        self._write_locks_table = dict()

    @property
    def deadlock_detector(self):
        return self._deadlock_detector

    # Returns a set of transaction-ids of the transactions that hold locks
    # that collide with the given requested lock (variable, read_or_write).
    # If no such, returns None.
    def _collides_with(self, transaction_id, variable, read_or_write):
        assert read_or_write in {'read', 'write'}
        if variable in self._write_locks_table:
            assert variable not in self._read_locks_table.keys() \
                   or self._read_locks_table[variable] == {self._write_locks_table[variable]}
            if self._write_locks_table[variable] == transaction_id:
                return None
            return {self._write_locks_table[variable]}
        if variable in self._read_locks_table and read_or_write == 'write':
            readers_without_me = self._read_locks_table[variable].difference({transaction_id})
            if len(readers_without_me) > 0:
                return readers_without_me
        return None

    # Returns:
    #   "GOT_LOCK" the lock has been acquired successfully (or the transaction already holds the lock).
    #   "WAIT" is the lock could not be acquired now (someone else is holding it), but there is no dependency cycle.
    #   "DEADLOCK" if a deadlock will be created if the transaction would wait for this wanted lock.
    def try_acquire_lock(self, transaction_id, variable, read_or_write):
        assert read_or_write in {'read', 'write'}

        # Get the set of transaction-ids of the transactions that hold locks that collide
        # with the requested lock (variable, read_or_write).
        collides_with = self._collides_with(transaction_id, variable, read_or_write)

        # `collides_with` is None iff the lock can be acquired now.
        if collides_with is None:
            if read_or_write == 'read':
                # if the lock is already acquired - finish.
                if transaction_id in self._read_locks_table[variable]:
                    return 'GOT_LOCK', None

                self._read_locks_table[variable].add(transaction_id)
                self._transactions_locks_sets[transaction_id].read_locks.add(variable)

                Logger().log('     Locks: Transaction {tid} acquired read lock for variable `{variable}`.'.format(
                    tid=transaction_id, variable=variable), log_type_name='locks')
            else:
                assert variable not in self._write_locks_table or self._write_locks_table[variable] == transaction_id

                # if the lock is already acquired - finish.
                if variable in self._write_locks_table:
                    assert self._write_locks_table[variable] == transaction_id
                    return 'GOT_LOCK', None

                self._write_locks_table[variable] = transaction_id
                self._transactions_locks_sets[transaction_id].write_locks.add(variable)

                Logger().log('     Locks: Transaction {tid} acquired write lock for variable `{variable}`.'.format(
                    tid=transaction_id, variable=variable), log_type_name='locks')
            return 'GOT_LOCK', None

        # The lock cannot be acquired now. Lets chech whether waiting for it would
        # result in a deadlock.
        assert isinstance(collides_with, set) and len(collides_with) > 0
        for wait_for_tid in collides_with:
            deadlock_cycle_detected = self._deadlock_detector.wait_for(transaction_id, wait_for_tid)
            if deadlock_cycle_detected is not None:
                return 'DEADLOCK', deadlock_cycle_detected
        return 'WAIT', collides_with

    # When a transaction is done, we release all of its acquired locks and remove it from the "wait-for" graph.
    def release_all_locks(self, transaction_id):
        # remove the just-ended-transaction from the "wait-for" graph.
        self._deadlock_detector.transaction_ended(transaction_id)

        # remove the locks from the data structures: self._read_locks_table, self._write_locks_table
        for read_var in self._transactions_locks_sets[transaction_id].read_locks:
            assert read_var in self._read_locks_table
            assert transaction_id in self._read_locks_table[read_var]
            self._read_locks_table[read_var].remove(transaction_id)
            if len(self._read_locks_table[read_var]) == 0:
                self._read_locks_table.pop(read_var)

        for write_var in self._transactions_locks_sets[transaction_id].write_locks:
            assert write_var in self._write_locks_table
            assert transaction_id == self._write_locks_table[write_var]
            self._write_locks_table.pop(write_var)

        if len(self._transactions_locks_sets[transaction_id].read_locks) > 0:
            Logger().log('     Locks: Transaction {tid} released read locks for variables `{variables}`.'.format(
                tid=transaction_id,
                variables=str(self._transactions_locks_sets[transaction_id].read_locks)), log_type_name='locks')
        if len(self._transactions_locks_sets[transaction_id].write_locks) > 0:
            Logger().log('     Locks: Transaction {tid} released write locks for variables `{variables}`.'.format(
                tid=transaction_id,
                variables=str(self._transactions_locks_sets[transaction_id].write_locks)), log_type_name='locks')

        self._transactions_locks_sets.pop(transaction_id)

    # Returns whether there is a deadlock at the moment
    def is_deadlock(self):
        return self._deadlock_detector.find_deadlock_cycle() is not None

    def __str__(self):
        return 'read locks: ' + str(dict(self._read_locks_table)) + '   ;   write locks:' + str(self._write_locks_table)
