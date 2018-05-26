from collections import defaultdict
from deadlock_detector import DeadlockDetector


class LocksManager:
    # TransactionLocksSet = namedtuple('TransactionLocksSet', ['read_locks', 'write_locks'])
    class TransactionLocksSet:
        def __init__(self):
            self.read_locks = set()
            self.write_locks = set()

    def __init__(self):
        self._deadlock_detector = DeadlockDetector()

        # locks table fields (read locks multiset + write locks set)
        # our multiset would be a dict ! for each key will count the amount of its duplications.
        # dict : key - transaction id, value - tuple(read,write)
        self._transactions_locks_sets = defaultdict(LocksManager.TransactionLocksSet)  # { transaction_id: TransactionLocksSet }
        # the read locks table : the key is the variable to lock
        #                        the value is a set of who locks it at the moment
        self._read_locks_table = defaultdict(set)
        # the write locks table : the key is the variable to lock
        #                         the value is what transaction currently locks the variable
        self._write_locks_table = dict()

    def _collides_with(self, transaction_id, variable, read_or_write):
        assert read_or_write in {'read', 'write'}
        if variable in self._write_locks_table:
            assert len(self._read_locks_table[variable]) == 0 \
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
    #   "GOT_LOCK" the lock has been acquired successfully (or the transaction also has the lock).
    #   "WAIT" is the lock could not be acquired now (someone else is holding it, but there is no dependency cycle).
    #   "DEADLOCK" if a deadlock will be created if the transaction would wait for this wanted lock.
    def try_acquire_lock(self, transaction_id, variable, read_or_write):
        assert read_or_write in {'read', 'write'}
        collides_with = self._collides_with(transaction_id, variable, read_or_write)
        if collides_with is None:
            if read_or_write == 'read':
                self._read_locks_table[variable].add(transaction_id)
                self._transactions_locks_sets[transaction_id].read_locks.add(variable)
            else:
                assert variable not in self._write_locks_table or self._write_locks_table[variable] == transaction_id
                self._write_locks_table[variable] = transaction_id
                self._transactions_locks_sets[transaction_id].write_locks.add(variable)
            return 'GOT_LOCK', None
        assert isinstance(collides_with, set) and len(collides_with) > 0
        for wait_for_tid in collides_with:
            deadlock_cycle_detected = self._deadlock_detector.wait_for(transaction_id, wait_for_tid)
            if deadlock_cycle_detected is not None:
                return 'DEADLOCK', deadlock_cycle_detected
        return 'WAIT', collides_with

    # when transaction_id is done, we release all of its acquired locks and remove it from the "wait-for" graph.
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

        self._transactions_locks_sets.pop(transaction_id)

    # returns if there is a deadlock at the moment
    def is_deadlock(self):
        return self._deadlock_detector.find_deadlock_cycle() is not None
