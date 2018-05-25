from abc import ABC, abstractmethod
from collections import namedtuple
from doubly_linked_list import DoublyLinkedList

OPERATION_TYPES = {'read', 'write', 'commit'}  # FIXME: do we allow `abort`?
VARIABLE_OPERATION_TYPES = {'read', 'write'}


# This is a pure abstract class for an operation (one cannot make an instance of `Operation`).
# It is inherited later on by `WriteOperation`, `ReadOperation`, and `CommitOperation`.
# As can be seen later, each Transaction may contain operations.
# Transaction-ID can be assigned once in a life of an operation object,
# and it should be done by the containing transaction (when adding the operation to the transaction).
# Operation is born un-completed. It may become completed after calling `try_perform(..)`.
# The method `try_perform(..)` is implemented by the inheritor classes.
class Operation(ABC):
    def __init__(self, operation_type, variable=None):
        assert operation_type in OPERATION_TYPES
        assert operation_type not in VARIABLE_OPERATION_TYPES or variable is not None
        self._transaction_id = None
        self._operation_type = operation_type
        self._variable = variable
        self._is_completed = False

    def get_type(self):
        return self._operation_type

    @property
    def transaction_id(self):
        assert self._transaction_id is not None
        return self._transaction_id

    @transaction_id.setter
    def transaction_id(self, transaction_id):
        assert self._transaction_id is None
        self._transaction_id = transaction_id

    @property
    def variable(self):
        assert self._operation_type in VARIABLE_OPERATION_TYPES
        return self._variable

    @property
    def is_completed(self):
        return self._is_completed

    # Each inherited operation type (write/read/commit) must override & implement this method.
    @abstractmethod
    def try_perform(self, scheduler):
        ...


class WriteOperation(Operation):
    def __init__(self, variable, to_write_value=None):
        super().__init__(operation_type='write',
                         variable=variable)
        self._to_write_value = to_write_value

    @property
    def to_write_value(self):
        assert self._to_write_value is not None
        return self._to_write_value

    @to_write_value.setter
    def to_write_value(self, to_write_value):
        assert self._to_write_value is None
        self._to_write_value = to_write_value

    def try_perform(self, scheduler):
        assert not self._is_completed
        succeed = scheduler.try_write(self.transaction_id, self.variable, self._to_write_value)
        self._is_completed = succeed
        return succeed


class ReadOperation(Operation):
    def __init__(self, variable):
        super().__init__(operation_type='read',
                         variable=variable)
        self._read_value = None  # Only for `read` operations. After a value has been read it would be assigned here.

    @property
    def read_value(self):
        assert self._read_value is not None
        assert self._is_completed
        return self._read_value

    @read_value.setter
    def read_value(self, value):
        assert self._read_value is None
        assert not self._is_completed
        self._read_value = value

    def try_perform(self, scheduler):
        assert not self._is_completed
        read_value = scheduler.try_read(self.transaction_id, self.variable)
        if read_value is None:
            return False
        self._is_completed = True
        self._read_value = read_value
        return True


class CommitOperation(Operation):
    def __init__(self):
        super().__init__(operation_type='commit')

    def try_perform(self, scheduler):
        assert not self._is_completed
        # TODO: impl - what should we do here actually?
        # answer : update the versions with a new one that was written in that operation
        self._is_completed = True
        return True


# We could create our own type. In real life case, we should create our own type so it would deal with overflows.
Timestamp = int
Timespan = namedtuple('Timespan', ['from_ts', 'to_ts'])


class TimestampsManager:
    def __init__(self):
        self._last_timestamp = Timestamp()

    def get_next_ts(self):
        self._last_timestamp += 1
        return self._last_timestamp

    def peek_next_ts(self):
        return self._last_timestamp + 1


# Transaction is the API between the scheduler and the user.
# It allows the user to add operations to it, using `add_operation(..)` method.
# The scheduler calls the method `try_perform_next_operation(..)` when it decides to.
# When the scheduler calls this method, the transaction tells the next operation to
# try perform itself, using the method `next_operation.try_perform(..)`.
# It the next operation successfully performed itself, the transaction would remove
# this operation from the `_waiting_operations_queue`.
# After each time the scheduler tries to execute the next operation (using the above
# mentioned method), a users' callback is called. If the operation has been successfully
# completed, the callback `on_operation_complete_callback(..)` is called.
# Otherwise, the callback `on_operation_failed_callback(..)` is called.
# These users' callbacks are set on the transaction creation.
class Transaction:
    def __init__(self, transaction_id, is_read_only: bool=False,
                 on_operation_complete_callback=None,
                 on_operation_failed_callback=None,
                 on_transaction_aborted_callback=None,
                 ask_user_for_next_operation_callback=None):
        self._transaction_id = transaction_id
        self._is_read_only = is_read_only
        self._waiting_operations_queue = []  # list of instances of `Operation`.
        self._is_completed = False
        self._is_aborted = False
        self._timestamp = None

        # To be called after an operation has been completed.
        self._on_operation_complete_callback = on_operation_complete_callback
        # To be called after an operation has failed (and now waiting till next attempt) due to locks.
        self._on_operation_failed_callback = on_operation_failed_callback
        # To be called after a transaction has been aborted by the scheduler.
        self._on_transaction_aborted_callback = on_transaction_aborted_callback
        # To be called by the scheduler when the waiting operations queue is empty and
        # `has_waiting_operation_to_perform()` is called (it is called by the scheduler inside of `run()`.
        self._ask_user_for_next_operation_callback = ask_user_for_next_operation_callback

        # Transactions are stored in a list, stored by transaction id, so that the scheduler can
        # iterate over the transactions by the order of their transaction id.
        # Besides the above mentioned list, there are also 3 other lists sorted by arrival time:
        #   (1) all transactions list ; (2) read-only transactions list ; (3) update transactions list.
        # Each transaction stores a pointer to its own node in each list, so that given a transaction
        # we could find efficiently the next & previous transactions in each list the transaction
        # belongs to.
        self.transactions_by_tid_list_node = None
        self.transactions_by_arrival_list_node = None
        self.ro_transactions_by_arrival_list_node = None
        self.u_transactions_by_arrival_list_node = None

        # When a transaction tries to perform its operation, and the scheduler cannot acquire locks
        # because of other transactions, it assigns to this public field the set of transactions
        # that the current transaction is waiting for. It is used later in the run-log printings.
        self.waits_for = None

    @property
    def transaction_id(self):
        return self._transaction_id

    @property
    def is_read_only(self):
        return self._is_read_only

    @property
    def is_completed(self):
        return self._is_completed

    @property
    def is_aborted(self):
        return self._is_aborted

    @property
    def is_finished(self):
        return self._is_aborted or self._is_completed

    @property
    def timestamp(self):
        assert self._timestamp is not None
        return self._timestamp

    @timestamp.setter
    def timestamp(self, ts: Timestamp):
        assert self._timestamp is None  # can be set only once in a life of a transaction.
        self._timestamp = ts

    @property
    def has_timestamp(self):
        return self._timestamp is not None

    def peek_next_operation(self):
        assert len(self._waiting_operations_queue) > 0
        return self._waiting_operations_queue[0]

    def ask_user_for_next_operation(self, scheduler):
        if self._ask_user_for_next_operation_callback is not None:
            self._ask_user_for_next_operation_callback(self, scheduler)

    def has_waiting_operation_to_perform(self, scheduler):
        if len(self._waiting_operations_queue) < 1:
            self.ask_user_for_next_operation(scheduler)
        return len(self._waiting_operations_queue) > 0

    # Returns the operation that we tried to perform (if exists one in the waiting queue).
    # If there is no operation in the operations waiting queue, ask the user for one.
    # To check whether it has been performed, use `operation.is_completed`.
    def try_perform_next_operation(self, scheduler):
        assert len(self._waiting_operations_queue) > 0

        self.waits_for = None

        next_operation = self._waiting_operations_queue[0]
        next_operation.try_perform(scheduler)

        if self.is_aborted:
            # TODO: what to do here?
            return next_operation

        if not next_operation.is_completed:
            self._on_operation_failed_callback(self, scheduler, next_operation)
            return next_operation

        queue_head = self._waiting_operations_queue.pop(0)  # remove the list head
        assert queue_head == next_operation
        if next_operation.get_type() == 'commit':
            self._is_completed = True
        if self._on_operation_complete_callback:
            # The user callback might now add the next operation.
            self._on_operation_complete_callback(self, scheduler, next_operation)
        return next_operation

    # called by the scheduler after removing this transaction from it's transactions list.
    def abort(self, scheduler, reason):
        assert not self._is_aborted and not self._is_completed
        assert self._transaction_id is not None
        assert scheduler.get_transaction_by_id(self._transaction_id) is None
        self._is_aborted = True
        # TODO: what else should be done here?
        if self._on_transaction_aborted_callback:
            self._on_transaction_aborted_callback(self, scheduler, reason)

    def add_operation(self, operation: Operation):
        assert not self._is_read_only or operation.get_type() != 'write'
        self._waiting_operations_queue.append(operation)  # FIXME: verify append adds in the end?
        operation.transaction_id = self.transaction_id


# This is a pure-abstract class. It is inherited later by the `ROMVScheduler` and the `SerialScheduler`.
# The `Scheduler` include basic generic methods for storing the transactions in lists and iterating
# over them. The `Scheduler` defined an interface, so that any actual scheduler that inherits from
# `Scheduler` must conform with that interface. Any scheduler must implement the methods:
#   run()
#       It iterates over that transactions and tries to perform their first awaiting operation.
#       The method `iterate_over_transactions_by_tid_and_safely_remove_marked_to_remove_transactions()`
#       of the scheduler might be handy here.
#   try_write(transaction_id, variable, value)
#       When the scheduler tries to perform a "write" operation (of a transaction), the operation
#       receives a pointer to the scheduler. The method `operation.try_perform(scheduler)` might
#       call the method `scheduler.try_write(..)`. This method might succeed of fail. If it fails
#       it means it couldn't acquire the necessary locks for that transactions, and the operation
#       should wait until these desired locks will be released. That mechanism allows the scheduler
#       to enforce its own locking mechanism.
#   try_read(transaction_id, variable)
#       Similar to the explanation above. Except that here the scheduler might choose to handle
#       differently read-only transactions.
class Scheduler(ABC):
    SchedulingSchemes = {'RR', 'serial'}
    ROTransaction = Transaction
    UTransaction = Transaction

    def __init__(self, scheduling_scheme='serial'):
        # The `scheduling_scheme` affects the order of the iteration over transactions
        # as enforced by the ongoing transactions iterator. The method `run()` is using
        # that iterator. More explanation about the ongoing transactions iterator later.
        assert(scheduling_scheme in self.SchedulingSchemes)
        self._scheduling_scheme = scheduling_scheme

        # Note: All of the lists mentioned below keep a track only for ongoing transactions.
        # Committed and aborted transactions are eventually removed from these data structures.

        # Transactions are stored in a list, stored by transaction id, so that the scheduler can
        # iterate over the transactions by the order of their transaction id.
        self._ongoing_transactions_by_tid = DoublyLinkedList()

        # Besides the above mentioned list, there are also 3 other lists sorted by arrival time:
        #   (1) all transactions list ; (2) read-only transactions list ; (3) update transactions list.
        # These lists will be used for garbage-collection operations.
        self._ongoing_transactions_by_arrival = DoublyLinkedList()
        self._ongoing_ro_transactions_by_arrival = DoublyLinkedList()
        self._ongoing_u_transactions_by_arrival = DoublyLinkedList()

        # During the main iteration over the transactions (inside of the `run()` method), the scheduler
        # might decide to remove a transaction (if it commits or aborts). However, it is not safe to
        # remove the transaction from the list while iterating over it. We solve this issue by only
        # marking the transaction as "to be removed", while iterating over the list. Later, in the end
        # of each loop over the transactions, we perform the actual removal by calling the method
        # `remove_marked_to_remove_transactions()`. It is done in the iterator as described later on.
        self._to_remove_transactions = DoublyLinkedList()

        # Mapping from transaction id to the matching transaction instance. Keeps a track only for
        # ongoing transactions. Committed and aborted transactions are removed from this mapping.
        # The operations stores the transaction_id they belongs to, but now the transaction instance.
        # In the `operation.try_perform()` we might need to retrieve the transaction instance.
        # It is done by calling `scheduler.get_transaction_by_id(tid)` that is using this mapping.
        self._ongoing_transactions_mapping = dict()

        # Each time a transaction reaches its serialization point, the method `serialization_point()` is called,
        # with the serialized transaction id, and this transaction id is added to this list.
        self._serialization_order = []

    @property
    def scheduling_scheme(self):
        return self._scheduling_scheme

    def add_transaction(self, transaction: Transaction):
        assert not transaction.is_completed and not transaction.is_aborted
        assert transaction.transaction_id not in self._ongoing_transactions_mapping

        # Add the transaction to the correct place in the transactions list
        # sorted by transaction id.
        insert_after_transaction = self._find_transaction_with_maximal_tid_lower_than(transaction.transaction_id)
        insert_after_transaction_node = None
        if insert_after_transaction is not None:
            insert_after_transaction_node = insert_after_transaction.transactions_by_tid_list_node
        node = self._ongoing_transactions_by_tid.insert_after_node(transaction, insert_after_transaction_node)
        transaction.transactions_by_tid_list_node = node

        # Add the transaction to the end of the transactions list sorted by arrival.
        node = self._ongoing_transactions_by_arrival.push_back(transaction)
        transaction.transactions_by_arrival_list_node = node

        if transaction.is_read_only:
            node = self._ongoing_ro_transactions_by_arrival.push_back(transaction)
            transaction.ro_transactions_by_arrival_list_node = node
        else:
            node = self._ongoing_u_transactions_by_arrival.push_back(transaction)
            transaction.u_transactions_by_arrival_list_node = node

        # Add the new transaction to the mapping by transaction-id.
        self._ongoing_transactions_mapping[transaction.transaction_id] = transaction

        # The method `on_add_transaction` might be overridden by the inheritor scheduler,
        # if it has to do something each time a transaction is added. Calling it here is
        # the mechanism to notify the inheritor scheduler that a transaction has been added.
        self.on_add_transaction(transaction)

    # TODO: doc!
    def mark_transaction_to_remove(self, transaction: Transaction):
        assert transaction.transaction_id in self._ongoing_transactions_mapping
        del self._ongoing_transactions_mapping[transaction.transaction_id]
        self._to_remove_transactions.push_back(transaction)

    # TODO: doc!
    def remove_marked_to_remove_transactions(self):
        for transaction_to_remove in self._to_remove_transactions:
            self.remove_transaction(transaction_to_remove)
        self._to_remove_transactions.clear()

    # TODO: doc!
    def remove_transaction(self, transaction_to_remove: Transaction):
        self._ongoing_transactions_by_tid.remove_node(transaction_to_remove.transactions_by_tid_list_node)
        self._ongoing_transactions_by_arrival.remove_node(transaction_to_remove.transactions_by_arrival_list_node)
        if transaction_to_remove.is_read_only:
            self._ongoing_ro_transactions_by_arrival.remove_node(transaction_to_remove.ro_transactions_by_arrival_list_node)
        else:
            self._ongoing_u_transactions_by_arrival.remove_node(transaction_to_remove.u_transactions_by_arrival_list_node)
        # TODO: doc the following check.
        if transaction_to_remove.transaction_id in self._ongoing_transactions_mapping and \
                self._ongoing_transactions_mapping[transaction_to_remove.transaction_id] is transaction_to_remove:
            self._ongoing_transactions_mapping.pop(transaction_to_remove.transaction_id)

    def get_transaction_by_id(self, transaction_id):
        if transaction_id in self._ongoing_transactions_mapping:
            return self._ongoing_transactions_mapping[transaction_id]
        return None

    # When adding a new transaction, we want to inset it inside of the ongoing transactions list
    # sorted by tid, in the right position. In order to do so, we need to find this correct position.
    # This method help us finding this position in the list to insert after it.
    def _find_transaction_with_maximal_tid_lower_than(self, transaction_id):
        prev_transaction = None
        for transaction in self._ongoing_transactions_by_tid:
            if transaction.transaction_id >= transaction_id:
                break
            prev_transaction = transaction
        return prev_transaction

    # This is a generator function. It returns an iterator. The returned iterator yields a
    # a transaction in each iteration. The iterator is fulfilled when no more ongoing
    # transactions exist. The iteration scheme can be either 'RR' (round-robin) based, or
    # serial based. It is determined by the scheduling-scheme that is set in the c'tor.
    # It should be used in the implementation of the method "run()", where it is needed to
    # iterate over the ongoing transactions by their transaction id and try to perform their
    # awaiting operations, until no more active transactions remains.
    # Usage example: `for transaction in self.iterate_over_transactions_...(): <for loop body>`
    # Notice that while the iteration the scheduler might decide to remove a transaction (if it
    # commits or aborts). However, it is not safe to remove the transaction from the list while
    # iterating over it. We solve this issue by only marking the transaction as "to be removed",
    # while iterating over the list. Later, in the end of each loop over the transactions, we
    # here perform the actual removal by calling `remove_marked_to_remove_transactions()`.
    def iterate_over_transactions_by_tid_and_safely_remove_marked_to_remove_transactions(self):
        while len(self._ongoing_transactions_by_tid) > 0:
            if self._scheduling_scheme == 'RR':
                for transaction in self._ongoing_transactions_by_tid:
                    if transaction.is_aborted or transaction.is_completed:
                        continue
                    yield transaction
            elif self._scheduling_scheme == 'serial':
                transaction = self._ongoing_transactions_by_tid.peek_front()
                if not transaction.is_completed and not transaction.is_aborted:
                    yield transaction
            else:
                raise ValueError('Invalid scheduling scheme `{}`.'.format(self._scheduling_scheme))
            # Deletion cannot be performed inside the iteration over the transactions list.
            # Hence, we only mark transactions for deletion while iterating, and on the end
            # of each outer loop we remove all of the transaction that are marked to be removed.
            self.remove_marked_to_remove_transactions()

    # Each time a transaction reaches its serialization point, the method `serialization_point()` is called with
    # the serialized transaction id.
    def serialization_point(self, transaction_id):
        self._serialization_order.append(transaction_id)

    def get_serialization_order(self):
        return iter(self._serialization_order)

    # Called by the scheduler each time the method `add_transaction()` is called.
    # Might be overridden by the inheritor scheduler, if it has to do something
    # each time a transaction is added.
    @abstractmethod
    def on_add_transaction(self, transaction: Transaction):
        pass  # default implementation - do nothing

    # Called by the user. Perform transactions until no transactions left.
    # Must be implemented by the inheritor scheduler.
    @abstractmethod
    def run(self):
        ...

    # Called by an operation of a transaction, when `next_operation.try_perform(..)` is called by its transaction.
    # Must be implemented by the inheritor scheduler.
    @abstractmethod
    def try_write(self, transaction_id, variable, value):
        ...

    # Called by an operation of a transaction, when `next_operation.try_perform(..)` is called by its transaction.
    # Must be implemented by the inheritor scheduler.
    @abstractmethod
    def try_read(self, transaction_id, variable):
        ...

    # May be called by the user in order to iterate over the values of the variables in the DB.
    # Should return an iterable type of pairs (variable_name, latest_value) or (variable_name, list_of_versions)
    # We use it in order to print values in debug mode.
    @abstractmethod
    def get_variables(self):
        ...


# For the GC mechanism we sometimes have to store a set of variables. We see later
# why and how we do it. The catch is that we only relay on the No-False-Negative
# property of that variables set. Here we just show a naive implementation of such
# set data structure. In real-life we might want to use a more space-efficient data
# structure instead. The bloom-filter data structure is an example for one. More
# details about it under the documentation of the garbage-collector implementation
# (in "romv_scheduler.py").
class NoFalseNegativeVariablesSet:
    def __init__(self):
        self._variables_set = set()

    def add_variable(self, variable):
        self._variables_set.add(variable)

    def add_variables(self, variables_set_to_add):
        assert isinstance(variables_set_to_add, NoFalseNegativeVariablesSet)
        self._variables_set = self._variables_set.union(variables_set_to_add._variables_set)

    def is_variable_exists(self, variable):
        return variable in self._variables_set

    def intersection(self, another_variables_set):
        assert isinstance(another_variables_set, NoFalseNegativeVariablesSet)
        new_variables_set = NoFalseNegativeVariablesSet()
        new_variables_set._variables_set = self._variables_set.intersection(another_variables_set._variables_set)
        return new_variables_set

    def difference(self, another_variables_set):
        assert isinstance(another_variables_set, NoFalseNegativeVariablesSet)
        new_variables_set = NoFalseNegativeVariablesSet()
        new_variables_set._variables_set = self._variables_set.difference(another_variables_set._variables_set)
        return new_variables_set

    def might_be_not_empty(self):
        return len(self._variables_set) > 0

    def __iter__(self):
        return iter(self._variables_set)
