from abc import ABC, abstractmethod
from doubly_linked_list import DoublyLinkedList
from transaction import Transaction


# This is a pure-abstract class. It is inherited later by the `ROMVScheduler` and the `SerialScheduler`.
# The `SchedulerInterface` include basic generic methods for storing the transactions in lists and iterating
# over them. The `SchedulerInterface` defined an interface, so that any actual scheduler that inherits from
# `SchedulerInterface` must conform with that interface. Any scheduler must implement the methods:
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
class SchedulerInterface(ABC):
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
