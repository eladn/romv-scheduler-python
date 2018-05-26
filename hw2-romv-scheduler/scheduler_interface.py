from abc import ABC, abstractmethod
from doubly_linked_list import DoublyLinkedList
from transaction import Transaction
from itertools import chain


# This is a pure-abstract class. It is inherited later by the `ROMVScheduler` and the `SerialScheduler`.
# The `SchedulerInterface` include basic generic methods for storing the transactions in lists and iterating
# over them. The `SchedulerInterface` is defined an interface, so that any actual scheduler that inherits from
# `SchedulerInterface` must conform with that interface. Any scheduler must implement the following methods:
#   run()
#       It iterates over that transactions and tries to perform their first awaiting operation.
#       The method `iterate_over_ongoing_transactions_and_safely_remove_marked_to_remove_transactions()`
#       of the scheduler might be handy for the implementation of this method.
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

        # Add the new transaction to the mapping by transaction-id.
        self._ongoing_transactions_mapping[transaction.transaction_id] = transaction

        # The method `on_add_transaction` might be overridden by the inheritor scheduler,
        # if it has to do something each time a transaction is added. Calling it here is
        # the mechanism to notify the inheritor scheduler that a transaction has been added.
        self.on_add_transaction(transaction)

    # Part of the mechanism for transaction removal while iteration.
    # We cannot remove a transaction while iterating over the transactions list.
    # Hence, we just `mark` the transaction for removal while iterating.
    # We actually remove the marked-to-remove transactions from the transactions
    # list only after the end of the iteration.
    # The marking to removal is by adding the transaction to a dedicated list.
    # We also remove the transaction from the mapping here.
    def mark_transaction_to_remove(self, transaction: Transaction):
        assert transaction.transaction_id in self._ongoing_transactions_mapping
        del self._ongoing_transactions_mapping[transaction.transaction_id]
        self._to_remove_transactions.push_back(transaction)

    # Part of the mechanism for transaction removal while iteration.
    # Iterate over the list of marked-to-remove transactions, and perform the
    # removal of each one of these.
    def remove_marked_to_remove_transactions(self):
        for transaction_to_remove in self._to_remove_transactions:
            self.remove_transaction(transaction_to_remove)
        self._to_remove_transactions.clear()

    # Actual full removal of a transaction from the scheduler.
    # Don't use it while iterating over the transactions list
    # (in that case use `mark_transaction_to_remove()` instead).
    def remove_transaction(self, transaction_to_remove: Transaction):
        self._ongoing_transactions_by_tid.remove_node(transaction_to_remove.transactions_by_tid_list_node)

        # When a transaction is aborted, the user callback is balled. The user then might "reset" the transaction.
        # That is, the user might create and add a new transaction with the same transaction-id to the scheduler.
        # This creates a problem because the scheduler marks the aborted transaction to be removed.
        # Because of the mechanism for transaction removal while iteration, the actual removal will take place
        # only after the new transaction has been added by the user. This new added transaction might have the
        # same transaction-id as the already-marked-to-be-removed aborted transaction. Hence, we check here whether
        # there exists a new transaction with the same id. In that case, we do not remove it from the mapping.
        if transaction_to_remove.transaction_id in self._ongoing_transactions_mapping and \
                self._ongoing_transactions_mapping[transaction_to_remove.transaction_id] is transaction_to_remove:
            self._ongoing_transactions_mapping.pop(transaction_to_remove.transaction_id)

        # Tell the inheritor scheduler that the transaction had been removed.
        self.on_transaction_removed(transaction_to_remove)

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

    # This is a generator function. It returns an iterator. The returned iterator yields
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
    def iterate_over_ongoing_transactions_by_tid_and_safely_remove_marked_to_remove_transactions(self):
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

    # This is a generator function. It returns an iterator. The returned iterator yields
    # a transaction in each iteration, by the given order `forced_run_order`.
    # The parameter `forced_run_order` is an iterable sequence that each element is the
    # transaction id of the transaction to run next. Each transaction is being yielded
    # over-and-over again until it is completed. We assume this is used with the serial
    # scheduling scheme.
    def iterate_over_ongoing_transactions_by_given_order_and_safely_remove_marked_to_remove_transactions(
            self, forced_run_order):
        assert forced_run_order is not None
        forced_run_order = list(forced_run_order)
        for tid in forced_run_order:
            transaction = self.get_transaction_by_id(tid)
            assert transaction is not None
            while not transaction.is_completed:
                assert not transaction.is_aborted
                yield transaction
                self.remove_marked_to_remove_transactions()

    # Return one of the iterators above, based on whether the parameter `forced_run_order` is given.
    def iterate_over_ongoing_transactions_and_safely_remove_marked_to_remove_transactions(self, forced_run_order=None):
        if forced_run_order is None:
            return self.iterate_over_ongoing_transactions_by_tid_and_safely_remove_marked_to_remove_transactions()
        assert self._scheduling_scheme == 'serial'
        return self.iterate_over_ongoing_transactions_by_given_order_and_safely_remove_marked_to_remove_transactions(
            forced_run_order)

    # Each time a transaction reaches its serialization point, the method `serialization_point()` is called with
    # the serialized transaction id.
    def serialization_point(self, transaction_id):
        self._serialization_order.append(transaction_id)

    def get_serialization_order(self):
        return iter(self._serialization_order)

    # Being raised by the following method.
    class NotEqualException(ValueError):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

    # Given any number of schedulers as arguments, compare the latest values of each variable of these schedulers.
    # Firstly, compare that all of these schedulers have exactly the same variables names.
    # Secondary, for each variable, verify that all of the schedulers have the same latest version for that variable.
    @staticmethod
    def compare_results(*schedulers):
        assert all(isinstance(scheduler, SchedulerInterface) for scheduler in schedulers)
        assert len(schedulers) > 1

        # list that each element is the {variables: values} dict of a given scheduler.
        all_values_of_all_schedulers = list(dict(scheduler.get_variables()) for scheduler in schedulers)

        # list that each element is the set of variables names of a given scheduler.
        variable_names_per_scheduler = list(set(item[0] for item in variables_of_scheduler)
                                            for variables_of_scheduler in all_values_of_all_schedulers)
        # set of all variable names that occurred in a scheduler.
        variables_names_union = set(chain(*variable_names_per_scheduler))

        # Verify that all of the schedulers have the same variable names.
        if not all(variables_names_union == variable_names_of_scheduler
                   for variable_names_of_scheduler in variable_names_per_scheduler):
            raise SchedulerInterface.NotEqualException(
                'Not all schedulers have the same variable names.')

        # Verify that for each variable, all of the schedulers have the same value for that variable.
        for variable_name in variable_names_per_scheduler[0]:
            # set of the latest values that the current variable got in the schedulers.
            values = set(variables_of_scheduler[variable_name][-1][0]  # take the value of the last version
                         if isinstance(variables_of_scheduler[variable_name], list)
                         else variables_of_scheduler[variable_name]
                         for variables_of_scheduler
                         in all_values_of_all_schedulers)
            # all of the schedulers got the same latest value for `variable` iff the values set is of size 1.
            if len(values) != 1:
                raise SchedulerInterface.NotEqualException(
                    'Not all schedulers have the same values of variable {variable}.'.format(variable=variable_name))

    # Called by the scheduler each time the method `add_transaction()` is called.
    # Might be overridden by the inheritor scheduler, if it has to do something
    # each time a transaction is added.
    @abstractmethod
    def on_add_transaction(self, transaction: Transaction):
        pass  # default implementation - do nothing

    # Called by the scheduler each time the method `remove_transaction()` is called.
    # Might be overridden by the inheritor scheduler, if it has to do something
    # each time a transaction is removed.
    @abstractmethod
    def on_transaction_removed(self, transaction: Transaction):
        pass  # default implementation - do nothing

    # Called by the user. Perform transactions until no transactions left.
    # Must be implemented by the inheritor scheduler.
    @abstractmethod
    def run(self, forced_run_order=None):
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
