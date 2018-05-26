from timestamps_manager import Timestamp
from operation import Operation


# Transaction is the main API between the scheduler and the user.
# It allows the user to add operations to it, using `add_operation(..)` method.
# The scheduler calls the method `try_perform_next_operation(..)` when it decides to.
# When the scheduler calls this method, the transaction tells the next operation to
# try perform itself, using the method `next_operation.try_perform(..)`.
# If the next operation successfully performed itself, the transaction would remove
# this operation from the `_waiting_operations_queue`.
# After each time the scheduler tries to execute the next operation (using the above
# mentioned method), a users' callback is called. If the operation has been successfully
# completed, the callback `on_operation_complete_callback(..)` is called.
# Otherwise, the callback `on_operation_failed_callback(..)` is called.
# When `try_perform_next_operation(..)` is called (by the scheduler) but the queue
# `_waiting_operations_queue` is empty, the scheduler calls to the user callback
# `_ask_user_for_next_operation_callback(..)`. It gives the user an opportunity to add
# the next operation for that transaction. However, the user does not have to do so.
# The user can also take advantage of the callback `on_operation_complete_callback(..)`
# in order to add the next operation to be performed.
# All of these mentioned users' callbacks are set on the transaction creation.
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
        # Each transaction stores a pointer to its own node in the list, so that given a transaction
        # we could find efficiently the next & previous transactions in the list the transaction
        # belongs to.
        self.transactions_by_tid_list_node = None

        # When a transaction tries to perform its operation, and the scheduler cannot acquire locks
        # because of other transactions, it assigns to this public field the set of transactions
        # that the current transaction is waiting for. It is used later in the run-log printings.
        # The user can access this field later.
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

        # Reset the "wait_for" field. It might be set by the scheduler if the operation
        # would fail to perform because it waits for another operation. In that case
        # the scheduler would assign to this field a set of transaction we wait for.
        self.waits_for = None

        next_operation = self._waiting_operations_queue[0]
        next_operation.try_perform(scheduler)

        # The scheduler might abort the transaction when `scheduler.try_write(..)`
        # calls `scheduler.try_write(..)` or `scheduler.try_read(..)`.
        # In that case, `scheduler.try_write(..)` would return and we would each here.
        # We should just return. The caller scheduler would continue handle this case.
        if self.is_aborted:
            return next_operation

        # The call to `next_operation.try_perform(..)` has failed, bacauuse it
        # called one of `scheduler.try_write(..)` or `scheduler.try_read(..)`,
        # and this inner call has been failed (for example: couldn't acquire lock).
        if not next_operation.is_completed:
            if self._on_operation_failed_callback:
                self._on_operation_failed_callback(self, scheduler, next_operation)
            return next_operation

        # The operation has been completed. Remove it from the waiting queue.
        queue_head = self._waiting_operations_queue.pop(0)  # remove the list head
        assert queue_head == next_operation

        if next_operation.get_type() == 'commit':
            self._is_completed = True
        if self._on_operation_complete_callback:
            # The user callback might now add the next operation.
            self._on_operation_complete_callback(self, scheduler, next_operation)

        return next_operation

    # Called by the scheduler when it decides to abort this transaction.
    # Notice: Called by the scheduler after removing this transaction from it's transactions list.
    # It might be called in the following trace:
    #   transaction.try_perform_next_operation(..)
    #   next_operation.try_perform(..)
    #   scheduler.try_write(..)
    #   transaction.abort(..)
    def abort(self, scheduler, reason):
        assert not self._is_aborted and not self._is_completed
        assert self._transaction_id is not None
        assert scheduler.get_transaction_by_id(self._transaction_id) is None
        self._is_aborted = True
        # Call the user callback.
        if self._on_transaction_aborted_callback:
            self._on_transaction_aborted_callback(self, scheduler, reason)

    # Called by the user.
    def add_operation(self, operation: Operation):
        assert not self._is_read_only or operation.get_type() != 'write'
        self._waiting_operations_queue.append(operation)
        operation.transaction_id = self.transaction_id
