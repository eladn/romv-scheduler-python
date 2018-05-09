from abc import ABC, abstractmethod

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
        super().__init__(operation_type='write',
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
        self._is_completed = True
        return True


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
    def __init__(self, transaction_id, is_read_only: bool,
                 on_operation_complete_callback=None,
                 on_operation_failed_callback=None):
        self._transaction_id = transaction_id
        self._is_read_only = is_read_only
        self._waiting_operations_queue = []
        self._is_completed = False
        # To be called after an operation has been completed.
        self._on_operation_complete_callback = on_operation_complete_callback
        # To be called after an operation has failed (and now waiting till next attempt) due to locks.
        self._on_operation_failed_callback = on_operation_failed_callback

    @property
    def transaction_id(self):
        return self._transaction_id

    @property
    def is_read_only(self):
        return self._is_read_only

    @property
    def is_completed(self):
        return self._is_completed

    def peek_next_operation(self):
        assert len(self._waiting_operations_queue) > 0
        return self._waiting_operations_queue[0]

    def try_perform_next_operation(self, scheduler):
        assert len(self._waiting_operations_queue) > 0

        next_operation = self._waiting_operations_queue[0]
        next_operation.try_perform(scheduler)

        if not next_operation.is_completed:
            self._on_operation_failed_callback(self, scheduler, next_operation)
            return False

        queue_head = self._waiting_operations_queue.pop(index=0)
        assert queue_head == next_operation
        if next_operation.get_type() == 'commit':
            self._is_completed = True
        if self._on_operation_complete_callback:
            # The user callback might now add the next operation.
            self._on_operation_complete_callback(self, scheduler, next_operation)
        return True

    def add_operation(self, operation: Operation):
        assert not self._is_read_only or operation.get_type() != 'write'
        self._waiting_operations_queue.append(operation)  # FIXME: verify append adds in the end?
        operation.transaction_id = self.transaction_id


# This is a pure-abstract class. It is inherited later by the `ROMVScheduler` and the `SerialScheduler`.
class Scheduler(ABC):
    def __init__(self):
        pass

    # Called by the user.
    @abstractmethod
    def add_transaction(self, transaction: Transaction):
        ...

    # Called by the user. Perform transactions until no transactions left.
    @abstractmethod
    def run(self):
        ...

    # Called by an operation of a transaction, when `next_operation.try_perform(..)` is called by its transaction.
    @abstractmethod
    def try_write(self, transaction_id, variable, value):
        ...

    # Called by an operation of a transaction, when `next_operation.try_perform(..)` is called by its transaction.
    @abstractmethod
    def try_read(self, transaction_id, variable):
        ...
