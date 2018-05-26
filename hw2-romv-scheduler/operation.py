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
