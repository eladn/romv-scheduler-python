from operation import Operation, ReadOperation, WriteOperation
from typing import Optional


# Operation simulator is responsible for storing an operation to perform.
# A simple `Operation` is not familiar with the concept of "local variables".
# Sometimes the next operation to perform might read a value from a local variable, or write a value to it.
# The inheritors `ReadOperationSimulator` and `WriteOperationSimulator` handle accessing these local variables.
# All of the local variable are stored by the `TransactionSimulator` as can be seen later.
# The `TransactionSimulator` may contain instances of kind `OperationSimulator`.
class OperationSimulator:
    def __init__(self, operation: Optional[Operation], operation_number: int):
        self._operation = operation
        self._operation_number = operation_number

    @property
    def operation(self):
        return self._operation

    @property
    def operation_number(self):
        return self._operation_number

    def __str__(self):
        return 'unknown-operation'


class ReadOperationSimulator(OperationSimulator):
    def __init__(self, operation: Operation, operation_number: int, dest_local_variable_name: str):
        super().__init__(operation, operation_number)
        self._dest_local_variable_name = dest_local_variable_name

    @property
    def dest_local_variable_name(self):
        return self._dest_local_variable_name

    def __str__(self):
        assert isinstance(self.operation, ReadOperation)
        ret_str = self._dest_local_variable_name + '=r(' + self.operation.variable + ')'
        if self.operation.is_completed:
            ret_str += '=' + self.operation.read_value
        return ret_str


class WriteOperationSimulator(OperationSimulator):
    def __init__(self, operation: Operation, operation_number: int, src_local_variable_name_or_const_val):
        super().__init__(operation, operation_number)
        self._src_local_variable_name = None
        self._const_val = None
        potential_identifier = str(src_local_variable_name_or_const_val).strip()
        if potential_identifier.isidentifier():
            self._src_local_variable_name = potential_identifier
        else:
            self._const_val = src_local_variable_name_or_const_val  # TODO: do we want to cast the value to int() ?

    def get_value_to_write(self, local_variables):
        if self._const_val is not None:
            assert self._src_local_variable_name is None
            return self._const_val
        assert self._src_local_variable_name is not None
        return local_variables[self._src_local_variable_name]

    def __str__(self):
        assert isinstance(self.operation, WriteOperation)
        src_value_to_write = self._src_local_variable_name if self._const_val is None else self._const_val
        actual_value_written = ''
        if self._const_val is None and self.operation.is_completed:
            actual_value_written = '=' + self.operation.to_write_value
        ret_str = 'w(' + self.operation.variable + ', ' + src_value_to_write + actual_value_written + ')'
        return ret_str


class CommitOperationSimulator(OperationSimulator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __str__(self):
        return 'commit'


class SuspendOperationSimulator(OperationSimulator):
    def __init__(self, operation_number: int):
        super().__init__(None, operation_number)

    def __str__(self):
        return 'suspend'
