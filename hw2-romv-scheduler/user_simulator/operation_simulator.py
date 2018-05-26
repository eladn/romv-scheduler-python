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

    class NotEqualException(ValueError):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

    @staticmethod
    def compare_all(*operation_simulators):
        assert len(operation_simulators) > 1
        assert all(isinstance(operation_simulator, OperationSimulator)
                   for operation_simulator in operation_simulators)
        if len(set((type(operation_simulator)
                    for operation_simulator in operation_simulators))) > 1:
            raise OperationSimulator.NotEqualException(
                'Not all operation simulators are of the same type.')
        operation_simulator_type = type(operation_simulators[0])
        operation_simulator_type.compare_all(*operation_simulators)


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

    @staticmethod
    def compare_all(*operation_simulators):
        assert len(operation_simulators) > 1
        assert all(isinstance(operation_simulator, ReadOperationSimulator)
                   for operation_simulator in operation_simulators)
        assert all(operation_simulator.operation.is_completed
                   for operation_simulator in operation_simulators)
        if len(set(((operation_simulator._dest_local_variable_name,
                     operation_simulator.operation_number,
                     operation_simulator.operation.variable,
                     operation_simulator.operation.transaction_id)
                    for operation_simulator in operation_simulators))) > 1:
            raise OperationSimulator.NotEqualException(
                'Not all read operation simulators are identical.')
        if len(set((operation_simulator.operation.read_value
                    for operation_simulator in operation_simulators))) > 1:
            raise OperationSimulator.NotEqualException(
                'Read operation simulators: #{operation_number} transation id: {tid}. Not all read-operation-simulators read the same value. {values}'.format(
                    operation_number=operation_simulators[0].operation_number,
                    tid=operation_simulators[0].operation.transaction_id,
                    values=(', '.join(str(operation_simulator) for operation_simulator in operation_simulators))
                ))


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

    @staticmethod
    def compare_all(*operation_simulators):
        assert len(operation_simulators) > 1
        assert all(isinstance(operation_simulator, WriteOperationSimulator)
                   for operation_simulator in operation_simulators)
        assert all(operation_simulator.operation.is_completed
                   for operation_simulator in operation_simulators)
        if len(set(((operation_simulator._src_local_variable_name,
                     operation_simulator._const_val,
                     operation_simulator.operation_number,
                     operation_simulator.operation.variable,
                     operation_simulator.operation.transaction_id)
                    for operation_simulator in operation_simulators))) > 1:
            raise OperationSimulator.NotEqualException(
                'Not all write operation simulators are identical.')
        if len(set((operation_simulator.operation.to_write_value
                    for operation_simulator in operation_simulators))) > 1:
            raise OperationSimulator.NotEqualException(
                'Write operation simulators: #{operation_number} transation id: {tid}. Not all write-operation-simulators written the same value. {values}'.format(
                    operation_number=operation_simulators[0].operation_number,
                    tid=operation_simulators[0].operation.transaction_id,
                    values=(', '.join(str(operation_simulator) for operation_simulator in operation_simulators))
                ))


class CommitOperationSimulator(OperationSimulator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __str__(self):
        return 'commit'

    @staticmethod
    def compare_all(*operation_simulators):
        assert len(operation_simulators) > 1
        assert all(isinstance(operation_simulator, CommitOperationSimulator)
                   for operation_simulator in operation_simulators)
        assert all(operation_simulator.operation.is_completed
                   for operation_simulator in operation_simulators)


class SuspendOperationSimulator(OperationSimulator):
    def __init__(self, operation_number: int):
        super().__init__(None, operation_number)

    def __str__(self):
        return 'suspend'

    @staticmethod
    def compare_all(*operation_simulators):
        assert len(operation_simulators) > 1
        assert all(isinstance(operation_simulator, SuspendOperationSimulator)
                   for operation_simulator in operation_simulators)
        assert all(operation_simulator.operation is None
                   for operation_simulator in operation_simulators)
