from operation import Operation, ReadOperation, WriteOperation
from user_simulator.transaction_parsing_patterns import TransactionParsingPatterns
from operation import Operation, ReadOperation, WriteOperation, CommitOperation
from typing import Optional
import regex  # for parsing the test file. we use `regex` rather than known `re` to support named groups in patterns.


# Operation simulator is responsible for storing an operation to perform.
# A simple `Operation` is not familiar with the concept of "local variables".
# Sometimes the next operation to perform might read a value from a local variable, or write a value to it.
# The inheritors `ReadOperationSimulator` and `WriteOperationSimulator` handle accessing these local variables.
# All of the local variable are stored by the `TransactionSimulator` as can be seen later.
# The `TransactionSimulator` may contain instances of kind `OperationSimulator`.
class OperationSimulator:
    def __init__(self, operation: Optional[Operation], operation_number: Optional[int]=None):
        self._operation = operation
        self._operation_number = operation_number

    @property
    def operation(self):
        return self._operation

    @property
    def operation_number(self):
        return self._operation_number

    @operation_number.setter
    def operation_number(self, op_num):
        assert not self._operation_number
        self._operation_number = op_num

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
    def __init__(self, operation: Operation, operation_number: Optional[int], dest_local_variable_name: str):
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
    def __init__(self, operation: Operation, operation_number: Optional[int], src_local_variable_name_or_const_val):
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
    def __init__(self, nr_yield_epochs: int, operation_number: Optional[int]):
        assert nr_yield_epochs >= 1
        super().__init__(None, operation_number)
        self._nr_yield_epochs = nr_yield_epochs
        self._nr_completed_yield_epochs = 0

    def next_epoch(self):
        assert self._nr_completed_yield_epochs < self._nr_yield_epochs
        self._nr_completed_yield_epochs += 1

    def should_still_yield(self):
        return self._nr_completed_yield_epochs < self._nr_yield_epochs

    def __str__(self):
        text = 'yield'
        if self._nr_yield_epochs > 1:
            text += '[{}/{}]'.format(self._nr_completed_yield_epochs, self._nr_yield_epochs)
        return text

    @staticmethod
    def compare_all(*operation_simulators):
        assert len(operation_simulators) > 1
        assert all(isinstance(operation_simulator, SuspendOperationSimulator)
                   for operation_simulator in operation_simulators)
        assert all(operation_simulator.operation is None
                   for operation_simulator in operation_simulators)


class OperationSimulatorParser:

    # Factory function. creates an `OperationSimulator` from a test line.
    @staticmethod
    def parse(operation_str: str, transaction_id):
        read_operation_parser = regex.compile('^' + TransactionParsingPatterns.read_operation_pattern + '$')
        parsed_read_operation = read_operation_parser.match(operation_str)
        write_operation_parser = regex.compile('^' + TransactionParsingPatterns.write_operation_pattern + '$')
        parsed_write_operation = write_operation_parser.match(operation_str)
        commit_operation_parser = regex.compile('^' + TransactionParsingPatterns.commit_operation_pattern + '$')
        parsed_commit_operation = commit_operation_parser.match(operation_str)
        suspend_operation_parser = regex.compile('^' + TransactionParsingPatterns.suspend_operation_pattern + '$')
        parsed_suspend_operation = suspend_operation_parser.match(operation_str)

        number_of_parsed_op_types = bool(parsed_read_operation) + \
                                    bool(parsed_write_operation) + \
                                    bool(parsed_commit_operation) + \
                                    bool(parsed_suspend_operation)
        assert number_of_parsed_op_types == 1

        if parsed_read_operation:
            local_var_identifier = parsed_read_operation.capturesdict()['local_var_identifier']
            assert len(local_var_identifier) == 1
            local_var_identifier = local_var_identifier[0]

            var_identifier = parsed_read_operation.capturesdict()['var_identifier']
            assert len(var_identifier) == 1
            var_identifier = var_identifier[0]

            read_operation = ReadOperation(var_identifier)
            return ReadOperationSimulator(read_operation, None, local_var_identifier)

        elif parsed_write_operation:
            var_identifier = parsed_write_operation.capturesdict()['var_identifier']
            assert len(var_identifier) == 1
            var_identifier = var_identifier[0]

            write_value = parsed_write_operation.capturesdict()['write_value']
            assert len(write_value) == 1
            write_value = write_value[0]

            write_operation = WriteOperation(var_identifier)
            return WriteOperationSimulator(write_operation, None, write_value)

        elif parsed_commit_operation:
            tid = parsed_commit_operation.capturesdict()['transaction_id']
            assert len(tid) == 1
            tid = int(tid[0])
            assert tid == transaction_id

            commit_operation = CommitOperation()
            return CommitOperationSimulator(commit_operation)

        elif parsed_suspend_operation:
            nr_yield_epochs = 1
            if 'nr_yield_epochs' in parsed_suspend_operation.capturesdict() \
                    and len(parsed_suspend_operation.capturesdict()['nr_yield_epochs']) > 0:
                nr_yield_epochs = parsed_suspend_operation.capturesdict()['nr_yield_epochs']
                assert len(nr_yield_epochs) == 1
                nr_yield_epochs = int(nr_yield_epochs[0])
            return SuspendOperationSimulator(nr_yield_epochs, 0)

        assert False
