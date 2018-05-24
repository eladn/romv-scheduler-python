import regex  # for parsing the test file. we use `regex` rather than known `re` to support named groups in patterns.
import copy  # for deep-coping the operation-simulators list, each transaction execution attempt.
from scheduler_base_modules import Scheduler, Transaction, Operation, WriteOperation, ReadOperation, CommitOperation
from logger import Logger


# Used to print the status to output log.
class SchedulerExecutionLogger:
    @staticmethod
    def transaction_reset(transaction_simulator):
        Logger().log("{trans} reset".format(trans=transaction_simulator.to_log_str()))

    @staticmethod
    def transaction_action(transaction_simulator, operation_simulator):
        Logger().log("{trans} action {action_no}{waiting}".format(
            trans=transaction_simulator.to_log_str(),
            action_no=operation_simulator.operation_number,
            waiting=('' if operation_simulator.operation.is_completed else ' WAITING')))

    @staticmethod
    def print_variables(scheduler: Scheduler):
        Logger().log('Variables: {}'.format(list(scheduler.get_variables())),
                     log_type_name='scheduler_simulator_verbose')


# Operation simulator is responsible for storing an operation to perform.
# A simple `Operation` is not familiar with the concept of "local variables".
# Sometimes the next operation to perform might read a value from a local variable, or write a value to it.
# The inheritors `ReadOperationSimulator` and `WriteOperationSimulator` handle accessing these local variables.
# All of the local variable are stored by the `TransactionSimulator` as can be seen later.
# The `TransactionSimulator` may contain instances of kind `OperationSimulator`.
class OperationSimulator:
    def __init__(self, operation: Operation, operation_number: int):
        self._operation = operation
        self._operation_number = operation_number

    @property
    def operation(self):
        return self._operation

    @property
    def operation_number(self):
        return self._operation_number


class ReadOperationSimulator(OperationSimulator):
    def __init__(self, operation: Operation, operation_number: int, dest_local_variable_name: str):
        super().__init__(operation, operation_number)
        self._dest_local_variable_name = dest_local_variable_name

    @property
    def dest_local_variable_name(self):
        return self._dest_local_variable_name


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


# Collection of patterns used for parsing the input workload test file.
# Used by the method: TransactionSimulator.parse_transaction_from_test_line(..)
# Used by the method: TransactionSimulator.parse_and_add_operation(..)
class TransactionParsingPatterns:
    number_pattern = '(([1-9][0-9]*)|0)'
    identifier_pattern = '[a-zA-Z_][a-zA-Z_0-9]*'
    const_value_pattern = number_pattern
    local_var_identifier_pattern = '(?P<local_var_identifier>' + identifier_pattern + ')'
    var_identifier_pattern = '(?P<var_identifier>' + identifier_pattern + ')'
    optional_line_comment_pattern = '(?P<line_comment>((\/\/)|(\#)).*)?'

    comment_line_pattern = '^[\s]*' + optional_line_comment_pattern + '$'

    scheduling_scheme_num = '(?P<scheduling_scheme_num>[12])'
    num_of_transactions = '(?P<num_of_transactions>' + number_pattern + ')'
    test_first_line_pattern = '^' + scheduling_scheme_num + \
                              '[\s]+' + num_of_transactions + \
                              '[\s]*' + optional_line_comment_pattern + '$'

    transaction_type_pattern = '(?P<transaction_type>[UR])'
    transaction_id_pattern = '(?P<transaction_id>' + number_pattern + ')'
    transaction_header_pattern = transaction_type_pattern + '[\s]+' + \
                                 transaction_id_pattern + '[\s]+' + \
                                 '(?P<transaction_operations>.*)\;'
    transaction_line_pattern = '^' + transaction_header_pattern + \
                               '[\s]*' + optional_line_comment_pattern + '$'

    write_value_pattern = '(?P<write_value>' + local_var_identifier_pattern + '|' + const_value_pattern + ')'
    write_operation_pattern = '(w\(' + var_identifier_pattern + '[\s]*\,[\s]*' + write_value_pattern + '\))'
    read_operation_pattern = '(' + local_var_identifier_pattern + \
                             '[\s]*\=[\s]*' + \
                             'r\(' + var_identifier_pattern + '\))'
    commit_operation_pattern = '(c' + transaction_id_pattern + ')'
    operation_pattern = '(?P<operation>(' + read_operation_pattern + '|' + write_operation_pattern + \
                        '|' + commit_operation_pattern + '))'
    operations_pattern = '(' + operation_pattern + '[\s]+)*'


# Simulate the execution of a transaction.
# Stores all of the local variables that can be accessed by the operation-simulators.
# After each operation completes, the `on_complete_callback` will be called (by the scheduler),
# and the next operation to perform would be added by the `TransactionSimulator`.
# When the `Scheduler` encounters a deadlock, it chooses a victim transaction and aborts it.
# We should "reset" a transaction. Means, if it is aborted we should try to execute it all over again.
# Prima facie, we could think to add a transaction "reset" feature to the Scheduler module itself.
# In reality it makes no sense to do so, because the read operations might read other values in two different
# executions. Hence, local-variables (which are not known by the scheduler but only known to the user),
# might get other realizations during the two executions. Hence, the operations that the user create and add
# to the transaction might be different in the two executions.
# In conclusion, we understand that the "reset" feature must be supported by the `TransactionSimulator`, that
# simulates the user. It means that the `TransactionSimulator` would have to store all of the operation-simulators,
# so that they could be used again in a case of aborting a transaction.
class TransactionSimulator:
    def __init__(self, transaction_id, is_read_only: bool):
        self._transaction_id = transaction_id
        self._is_read_only = is_read_only
        self._all_operation_simulators = []

        # The following fields will be initialized later when adding the transaction to a scheduler.
        # It might happen multiple times, because we support "reset"ing a transaction in a case of
        # abortion by the scheduler.
        self._transaction = None
        self._local_variables = None
        self._ongoing_operation_simulators_queue = None
        self._execution_attempt_no = 0  # incremented each time adding the transaction to a scheduler.

    # Factory function. creates a `TransactionSimulator` from a test line.
    @staticmethod
    def parse_transaction_from_test_line(transaction_line: str):
        transaction_line = transaction_line.strip()
        transaction_line_parser = regex.compile(TransactionParsingPatterns.transaction_line_pattern)
        parsed_transaction_line = transaction_line_parser.match(transaction_line)

        assert parsed_transaction_line
        assert len(parsed_transaction_line.capturesdict()['transaction_id']) == 1
        transaction_id = int(parsed_transaction_line.capturesdict()['transaction_id'][0])
        assert len(parsed_transaction_line.capturesdict()['transaction_type']) == 1
        transaction_type = parsed_transaction_line.capturesdict()['transaction_type'][0]
        assert transaction_type in {'R', 'U'}
        is_read_only = (transaction_type == 'R')

        assert len(parsed_transaction_line.capturesdict()['transaction_operations']) == 1
        operations_str = parsed_transaction_line.capturesdict()['transaction_operations'][0].strip()
        operations_str += ' '  # the operations parser expects each operation to terminate with following spaces.

        transaction_simulator = TransactionSimulator(transaction_id, is_read_only)

        operations_parser = regex.compile('^' + TransactionParsingPatterns.operations_pattern + '$')
        parsed_operations = operations_parser.match(operations_str)
        assert parsed_operations
        for operation_str in parsed_operations.capturesdict()['operation']:
            assert operation_str
            transaction_simulator.parse_and_add_operation(operation_str)

        return transaction_simulator

    def parse_and_add_operation(self, operation_str):
        read_operation_parser = regex.compile('^' + TransactionParsingPatterns.read_operation_pattern + '$')
        parsed_read_operation = read_operation_parser.match(operation_str)
        write_operation_parser = regex.compile('^' + TransactionParsingPatterns.write_operation_pattern + '$')
        parsed_write_operation = write_operation_parser.match(operation_str)
        commit_operation_parser = regex.compile('^' + TransactionParsingPatterns.commit_operation_pattern + '$')
        parsed_commit_operation = commit_operation_parser.match(operation_str)

        number_of_parsed_op_types = bool(parsed_read_operation) + \
                                    bool(parsed_write_operation) + \
                                    bool(parsed_commit_operation)
        assert number_of_parsed_op_types == 1

        if parsed_read_operation:
            local_var_identifier = parsed_read_operation.capturesdict()['local_var_identifier']
            assert len(local_var_identifier) == 1
            local_var_identifier = local_var_identifier[0]

            var_identifier = parsed_read_operation.capturesdict()['var_identifier']
            assert len(var_identifier) == 1
            var_identifier = var_identifier[0]

            read_operation = ReadOperation(var_identifier)
            self.add_read_operation_simulator(read_operation, local_var_identifier)

        elif parsed_write_operation:
            var_identifier = parsed_write_operation.capturesdict()['var_identifier']
            assert len(var_identifier) == 1
            var_identifier = var_identifier[0]

            write_value = parsed_write_operation.capturesdict()['write_value']
            assert len(write_value) == 1
            write_value = write_value[0]

            write_operation = WriteOperation(var_identifier)
            self.add_write_operation_simulator(write_operation, write_value)

        elif parsed_commit_operation:
            transaction_id = parsed_commit_operation.capturesdict()['transaction_id']
            assert len(transaction_id) == 1
            transaction_id = int(transaction_id[0])
            assert transaction_id == self.transaction_id

            self.add_commit_operation_simulator(CommitOperation())

    @property
    def transaction_id(self):
        return self._transaction_id

    @property
    def execution_attempt_number(self):
        return self._execution_attempt_no

    def create_transaction(self, scheduler: Scheduler):
        assert self._transaction is None
        # Use automatic variable `me` to be captured by the lambda functions. Maybe we could just
        # use `self` in the lambda functions. I didn't want to take the chance it might be wrong.
        me = self
        TransactionType = scheduler.ROTransaction if self._is_read_only else scheduler.UTransaction
        self._transaction = TransactionType(self._transaction_id,
                                            is_read_only=self._is_read_only,
                                            on_operation_complete_callback=lambda *args: me.operation_completed(*args),
                                            on_operation_failed_callback=lambda *args: me.operation_failed(*args),
                                            on_transaction_aborted_callback=lambda *args: me.transaction_aborted(*args))

    def add_transaction_to_scheduler(self, scheduler):
        assert self._transaction is None
        self._execution_attempt_no += 1
        self._local_variables = dict()
        self._ongoing_operation_simulators_queue = copy.deepcopy(self._all_operation_simulators)
        self.create_transaction(scheduler)
        # Add the first operation to the transaction, so the transaction won't be empty.
        self.add_next_operation_to_transaction_if_needed()
        scheduler.add_transaction(self._transaction)

    def reset_transaction(self, scheduler):
        assert scheduler.get_transaction_by_id(self._transaction.transaction_id) is None
        assert self._transaction is not None
        assert self._transaction.is_aborted
        self._transaction = None
        self.add_transaction_to_scheduler(scheduler)

    def add_write_operation_simulator(self, write_operation: WriteOperation, src_variable_name_or_const_val):
        assert self._execution_attempt_no == 0 and self._transaction is None
        operation_simulator = WriteOperationSimulator(write_operation,
                                                      len(self._all_operation_simulators) + 1,
                                                      src_variable_name_or_const_val)
        self._all_operation_simulators.append(operation_simulator)

    def add_read_operation_simulator(self, read_operation: ReadOperation, dest_variable_name):
        assert self._execution_attempt_no == 0 and self._transaction is None
        operation_simulator = ReadOperationSimulator(read_operation,
                                                     len(self._all_operation_simulators) + 1,
                                                     dest_variable_name)
        self._all_operation_simulators.append(operation_simulator)

    def add_commit_operation_simulator(self, commit_operation: CommitOperation):
        assert self._execution_attempt_no == 0 and self._transaction is None
        operation_simulator = OperationSimulator(commit_operation, len(self._all_operation_simulators) + 1)
        self._all_operation_simulators.append(operation_simulator)

    def add_next_operation_to_transaction_if_needed(self):
        assert self._transaction is not None
        if len(self._ongoing_operation_simulators_queue) < 1:
            return
        next_operation_simulator = self._ongoing_operation_simulators_queue[0]
        if next_operation_simulator.operation.get_type() == 'write':
            value_to_write = next_operation_simulator.get_value_to_write(self._local_variables)
            next_operation_simulator.operation.to_write_value = value_to_write
        self._transaction.add_operation(next_operation_simulator.operation)

    def operation_completed(self, transaction: Transaction, scheduler: Scheduler, operation: Operation):
        assert len(self._ongoing_operation_simulators_queue) > 0
        operation_simulator = self._ongoing_operation_simulators_queue.pop(0)  # remove list head
        assert(operation == operation_simulator.operation)
        if operation.get_type() == 'read':
            assert isinstance(operation, ReadOperation)
            dest_local_var_name = operation_simulator.dest_local_variable_name
            self._local_variables[dest_local_var_name] = operation.read_value
        self.add_next_operation_to_transaction_if_needed()

        # print to execution log!
        SchedulerExecutionLogger.transaction_action(self, operation_simulator)
        SchedulerExecutionLogger.print_variables(scheduler)

    def operation_failed(self, transaction: Transaction, scheduler: Scheduler, operation: Operation):
        assert not operation.is_completed
        assert len(self._ongoing_operation_simulators_queue) > 0
        operation_simulator = self._ongoing_operation_simulators_queue[0]
        assert(operation == operation_simulator.operation)

        # print to execution log!
        SchedulerExecutionLogger.transaction_action(self, operation_simulator)
        SchedulerExecutionLogger.print_variables(scheduler)

    def transaction_aborted(self, transaction: Transaction, scheduler: Scheduler):
        assert self._transaction is not None
        assert self._transaction.is_aborted

        # print to execution log!
        SchedulerExecutionLogger.transaction_reset(self)
        SchedulerExecutionLogger.print_variables(scheduler)

        self.reset_transaction(scheduler)

    def to_log_str(self):
        execution_attempt_number_str = ''
        if self._execution_attempt_no > 1:  # FIXME: should we always add the attempt number?
            execution_attempt_number_str = '({})'.format(self._execution_attempt_no)
        return "Transaction {transaction_id}{is_ro}{execution_attempt_number}".format(
            transaction_id=self._transaction_id,
            is_ro=('R' if self._is_read_only else 'U'),
            execution_attempt_number=execution_attempt_number_str)


# Parse the input test file and add transactions and their operations to the given scheduler.
class TransactionsWorkloadSimulator:
    def __init__(self, verbose=False):
        self._transaction_simulators = []
        self._transaction_id_to_transaction_simulator = dict()  # FIXME: maybe we don't need it
        self._schedule = 'RR'
        if not verbose:
            Logger().turn_off('scheduler_simulator_verbose')

    @property
    def schedule(self):
        return self._schedule

    # Parse the test file and add its contents to the simulator.
    def load_test_data(self, workload_data_filename):
        with open(workload_data_filename, 'r') as test_file:

            # Read comment lines before the test first line.
            test_first_line = TransactionsWorkloadSimulator._read_first_line_that_is_not_comment(test_file)

            # Parse test first line (scheduling scheme, number of transactions).
            test_first_line = test_first_line.strip()
            test_first_line_parser = regex.compile(TransactionParsingPatterns.test_first_line_pattern)
            parsed_test_first_line = test_first_line_parser.match(test_first_line)
            assert parsed_test_first_line
            assert len(parsed_test_first_line.capturesdict()['num_of_transactions']) == 1
            num_of_transactions = int(parsed_test_first_line.capturesdict()['num_of_transactions'][0])
            assert len(parsed_test_first_line.capturesdict()['scheduling_scheme_num']) == 1
            scheduling_scheme_num = int(parsed_test_first_line.capturesdict()['scheduling_scheme_num'][0])
            assert scheduling_scheme_num in {1, 2}
            self._schedule = 'serial' if (scheduling_scheme_num == 1) else 'RR'

            # Note: each transaction is promised to be contained in its own single line.
            for transaction_line in test_file:
                transaction_line = transaction_line.strip()
                if not transaction_line:
                    continue  # ignore a blank line
                if TransactionsWorkloadSimulator._is_comment_line(transaction_line):
                    continue
                transaction_simulator = TransactionSimulator.parse_transaction_from_test_line(transaction_line)
                self._transaction_simulators.append(transaction_simulator)
                self._transaction_id_to_transaction_simulator[transaction_simulator.transaction_id] = transaction_simulator
            assert num_of_transactions == len(self._transaction_simulators)

    @staticmethod
    def _is_comment_line(test_line):
        comment_line_parser = regex.compile(TransactionParsingPatterns.comment_line_pattern)
        parsed_comment_line = comment_line_parser.match(test_line)
        return parsed_comment_line is not None

    @staticmethod
    def _read_first_line_that_is_not_comment(test_file):
        for line in test_file:
            if not TransactionsWorkloadSimulator._is_comment_line(line):
                return line
        return None

    # Given an scheduler, initiate the transactions (except for T0) in the scheduler.
    # For each transaction, add the first operation to it.
    # For each transaction, add a callback to be called by the scheduler after an operation has been completed,
    # so that the matching TransactionSimulator would insert the next operation.
    def add_workload_to_scheduler(self, scheduler: Scheduler):
        for transaction_simulator in self._transaction_simulators:
            # FIXME: do we want to just skip the first transaction?
            if transaction_simulator.transaction_id == 0:
                continue
            transaction_simulator.add_transaction_to_scheduler(scheduler)

    def add_initialization_transaction_to_scheduler(self, scheduler: Scheduler):
        # FIXME: do we want to just detect it as first transaction?
        initialization_transaction = self._transaction_id_to_transaction_simulator[0]
        initialization_transaction.add_transaction_to_scheduler(scheduler)
