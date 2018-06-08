from user_simulator.transaction_parsing_patterns import TransactionParsingPatterns
from user_simulator.operation_simulator import OperationSimulator, ReadOperationSimulator, \
    WriteOperationSimulator, CommitOperationSimulator, SuspendOperationSimulator
from operation import Operation, ReadOperation, WriteOperation, CommitOperation
from transaction import Transaction
from scheduler_interface import SchedulerInterface
from logger import Logger
import regex  # for parsing the test file. we use `regex` rather than known `re` to support named groups in patterns.
import copy  # for deep-coping the operation-simulators list, each transaction execution attempt.


# Used to print the status to output log.
class SchedulerExecutionLogger:
    @staticmethod
    def transaction_reset(transaction_simulator, abort_reason):
        reason = '   reason: ' + str(abort_reason) if Logger().is_log_type_set_on('deadlock_cycle') else ''
        Logger().log("{trans} RESET{abort_reason}".format(trans=transaction_simulator.to_log_str(),
                                                          abort_reason=reason))

    @staticmethod
    def transaction_action(transaction_simulator, operation_simulator):
        full_state = (' ' + transaction_simulator.to_full_state_str()
                      if Logger().is_log_type_set_on('transaction_state') else '')
        action_no = str(operation_simulator.operation_number)
        if not Logger().is_log_type_set_on('oded_style'):
            action_no = action_no.ljust(2)
        action_no = 'action ' + action_no if not Logger().is_log_type_set_on('transaction_state') else ''
        is_waiting = (operation_simulator.operation is not None and not operation_simulator.operation.is_completed)
        wait_str = 'WAIT' if Logger().is_log_type_set_on('transaction_state') else ' WAITING'
        Logger().log("{trans} {action_no}{waiting}{full_state}".format(
            trans=transaction_simulator.to_log_str(),
            action_no=action_no,
            waiting=(wait_str if is_waiting else ' ' * len(wait_str)),
            full_state=full_state))
        if is_waiting:
            Logger().log('     Waiting for locks from transactions: ' + str(transaction_simulator.transaction.waits_for),
                         log_type_name='wait_for')

    @staticmethod
    def print_variables(scheduler: SchedulerInterface):
        Logger().log('Variables: {}'.format(list(scheduler.get_variables())),
                     log_type_name='variables')


# Simulate the execution of a transaction.
# Stores all of the local variables that can be accessed by the operation-simulators.
# After each operation completes, the `on_complete_callback` will be called (by the scheduler),
# and the next operation to perform would be added by the `TransactionSimulator`.
# When the `SchedulerInterface` encounters a deadlock, it chooses a victim transaction and aborts it.
# We should "reset" a transaction. Means, if it is aborted we should try to execute it all over again.
# Prima facie, we could think to add a transaction "reset" feature to the SchedulerInterface module itself.
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
        self._completed_operation_simulators_queue = None
        self._execution_attempt_no = 0  # incremented each time adding the transaction to a scheduler.

    def reset_simulator(self):
        # The following fields will be initialized later when adding the transaction to a scheduler.
        # It might happen multiple times, because we support "reset"ing a transaction in a case of
        # abortion by the scheduler.
        self._transaction = None
        self._local_variables = None
        self._ongoing_operation_simulators_queue = None
        self._completed_operation_simulators_queue = None
        self._execution_attempt_no = 0  # incremented each time adding the transaction to a scheduler.

    def clone(self):
        new_transaction_simulator = TransactionSimulator(self.transaction_id, self._is_read_only)
        new_transaction_simulator._all_operation_simulators = copy.deepcopy(self._all_operation_simulators)
        return new_transaction_simulator

    @property
    def transaction(self):
        return self._transaction

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

        elif parsed_suspend_operation:
            nr_yield_epochs = 1
            if 'nr_yield_epochs' in parsed_suspend_operation.capturesdict()\
                    and len(parsed_suspend_operation.capturesdict()['nr_yield_epochs']) > 0:
                nr_yield_epochs = parsed_suspend_operation.capturesdict()['nr_yield_epochs']
                assert len(nr_yield_epochs) == 1
                nr_yield_epochs = int(nr_yield_epochs[0])
            self.add_suspend_operation_simulator(nr_yield_epochs)

    @property
    def transaction_id(self):
        return self._transaction_id

    @property
    def execution_attempt_number(self):
        return self._execution_attempt_no

    def create_transaction(self, scheduler: SchedulerInterface):
        assert self._transaction is None
        # Use automatic variable `me` to be captured by the lambda functions. Maybe we could just
        # use `self` in the lambda functions. I didn't want to take the chance it might be wrong.
        me = self
        TransactionType = scheduler.ROTransaction if self._is_read_only else scheduler.UTransaction
        self._transaction = TransactionType(
            self._transaction_id,
            is_read_only=self._is_read_only,
            on_operation_complete_callback=lambda *args: me.operation_completed(*args),
            on_operation_failed_callback=lambda *args: me.operation_failed(*args),
            on_transaction_aborted_callback=lambda *args: me.transaction_aborted(*args),
            ask_user_for_next_operation_callback=lambda *args: me.scheduler_asked_for_next_operation(*args))

    def add_transaction_to_scheduler(self, scheduler):
        assert self._transaction is None
        self._execution_attempt_no += 1
        self._local_variables = dict()
        self._ongoing_operation_simulators_queue = copy.deepcopy(self._all_operation_simulators)
        self._completed_operation_simulators_queue = []
        self.create_transaction(scheduler)
        # Add the first operation to the transaction, so the transaction won't be empty.
        # self.add_next_operation_to_transaction_if_needed()
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
        operation_simulator = CommitOperationSimulator(commit_operation, len(self._all_operation_simulators) + 1)
        self._all_operation_simulators.append(operation_simulator)

    def add_suspend_operation_simulator(self, nr_yield_epochs: int):
        operation_simulator = SuspendOperationSimulator(nr_yield_epochs, len(self._all_operation_simulators) + 1)
        self._all_operation_simulators.append(operation_simulator)

    # Called when the transaction waiting queue is empty the scheduler asks for the next operation to perform.
    def add_next_operation_to_transaction_if_needed(self):
        assert self._transaction is not None
        if len(self._ongoing_operation_simulators_queue) < 1:
            return False
        next_operation_simulator = self._ongoing_operation_simulators_queue[0]

        # For suspend operation simulator, we do not add any operation to the transaction.
        if isinstance(next_operation_simulator, SuspendOperationSimulator):
            next_operation_simulator.next_epoch()
            if not next_operation_simulator.should_still_yield():
                self.operation_completed(self._transaction, None, None)
            else:
                SchedulerExecutionLogger.transaction_action(self, next_operation_simulator)
            return False

        assert next_operation_simulator.operation is not None
        if next_operation_simulator.operation.get_type() == 'write':
            value_to_write = next_operation_simulator.get_value_to_write(self._local_variables)
            next_operation_simulator.operation.to_write_value = value_to_write
        self._transaction.add_operation(next_operation_simulator.operation)
        return True

    def operation_completed(self, transaction: Transaction, scheduler: SchedulerInterface, operation: Operation):
        assert len(self._ongoing_operation_simulators_queue) > 0

        operation_simulator = self._ongoing_operation_simulators_queue[0]  # this should be the next awaiting operation
        assert(operation == operation_simulator.operation)

        if isinstance(operation_simulator, ReadOperationSimulator):
            assert operation is not None and operation.get_type() == 'read'
            assert isinstance(operation, ReadOperation)
            dest_local_var_name = operation_simulator.dest_local_variable_name
            self._local_variables[dest_local_var_name] = operation.read_value

        # print to execution log!
        SchedulerExecutionLogger.transaction_action(self, operation_simulator)

        if scheduler is not None:
            SchedulerExecutionLogger.print_variables(scheduler)

        popped_operation_simulator = self._ongoing_operation_simulators_queue.pop(0)  # remove list head
        assert popped_operation_simulator == operation_simulator
        self._completed_operation_simulators_queue.append(operation_simulator)

        # Note: We no longer add the next operation whenever an operation is finished.

    def operation_failed(self, transaction: Transaction, scheduler: SchedulerInterface, operation: Operation):
        assert not operation.is_completed
        assert len(self._ongoing_operation_simulators_queue) > 0
        operation_simulator = self._ongoing_operation_simulators_queue[0]
        assert(operation == operation_simulator.operation)

        # print to execution log!
        SchedulerExecutionLogger.transaction_action(self, operation_simulator)
        SchedulerExecutionLogger.print_variables(scheduler)

    def transaction_aborted(self, transaction: Transaction, scheduler: SchedulerInterface, abort_reason):
        assert self._transaction is not None
        assert self._transaction.is_aborted

        # print to execution log!
        SchedulerExecutionLogger.transaction_reset(self, abort_reason)
        SchedulerExecutionLogger.print_variables(scheduler)

        self.reset_transaction(scheduler)

    def scheduler_asked_for_next_operation(self, transaction: Transaction, scheduler: SchedulerInterface):
        assert self._transaction is not None
        assert transaction == self._transaction
        self.add_next_operation_to_transaction_if_needed()

    def to_full_state_str(self):
        max_operation_str_size = 11
        completed_operations = (('   ' + str(transaction_simulator).ljust(max_operation_str_size)
                                 for transaction_simulator
                                 in self._completed_operation_simulators_queue))
        next_awaiting_operations = (str(transaction_simulator).ljust(max_operation_str_size)
                                    for transaction_simulator
                                    in self._ongoing_operation_simulators_queue)
        return ''.join(completed_operations) + ' > ' + '   '.join(next_awaiting_operations)

    def to_log_str(self):
        if Logger().is_log_type_set_on('oded_style'):
            return self._to_log_str_oded_style()
        return self._to_log_str_alternative_style()

    def _to_log_str_alternative_style(self):
        execution_attempt_number_str = '     '
        if self._execution_attempt_no > 1:
            execution_attempt_number_str = ' (#{})'.format(self._execution_attempt_no)
        return "Transaction {transaction_id} [{is_ro}]{execution_attempt_number}".format(
            transaction_id=self._transaction_id,
            is_ro=('-R-' if self._is_read_only else '*U*'),
            execution_attempt_number=execution_attempt_number_str)

    def _to_log_str_oded_style(self):
        execution_attempt_number_str = ''
        if self._execution_attempt_no > 1:
            execution_attempt_number_str = '({})'.format(self._execution_attempt_no)
        return "Transaction {transaction_id}{is_ro}{execution_attempt_number}".format(
            transaction_id=self._transaction_id,
            is_ro=('R' if self._is_read_only else 'U'),
            execution_attempt_number=execution_attempt_number_str)

    class NotEqualException(ValueError):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

    @staticmethod
    def compare_all(*transaction_simulators):
        assert len(transaction_simulators) > 1
        assert all(isinstance(transaction_simulator, TransactionSimulator)
                   for transaction_simulator in transaction_simulators)
        if any(transaction_simulator._completed_operation_simulators_queue is None
               for transaction_simulator in transaction_simulators):
            raise TransactionSimulator.NotEqualException(
                'At least one of the transaction simulators has no active running simulation.')
        if any(len(transaction_simulator._ongoing_operation_simulators_queue) > 0
               for transaction_simulator in transaction_simulators):
            raise TransactionSimulator.NotEqualException(
                'At least one of the transaction simulators has uncompleted ongoing operations.')
        if len(set((len(transaction_simulator._completed_operation_simulators_queue)
                    for transaction_simulator in transaction_simulators))) > 1:
            raise TransactionSimulator.NotEqualException(
                'Not all transaction simulators have the same number of completed operation simulators.')
        for operation_simulators in zip(*(transaction_simulator._completed_operation_simulators_queue
                                          for transaction_simulator in transaction_simulators)):
            OperationSimulator.compare_all(*operation_simulators)
