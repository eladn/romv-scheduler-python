
from collections import namedtuple
from scheduler_modules import Scheduler, Transaction, Operation, WriteOperation, ReadOperation, CommitOperation


# Used to print the status to output log.
class SchedulerExecutionLogger:
    pass


OperationSimulator = namedtuple('OperationSimulator', ['operation', 'local_variable'])


# Simulate the execution of a transaction.
class TransactionSimulator:
    def __init__(self, transaction_id, is_read_only: bool):
        me = self
        self._transaction = Transaction(transaction_id,
                                        is_read_only,
                                        on_operation_complete_callback=lambda *args: me.operation_completed(*args),
                                        on_operation_failed_callback=lambda *args: me.operation_failed(*args))
        self._local_variables = dict()
        self._operation_simulators = []

    # Factory function. creates a `TransactionSimulator` from a test line.
    @staticmethod
    def parse_transaction_from_test_line(transaction_line: str):
        # TODO: parse the given `transaction_line`
        transaction_id = 0  # TODO: parse from `transaction_line`
        is_read_only = False  # TODO: parse from `transaction_line`
        transaction_simulator = TransactionSimulator(transaction_id, is_read_only)

        # TODO: parse transaction operations from line.
        # For each operation use: transaction_simulator.add_operation_simulator(operation, local_variable)

        return transaction_simulator

    @property
    def transaction_id(self):
        return self._transaction.transaction_id

    def add_transaction_to_scheduler(self, scheduler):
        scheduler.add_transaction(self._transaction)

    def add_operation_simulator(self, operation: Operation, local_variable):
        operation_simulator = OperationSimulator(operation=operation, local_variable=local_variable)
        self._operation_simulators.append(operation_simulator)

    def add_next_operation_to_transaction(self):
        next_operation_simulator = self._operation_simulators[0]
        if next_operation_simulator.operation == 'write':
            value_to_write = self._local_variables[next_operation_simulator.local_variable]
            next_operation_simulator.operation.write_value = value_to_write
        self._transaction.add_operation(next_operation_simulator.operation)

    def operation_completed(self, scheduler: Scheduler, operation: Operation):
        assert len(self._operation_simulators) > 0
        operation_simulator = self._operation_simulators.pop(index=0)
        assert(operation == operation_simulator.operation)
        if operation.get_type() == 'read':
            dest_local_var_name = operation_simulator.local_variable
            self._local_variables[dest_local_var_name] = operation.read_value
            self.add_next_operation_to_transaction()
        # TODO: print to execution log!

    def operation_failed(self, scheduler: Scheduler, operation: Operation):
        assert len(self._operation_simulators) > 0
        operation_simulator = self._operation_simulators.pop(index=0)
        assert(operation == operation_simulator.operation)
        # TODO: print to execution log!


# Parse the input test file and add transactions and their operations.
# After each operation ends a `on_complete_callback` will be called (by the scheduler),
# and the next operation would be added by the `TransactionsWorkloadSimulator`.
class TransactionsWorkloadSimulator:
    def __init__(self):
        self._transaction_simulators = []
        self._transaction_id_to_transaction_simulator = dict()  # FIXME: maybe we don't need it
        self._schedule = 'RR'

    def load_test_data(self, workload_data_filename='transactions.dat'):
        with open(workload_data_filename, 'r') as test_file:
            test_first_line = test_file.readline()
            # TODO: parse test first line
            num_of_transactions = 88888  # TODO: parse from `test_first_line`
            self._schedule = 'RR'  # TODO: parse from `test_first_line`
            # TODO: verify each transaction is in new line
            for transaction_line in test_file.readline():
                transaction_line = transaction_line.strip()
                if not transaction_line:
                    continue  # ignore a blank line
                transaction_simulator = TransactionSimulator.parse_transaction_from_test_line(transaction_line)
                self._transaction_simulators.append(transaction_simulator)
                self._transaction_id_to_transaction_simulator[transaction_simulator.transaction_id] = transaction_simulator
            assert num_of_transactions == len(self._transaction_simulators)

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
