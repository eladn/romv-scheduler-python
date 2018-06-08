from user_simulator.transaction_parsing_patterns import TransactionParsingPatterns
from user_simulator.transaction_simulator import TransactionSimulator, TransactionSimulatorParser
from scheduler_interface import SchedulerInterface
import regex  # for parsing the test file. we use `regex` rather than known `re` to support named groups in patterns.


# Parse the input test file and add transactions and their operations to the given scheduler.
class TransactionsWorkloadSimulator:
    def __init__(self):
        self._transaction_simulators = []
        self._transaction_id_to_transaction_simulator = dict()  # FIXME: maybe we don't need it
        self._schedule = 'RR'

    def clone(self):
        new_simulator = TransactionsWorkloadSimulator()
        new_simulator._schedule = self._schedule
        for transaction_simulator in self._transaction_simulators:
            new_transaction_simulator = transaction_simulator.clone()
            new_simulator._transaction_simulators.append(new_transaction_simulator)
            new_simulator._transaction_id_to_transaction_simulator[new_transaction_simulator.transaction_id] = new_transaction_simulator
        return new_simulator

    @property
    def schedule(self):
        return self._schedule

    # Parse the test file and add its contents to the simulator.
    def load_test_data(self, workload_data_filename):
        with open(workload_data_filename, 'r') as test_file:

            # Read comment lines before the test first line.
            test_first_line = TransactionsWorkloadSimulator._read_first_nonempty_line_that_is_not_comment(test_file)

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
                transaction_simulator = TransactionSimulatorParser.parse_transaction_from_test_line(transaction_line)
                self._transaction_simulators.append(transaction_simulator)
                self._transaction_id_to_transaction_simulator[transaction_simulator.transaction_id] = transaction_simulator
            assert num_of_transactions == len(self._transaction_simulators)

    @staticmethod
    def _is_comment_line(test_line):
        comment_line_parser = regex.compile(TransactionParsingPatterns.comment_line_pattern)
        parsed_comment_line = comment_line_parser.match(test_line)
        return parsed_comment_line is not None

    @staticmethod
    def _read_first_nonempty_line_that_is_not_comment(test_file):
        for line in test_file:
            if not line.strip():
                continue
            if not TransactionsWorkloadSimulator._is_comment_line(line):
                return line
        return None

    # Given an scheduler, initiate the transactions (except for T0) in the scheduler.
    # For each transaction, add the first operation to it.
    # For each transaction, add a callback to be called by the scheduler after an operation has been completed,
    # so that the matching TransactionSimulator would insert the next operation.
    def add_workload_to_scheduler(self, scheduler: SchedulerInterface):
        for transaction_simulator in self._transaction_simulators:
            # FIXME: do we want to just skip the first transaction?
            if transaction_simulator.transaction_id == 0:
                continue
            transaction_simulator.add_transaction_to_scheduler(scheduler)

    def reset_simulator(self):
        for transaction_simulator in self._transaction_simulators:
            transaction_simulator.reset_simulator()

    def add_initialization_transaction_to_scheduler(self, scheduler: SchedulerInterface):
        # FIXME: do we want to just detect it as first transaction?
        initialization_transaction = self._transaction_id_to_transaction_simulator[0]
        initialization_transaction.add_transaction_to_scheduler(scheduler)

    class NotEqualException(ValueError):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

    @staticmethod
    def compare_runs(*simulators):
        assert all(isinstance(simulator, TransactionsWorkloadSimulator) for simulator in simulators)
        assert len(simulators) > 1
        if len(set((len(simulator._transaction_simulators) for simulator in simulators))) > 1:
            raise TransactionsWorkloadSimulator.NotEqualException('Not all transactions have the same number of transactions.')
        for transaction_simulators in zip(*(simulator._transaction_simulators for simulator in simulators)):
            TransactionSimulator.compare_all(*transaction_simulators)
