
from scheduler_modules import Scheduler, Transaction, WriteOperation, ReadOperation, CommitOperation


# Used to print the status to output log.
class SchedulerExecutionLogger:
    pass


# Parse the input test file and add transactions and their operations.
# After each operation ends a `on_complete_callback` will be called (by the scheduler),
# and the next operation would be added by the `TransactionsWorkloadSimulator`.
class TransactionsWorkloadSimulator:
    def load_test_data(self, data_filename='transactions.dat'):
        pass  # TODO: impl

    def add_work_to_scheduler(self, scheduler: Scheduler):
        pass  # TODO: impl
