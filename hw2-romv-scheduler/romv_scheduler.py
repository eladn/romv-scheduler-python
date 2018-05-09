from scheduler_base_modules import Scheduler, Transaction


class MultiVersionDataManager:
    pass  # TODO: impl


class LocksTable:
    pass  # TODO: impl


class ROMVScheduler(Scheduler):
    def __init__(self):
        super().__init__()
        self._ongoing_transactions = []
        self._transactions_mapping = dict()
        self._locks_table = LocksTable()
        # TODO: do we want to maintain a `wait_for` graph?

    def add_transaction(self, transaction: Transaction):
        assert not transaction.is_completed
        self._ongoing_transactions.append(transaction)
        self._transactions_mapping[transaction.transaction_id] = transaction
        # TODO: here we need to give timestamp for RO transactions.

    def run(self):
        while len(self._ongoing_transactions) > 0:
            for transaction in self._ongoing_transactions:
                # Try execute next operation
                transaction.try_perform_next_operation(data_access_manager=self)
                # TODO: write to log what has just happened here.
                if transaction.is_completed:
                    pass  # TODO: release locks
            self.remove_completed_transactions()  # Cannot be performed inside the loop.
            self.detect_and_handle_deadlocks()

    def try_write(self, transaction_id, variable, value):
        pass  # TODO: impl

    def try_read(self, transaction_id, variable):
        pass  # TODO: impl

    def remove_completed_transactions(self):
        self._ongoing_transactions = [transaction
                                      for transaction in self._ongoing_transactions
                                      if not transaction.is_completed]
        self._transactions_mapping = {transaction.transaction_id: transaction
                                      for transaction in self._ongoing_transactions}

    def detect_and_handle_deadlocks(self):
        pass  # TODO: impl


class SerialScheduler(Scheduler):
    def add_transaction(self, transaction: Transaction):
        pass  # TODO: impl

    def run(self):
        pass  # TODO: impl

    def try_write(self, transaction_id, variable, value):
        pass  # TODO: impl

    def try_read(self, transaction_id, variable):
        pass  # TODO: impl
