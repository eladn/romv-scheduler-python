from scheduler_base_modules import Scheduler, Transaction


class SerialScheduler(Scheduler):
    def __init__(self):
        super().__init__(scheduling_scheme='serial')
        self._variables_latest_values = dict()

    def on_add_transaction(self, transaction: Transaction):
        pass  # We actually have nothing else to do here.

    def run(self):
        for transaction in self.iterate_over_transactions_by_tid_and_safely_remove_marked_to_remove_transactions():

            # The user haven't yet not assigned the next operation to perform for that transaction.
            if not transaction.has_waiting_operation_to_perform(self):
                continue

            operation = transaction.try_perform_next_operation(self)
            assert operation.is_completed
            if transaction.is_completed:
                self.serialization_point(transaction.transaction_id)
                self.mark_transaction_to_remove(transaction)

    def try_write(self, transaction_id, variable, value):
        self._variables_latest_values[variable] = value
        return True

    def try_read(self, transaction_id, variable):
        assert variable in self._variables_latest_values
        return self._variables_latest_values[variable]

    def get_variables(self):
        return self._variables_latest_values.items()
