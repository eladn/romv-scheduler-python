from scheduler_interface import SchedulerInterface
from transaction import Transaction


# Simple serial scheduler. We implemented such one in order to perform tests on
# the `ROMVScheduler`. Note that the `ROMVScheduler` has also a serial scheduling
# scheme option. By passing the flag `--sched=compare-all` to `main.py`, we compare
# the results of our 3 schedulers.
# The implementation here is strait-forward.
class SerialScheduler(SchedulerInterface):

    class Disk:
        def __init__(self):
            self.mapping_from_variable_to_latest_value = dict()

        def is_variable_stored_on_disk(self, variable_name):
            return variable_name in self.mapping_from_variable_to_latest_value

        def update_variable_value(self, variable, new_value):
            self.mapping_from_variable_to_latest_value[variable] = new_value

        def read_variable_value(self, variable):
            return self.mapping_from_variable_to_latest_value[variable]

    def __init__(self):
        super().__init__(scheduling_scheme='serial')
        self._disk = SerialScheduler.Disk()

    def on_add_transaction(self, transaction: Transaction):
        pass  # We actually have nothing else to do here.

    def on_transaction_removed(self, transaction: Transaction):
        pass  # We actually have nothing else to do here.

    def run(self, forced_run_order=None):
        for transaction in self.iterate_over_ongoing_transactions_and_safely_remove_marked_to_remove_transactions(
                forced_run_order):

            # The user have not yet assigned the next operation to perform for that transaction.
            if not transaction.has_waiting_operation_to_perform(self):
                continue

            operation = transaction.try_perform_next_operation(self)
            assert operation.is_completed
            if transaction.is_completed:
                self.serialization_point(transaction.transaction_id)
                self.mark_transaction_to_remove(transaction)

    def try_write(self, transaction_id, variable, value):
        self._disk.update_variable_value(variable, value)
        return True

    def try_read(self, transaction_id, variable):
        assert self._disk.is_variable_stored_on_disk(variable)
        return self._disk.read_variable_value(variable)

    def get_variables(self):
        return self._disk.mapping_from_variable_to_latest_value.items()
