from scheduler_base_modules import Scheduler, Transaction


class MultiVersionDataManager:
    pass  # TODO: impl


class LocksTable:
    pass  # TODO: impl


# TODO: fully doc it!
# When the `Scheduler` encounters a deadlock, it chooses a victim transaction and aborts it.
# It means that the scheduler would have to store the operations in a redo-log.
class ROMVScheduler(Scheduler):
    SchedulingSchemes = {'RR', 'serial'}

    def __init__(self, scheduling_scheme):
        super().__init__()
        assert scheduling_scheme in ROMVScheduler.SchedulingSchemes
        self._scheduling_scheme = scheduling_scheme
        self._locks_table = LocksTable()
        self._mv_data_manager = MultiVersionDataManager()
        # TODO: do we want to maintain a `wait_for` graph?

    def on_add_transaction(self, transaction: Transaction):
        # TODO: here we need to give timestamp for RO transactions.
        pass

    def run(self):
        # TODO: allow using `serial` scheduling-scheme iteration. Currently using only `RR`.

        while len(self._ongoing_transactions) > 0:
            for transaction in self._ongoing_transactions:
                # Try execute next operation
                transaction.try_perform_next_operation(data_access_manager=self)
                if transaction.is_completed:
                    pass  # TODO: release locks
            self.remove_completed_transactions()  # Cannot be performed inside the loop.
            self.detect_and_handle_deadlocks()

    def try_write(self, transaction_id, variable, value):
        pass  # TODO: impl

    def try_read(self, transaction_id, variable):
        pass  # TODO: impl

    def detect_and_handle_deadlocks(self):
        pass  # TODO: impl

    def abort_transaction(self, transaction: Transaction):
        self.remove_transaction(transaction)
        transaction.abort(self)
        # TODO: undo the transaction.
