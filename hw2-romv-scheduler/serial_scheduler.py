from scheduler_base_modules import Scheduler, Transaction


class SerialScheduler(Scheduler):
    def __init__(self):
        super().__init__()

    def on_add_transaction(self, transaction: Transaction):
        pass  # TODO: impl

    def run(self):
        pass  # TODO: impl

    def try_write(self, transaction_id, variable, value):
        pass  # TODO: impl

    def try_read(self, transaction_id, variable):
        pass  # TODO: impl
