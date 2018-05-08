from abc import ABC, abstractmethod

OPERATION_TYPES = {'read', 'write', 'commit'}  # FIXME: do we allow `abort`?
VARIABLE_OPERATION_TYPES = {'read', 'write'}


class Operation(ABC):
    def __init__(self, operation_type, variable=None):
        assert operation_type in OPERATION_TYPES
        assert operation_type not in VARIABLE_OPERATION_TYPES or variable is not None
        self._transaction_id = None
        self._operation_type = operation_type
        self._variable = variable
        self._is_completed = False

    def get_type(self):
        return self._operation_type

    @property
    def transaction_id(self):
        assert self._transaction_id is not None
        return self._transaction_id

    @transaction_id.setter
    def transaction_id(self, transaction_id):
        assert self._transaction_id is None
        self._transaction_id = transaction_id

    @property
    def variable(self):
        return self._variable

    @property
    def is_completed(self):
        return self._is_completed

    @abstractmethod
    def try_perform(self, data_access_manager):
        ...


class WriteOperation(Operation):
    def __init__(self, variable, to_write_value):
        super().__init__(operation_type='write',
                         variable=variable)
        self._to_write_value = to_write_value

    @property
    def to_write_value(self):
        assert self._to_write_value is not None
        return self._to_write_value

    def try_perform(self, data_access_manager):
        succeed = data_access_manager.try_write(self.transaction_id, self.variable, self._to_write_value)
        self._is_completed = succeed
        return succeed


class ReadOperation(Operation):
    def __init__(self, variable):
        super().__init__(operation_type='write',
                         variable=variable)
        self._read_value = None  # Only for `read` operations. After a value has been read it would be assigned here.

    @property
    def read_value(self):
        assert self._read_value is not None
        assert self._is_completed
        return self._read_value

    @read_value.setter
    def read_value(self, value):
        assert self._read_value is None
        assert not self._is_completed
        self._read_value = value

    def try_perform(self, data_access_manager):
        read_value = data_access_manager.try_read(self.transaction_id, self.variable)
        if read_value is None:
            return False
        self._is_completed = True
        self._read_value = read_value
        return True


class CommitOperation(Operation):
    def __init__(self):
        super().__init__(operation_type='commit')

    def try_perform(self, data_access_manager):
        # TODO: impl - what should we do here actually?
        self._is_completed = True
        return True


class Transaction:
    def __init__(self, transaction_id, is_read_only: bool, on_operation_complete_callback=None, on_operation_failed_callback=None):
        self._transaction_id = transaction_id
        self._is_read_only = is_read_only
        self._waiting_operations_queue = []
        self._is_completed = False
        # To be called after an operation is complete.
        self._on_operation_complete_callback = on_operation_complete_callback
        # To be called after an operation is failed (and now waiting till next attempt) due to locks.
        self._on_operation_failed_callback = on_operation_failed_callback

    @property
    def transaction_id(self):
        return self._transaction_id

    @property
    def is_read_only(self):
        return self._is_read_only

    @property
    def is_completed(self):
        return self._is_completed

    def get_next_operation(self):
        assert len(self._waiting_operations_queue) > 0
        return self._waiting_operations_queue.pop(index=0)

    def peek_next_operation(self):
        assert len(self._waiting_operations_queue) > 0
        return self._waiting_operations_queue[0]

    def try_perform_next_operation(self, data_access_manager):
        assert len(self._waiting_operations_queue) > 0

        next_operation = self._waiting_operations_queue[0]
        next_operation.try_perform(data_access_manager)

        if not next_operation.is_completed:
            self._on_operation_failed_callback(self, next_operation)
            return False

        queue_head = self._waiting_operations_queue.pop(index=0)
        assert queue_head == next_operation
        if next_operation.get_type() == 'commit':
            self._is_completed = True
        if self._on_operation_complete_callback:
            # The user callback might now add the next operation.
            self._on_operation_complete_callback(self, next_operation)
        return True

    def next_operation_completed(self):
        assert len(self._waiting_operations_queue) > 0

        completed_operation = self._waiting_operations_queue.pop(index=0)
        assert completed_operation.is_completed
        if completed_operation.get_type() == 'commit':
            assert not self._is_completed
            self._is_completed = True

    def add_operation(self, operation: Operation):
        assert not self._is_read_only or operation.get_type() != 'write'
        self._waiting_operations_queue.append(operation)  # FIXME: verify append adds in the end?
        operation.transaction_id = self.transaction_id


class MultiVersionDataManager:
    pass  # TODO: impl


class LocksTable:
    pass  # TODO: impl


class Scheduler(ABC):
    @abstractmethod
    def add_transaction(self, transaction: Transaction):
        ...

    @abstractmethod
    def run(self):
        ...

    @abstractmethod
    def try_write(self, transaction_id, variable, value):
        ...

    @abstractmethod
    def try_read(self, transaction_id, variable):
        ...


class ROMVScheduler(Scheduler):
    def __init__(self):
        self._ongoing_transactions = []
        self._locks_table = LocksTable()
        # TODO: do we want to maintain a `wait_for` graph?

    def add_transaction(self, transaction: Transaction):
        assert not transaction.is_completed
        self._ongoing_transactions.append(transaction)

    def run(self):
        while len(self._ongoing_transactions) > 0:
            for transaction in self._ongoing_transactions:
                # Try execute next operation
                transaction.try_perform_next_operation(data_access_manager=self)
                # TODO: write to log what has just happened here.
                if transaction.is_completed:
                    pass  # TODO: release locks
            self.remove_completed_transactions()
            self.detect_and_handle_deadlock()

    def try_write(self, transaction_id, variable, value):
        pass  # TODO: impl

    def try_read(self, transaction_id, variable):
        pass  # TODO: impl

    def remove_completed_transactions(self):
        self._ongoing_transactions = [transaction
                                      for transaction in self._ongoing_transactions
                                      if not transaction.is_completed]

    def detect_and_handle_deadlock(self):
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
