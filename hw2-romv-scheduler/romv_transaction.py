from no_false_negative_variables_set import NoFalseNegativeVariablesSet
from scheduler_interface import SchedulerInterface
from multi_version_data_manager import MultiVersionDataManager


class ROMVTransaction(SchedulerInterface.ROTransaction):
    def __init__(self, *args, **kargs):
        kargs['is_read_only'] = True
        super().__init__(*args, **kargs)
        # Each ongoing read-only transaction holds a set of variables that has been committed
        # (by an update transaction) before that reader has born, but after the youngest older
        # reader has been born. So that, in each interval of time between the birth of each pair
        # of consecutive read-only transactions, the youngest one holds a set of all the variables
        # that has been committed in that interval of time. This makes that reader "responsible"
        # for these versions in some sense. More about it in the GC mechanism explanation.
        self._committed_variables_set_since_last_reader_born = NoFalseNegativeVariablesSet()

    @property
    def committed_variables_set_since_last_reader_born(self):
        return self._committed_variables_set_since_last_reader_born

    @committed_variables_set_since_last_reader_born.setter
    def committed_variables_set_since_last_reader_born(self, new_set):
        assert isinstance(new_set, NoFalseNegativeVariablesSet)
        self._committed_variables_set_since_last_reader_born = new_set


class UMVTransaction(SchedulerInterface.UTransaction):
    def __init__(self, *args, **kwargs):
        kwargs['is_read_only'] = False
        super().__init__(*args, **kwargs)
        self._local_written_values = dict()  # TODO: is it ok to store them on memory only?
        # For each variable that the transaction updates, we store the previous version
        # of that variable. This data is needed for the GC mechanism. In order to know
        # which versions to evict, in a case where there is no reader that is "responsible"
        # for the previous version.
        self._committed_variables_latest_versions_before_update = None

    @property
    def committed_variables(self):
        return self._local_written_values.keys()

    @property
    def committed_variables_latest_versions_before_update(self):
        assert self._committed_variables_latest_versions_before_update is not None
        return self._committed_variables_latest_versions_before_update

    # Called by the scheduler after the transaction commits, in order to store to
    # disk the updates that have been made by this transaction, as new versions.
    def complete_writes(self, mv_data_manager: MultiVersionDataManager):
        # Find the currently latest versions for each variable that the transaction
        # writes to, except for the variables that are written in the first time in
        # the database (the method `get_latest_version_number()` returns None for these).
        self._committed_variables_latest_versions_before_update = {
            variable: mv_data_manager.get_latest_version_number(variable)
            for variable in self._local_written_values.keys()
            if mv_data_manager.get_latest_version_number(variable) is not None
        }
        for variable, value in self._local_written_values.items():
            mv_data_manager.write_new_version(variable, value, self.timestamp)

    # Updates that the transaction makes are accessible only for this transaction.
    # Hence, we store them locally on the transacting session instance.
    def local_write(self, variable, value):
        self._local_written_values[variable] = value

    def local_read(self, variable):
        if variable not in self._local_written_values:
            return None
        return self._local_written_values[variable]
