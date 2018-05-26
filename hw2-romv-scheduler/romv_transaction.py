from scheduler_interface import SchedulerInterface
from multi_version_data_manager import MultiVersionDataManager


class ROMVTransaction(SchedulerInterface.ROTransaction):
    def __init__(self, *args, **kargs):
        kargs['is_read_only'] = True
        super().__init__(*args, **kargs)

        # Used by the GC mechanism.
        # Each ongoing read-only transaction holds a set of old versions of variables.
        # When an update-transaction commits, it finds the youngest reader and passes
        # to it the responsibility for the previous version of tbe variables that it
        # just committed, and their timestamp is before the birth of that reader.
        # This makes that reader "responsible" for these versions in some sense.
        # When this reader commits, it have to mark these versions for eviction, or
        # pass the responsibility to an older reader.
        # More about it in the GC mechanism explanation.
        self.old_versions_under_my_responsibility = set()

        # Used by the GC mechanism.
        # We keep the read-only transactions stored in a list, sorted by timestamp.
        # When the transaction is given a timestamp, it is added to this list.
        self.ro_transactions_sorted_by_timestamp_list_node = None


class UMVTransaction(SchedulerInterface.UTransaction):
    def __init__(self, *args, **kwargs):
        kwargs['is_read_only'] = False
        super().__init__(*args, **kwargs)

        # The updates made during the update-transaction are stored inside of a local
        # mapping. It can be stored in the RAM or on the disk or both (caching) - we
        # did not explicitly referred that (in purpose).
        # The reason that it is ok is because no other transaction in the system can
        # read these updated version until this update-transaction commits.
        # On commit, these updates are written as new versions in the disk.
        # If a transaction updated a variable more than single time, only the final
        # version would be stored on the disk. Again, this is ok because the protocol
        # promises that no other transaction can access these intermediate updates.
        # When this update transaction performs a read, we first check whether this
        # variable has been written before by this transaction. If so, we load and
        # return the value in this the local mapping. Otherwise we read from the disk.
        # Practically this can be done in real-life case by maintaining a "bloom-filter"
        # in the memory that indicates which variables that transaction may have written
        # to avoid this 2 disk accesses.
        self._local_written_values = dict()

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
