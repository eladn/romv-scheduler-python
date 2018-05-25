from collections import namedtuple, defaultdict
from scheduler_base_modules import Scheduler, Transaction, Timestamp, TimestampsManager,\
    NoFalseNegativeVariablesSet, Timespan
from logger import Logger
import copy  # for deep-coping the disk in order to add the yet-committed variables so they could be printed to run-log.
import networkx as nx
assert int(nx.__version__.split('.')[0]) >= 2


# class meant for deadlock detecting using dependency graph
class DeadlockDetector:
    def __init__(self):
        self._wait_for_graph = nx.DiGraph()  # create a new directed graph (using networx lib).

    # Returns whether a dependency cycle has been created because of this new waiting.
    # If not, add the constrain to (add the matching edge to the graph).
    # add the edge and check if it creates deadlock - if so : remove edge, return false . else return true
    def wait_for(self, waiting_transaction_id, waiting_for_transaction_id):
        if not(self._wait_for_graph.has_node(waiting_transaction_id)):
            self._wait_for_graph.add_node(waiting_transaction_id)
        if not(self._wait_for_graph.has_node(waiting_for_transaction_id)):
            self._wait_for_graph.add_node(waiting_for_transaction_id)

        self._wait_for_graph.add_edge(waiting_transaction_id, waiting_for_transaction_id)
        if self.is_deadlock():
            self._wait_for_graph.remove_edge(waiting_transaction_id, waiting_for_transaction_id)
            return False
        return True

    # delete this transaction and the relevant edges when a certain transaction ends.
    def transaction_ended(self, ended_transaction_id):
        if self._wait_for_graph.has_node(ended_transaction_id):
            # should remove all the connected edges to the ended_transaction_id
            self._wait_for_graph.remove_node(ended_transaction_id)

    # checks if there is a cycle in the graph - is so returns true, else return false.
    def is_deadlock(self):
        try:
            cycle = nx.find_cycle(self._wait_for_graph, orientation='original')
            Logger().log('    >> is_deadlock() : found cycle {}'.format(cycle), 'scheduler_verbose')
            return True
        except nx.NetworkXNoCycle:
            return False


class LocksManager:
    # TransactionLocksSet = namedtuple('TransactionLocksSet', ['read_locks', 'write_locks'])
    class TransactionLocksSet:
        def __init__(self):
            self.read_locks = set()
            self.write_locks = set()

    def __init__(self):
        self._deadlock_detector = DeadlockDetector()

        # locks table fields (read locks multiset + write locks set)
        # our multiset would be a dict ! for each key will count the amount of its duplications.
        # dict : key - transaction id, value - tuple(read,write)
        self._transactions_locks_sets = defaultdict(LocksManager.TransactionLocksSet)  # { transaction_id: TransactionLocksSet }
        # the read locks table : the key is the variable to lock
        #                        the value is a set of who locks it at the moment
        self._read_locks_table = defaultdict(set)
        # the write locks table : the key is the variable to lock
        #                         the value is what transaction currently locks the variable
        self._write_locks_table = dict()

    def _collides_with(self, transaction_id, variable, read_or_write):
        assert read_or_write in {'read', 'write'}
        if variable in self._write_locks_table:
            assert len(self._read_locks_table[variable]) == 0 \
                   or self._read_locks_table[variable] == {self._write_locks_table[variable]}
            if self._write_locks_table[variable] == transaction_id:
                return None
            return {self._write_locks_table[variable]}
        if variable in self._read_locks_table and read_or_write == 'write':
            readers_without_me = self._read_locks_table[variable].difference({transaction_id})
            if len(readers_without_me) > 0:
                return readers_without_me
        return None

    # Returns:
    #   "GOT_LOCK" the lock has been acquired successfully (or the transaction also has the lock).
    #   "WAIT" is the lock could not be acquired now (someone else is holding it, but there is no dependency cycle).
    #   "DEADLOCK" if a deadlock will be created if the transaction would wait for this wanted lock.
    def try_acquire_lock(self, transaction_id, variable, read_or_write):
        assert read_or_write in {'read', 'write'}
        collides_with = self._collides_with(transaction_id, variable, read_or_write)
        if collides_with is None:
            if read_or_write == 'read':
                self._read_locks_table[variable].add(transaction_id)
                self._transactions_locks_sets[transaction_id].read_locks.add(variable)
            else:
                assert variable not in self._write_locks_table or self._write_locks_table[variable] == transaction_id
                self._write_locks_table[variable] = transaction_id
                self._transactions_locks_sets[transaction_id].write_locks.add(variable)
            return 'GOT_LOCK'
        assert isinstance(collides_with, set) and len(collides_with) > 0
        for wait_for_tid in collides_with:
            deadlock_detected = not self._deadlock_detector.wait_for(transaction_id, wait_for_tid)
            if deadlock_detected:
                return 'DEADLOCK'
        return 'WAIT'

    # when transaction_id is done, we release all of its acquired locks and remove it from the "wait-for" graph.
    def release_all_locks(self, transaction_id):
        # remove the just-ended-transaction from the "wait-for" graph.
        self._deadlock_detector.transaction_ended(transaction_id)

        # remove the locks from the data structures: self._read_locks_table, self._write_locks_table
        for read_var in self._transactions_locks_sets[transaction_id].read_locks:
            assert read_var in self._read_locks_table
            assert transaction_id in self._read_locks_table[read_var]
            self._read_locks_table[read_var].remove(transaction_id)
            if len(self._read_locks_table[read_var]) == 0:
                self._read_locks_table.pop(read_var)

        for write_var in self._transactions_locks_sets[transaction_id].write_locks:
            assert write_var in self._write_locks_table
            assert transaction_id == self._write_locks_table[write_var]
            self._write_locks_table.pop(write_var)

        self._transactions_locks_sets.pop(transaction_id)

    # returns if there is a deadlock at the moment
    def is_deadlock(self):
        return self._deadlock_detector.is_deadlock()


# The class is responsible for managing the versions of values of the variables in the system.
class MultiVersionDataManager:
    Version = namedtuple('Version', ['value', 'ts'])

    class Disk:
        def __init__(self):
            # a dictionary - for each variable we map a Version (consisted by value and ts).
            self.mapping_from_variable_to_versions_list = defaultdict(list)

        def access_versions_list_for_variable(self, variable_name):
            return self.mapping_from_variable_to_versions_list[variable_name]

        def is_variable_stored_on_disk(self, variable_name):
            return variable_name in self.mapping_from_variable_to_versions_list

    def __init__(self):
        self._disk = MultiVersionDataManager.Disk()

        # Local RAM cache saves the latest version ts for each variable.
        # Maps variable name to the latest committed ts for this value.
        self._cache_mapping_from_variable_to_its_lastest_ts = dict()

    # Returns the latest value before the given `max_ts` timestamp.
    # Returns `None` if there wasn't any write to this variable yet.
    def read_older_version_than(self, variable, max_ts: Timestamp):
        if variable not in self._disk.mapping_from_variable_to_versions_list:
            return None
        for version in reversed(self._disk.access_versions_list_for_variable(variable)):
            if version.ts < max_ts:
                return version.value
        return None

    # Adds the new version to the disk and update local cache.
    def write_new_version(self, variable, new_value, new_ts: Timestamp):
        new_version = MultiVersionDataManager.Version(value=new_value, ts=new_ts)
        assert len(self._disk.access_versions_list_for_variable(variable)) == 0\
               or self._disk.access_versions_list_for_variable(variable)[-1].ts < new_ts
        self._disk.access_versions_list_for_variable(variable).append(new_version)
        # update the latest ts in the local cache at RAM.
        self._cache_mapping_from_variable_to_its_lastest_ts[variable] = new_ts

    # Returns the ts of the latest version.
    # Returns `None` if there wasn't any write to this variable yet.
    def get_latest_version_number(self, variable):
        # Firstly lets take a look in the local cache (at RAM).
        if variable in self._cache_mapping_from_variable_to_its_lastest_ts:
            return self._cache_mapping_from_variable_to_its_lastest_ts[variable]

        # Variable not found in RAM. Lets access the disk to search for it.
        if not self._disk.is_variable_stored_on_disk(variable):
            return None
        latest_ts_from_disk = self._disk.access_versions_list_for_variable(variable)[-1].ts

        # Lets update the cache now.
        self._cache_mapping_from_variable_to_its_lastest_ts[variable] = latest_ts_from_disk
        return latest_ts_from_disk

    # Returns the value of the latest version of the given variable.
    # Returns `None` if there wasn't any write to this variable yet.
    def read_latest_version(self, variable):
        # Lets access the disk to search for it.
        if not self._disk.is_variable_stored_on_disk(variable):
            return None
        return self._disk.access_versions_list_for_variable(variable)[-1].value

    # This method is used by the GC eventual eviction mechanism.
    # Removes versions of the given variable that are inside of the given timespan
    # `remove_versions_in_timespan`, but do so only if the promised newer versions
    # exists. We check for existence of the promised newer versions it in order to
    # support using `NoFalseNegativeVariablesSet` for the GC mechanism. We elaborate
    # about it under the GC implementation.
    # Returns a boolean tuple - (bool,bool)
    #   The 1st bool: whether the version to remove has been found.
    #   The 2nd bool: whether the promised newer version has been found.
    #   The caller may assume the versions to remove has been removed iff both are true.
    def delete_old_versions_in_interval(self, variable,
                                        remove_versions_in_timespan: Timespan,
                                        promised_newer_version_in_timespan: Timespan):
        assert remove_versions_in_timespan.to_ts <= promised_newer_version_in_timespan.from_ts

        if not self._disk.is_variable_stored_on_disk(variable):
            return False, False

        flag_remove_found, flag_promised_found = False, False
        versions_to_remove = []  # marked versions to be remove
        for version in reversed(self._disk.access_versions_list_for_variable(variable)):
            if remove_versions_in_timespan.from_ts > version.ts > remove_versions_in_timespan.to_ts:
                flag_remove_found = True
                if flag_promised_found:
                    # Mark this version to remove. We cannot remove while iterating a python list.
                    # In real-life case we would like to remove it during the iteration.
                    versions_to_remove.append(version)
            if promised_newer_version_in_timespan.from_ts > version.ts > promised_newer_version_in_timespan.to_ts:
                flag_promised_found = True

        # Actually remove the versioned marked to remove.
        # In real-life case we would like to remove it during the finding iteration.
        for version_to_remove in versions_to_remove:
            self._disk.access_versions_list_for_variable(variable).remove(version_to_remove)

        return flag_remove_found, flag_promised_found

    # Used for printing all of the variables to the run-log for debugging purposes.
    def get_variables(self):
        for variable, versions_list in self._disk.mapping_from_variable_to_versions_list.items():
            yield variable, [tuple(version) for version in versions_list]


class ROMVTransaction(Scheduler.ROTransaction):
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


class UMVTransaction(Scheduler.UTransaction):
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


# The basic intuition is to evict the versions that no one would potentially need in
# the future. An update transaction may need only the latest version of any variable.
# But a read-only transaction may need the latest version since the transaction has
# born. We have to somehow keep track (efficiently) of versions and may need it in
# the future, so we could conservatively detect versions that no-one may need and evict
# it. Here we explain how the `MultiVersionGC` does it. Dealing wit updaters is simpler,
# so we explain it afterwards.
# Each ongoing read-only transaction holds a set of variables that has been committed
# (by an update transaction) before that reader has born, but after the youngest older
# reader has been born. So that, in each interval of time between the birth of each pair
# of consecutive read-only transactions, the youngest one holds a set of all the variables
# that has been committed in that interval of time. This makes that reader "responsible"
# for these versions in some sense. When a reader commits (and effectively ends its life),
# it should check whether the versions it is responsible for can be evicted. If so, it
# makes sure to delete these no-more-necessary versions. If some of these versions cannot
# get evicted (because another younger reader might need it or it is the latest version),
# the just-committed-reader should pass the responsibility for these versions to the oldest
# younger reader, as the just-committed-reader had never existed at all. Notice that a
# reader may access variables that has been committed in each one of the time-intervals
# that are older than the birth of that reader. Including its own interval, but also
# older ones.
# Practically, when an updater commits, it is still unknown which is the younger reader
# that is older than the just-committed-updater (because it would be born in the future).
# So we don't know in which set to store this variable. In order to deal with it, we
# keep a "global" set of variables, stored under the GC instance. When a new reader is
# born, this "global" set is assigned to that reader, and a new empty set it created
# and stored under this GC "global" pointer. It semantically means - variables that has
# been committed after the currently-youngest reader has been born.
# About updaters: When an update transaction commits, it checks whether there exists
# a reader that has born before the commit, but after the former version (of the same
# variable) has been committed. If so, this reader is responsible for the old version
# and we should do nothing. Otherwise, there exists no reader that is responsible for
# the former version. In that case, we should make sure to evict it when the updater
# commits. There exists no reader that needs this version, and there will be no such,
# because new readers would read the just-committed-version or newer one.
# GC-eviction-jobs:
# The GC is informed each time a transaction is committed. When the GC informed about a
# committed transaction, it checks about versions that can be evicted, as described below.
# When the GC encounters a version that has to be evicted, it does not actually perform
# the eviction. The eviction details are stored as a GC "job", in a dedicated gc-jobs-
# queue. Then, when the scheduler decides, it can offline call to `run_waiting_gc_jobs()`
# to perform the waiting previously registered evictions. The actual eviction application
# performs disk accesses. The idea is to avoid blocking the scheduler in favor of
# accessing the disk to perform GC evictions.
# Some notes about the variables set data structure in a real-life case:
# In a certain point in time, all of the variable names in the database would appear in
# the global set `_committed_variables_set_since_younger_reader_born`. We assume that
# in real-life case, a space-efficient data structure can be used here. We can use the
# fact that we only relay on the No-False-Negative property of our VariablesSet. This is
# because the GC-eviction-job only tells us on what variables and in what time-span we
# should check for existence in the disk. It is ok if sometimes when we ask the set for
# existence of a variable in the set, the set would answer "yes" despite the variable does
# not exist there. If such version does not exist we would see it when we search for it
# on the disk. From the way our suggested tracking mechanism works, it is promissed we
# will never evict a version that might be used by another transaction. We just want to know
# where to search for to avoid searching the whole disk. For example the bloom-filter data
# structure might be used as a variables set. In that case, we could not just iterate
# throughout all of the variables in the set, as we do now in the method `run_waiting_gc_jobs()`,
# because the hashing is (in a way) "one-way function". Instead, we could think of two
# "removal-trigger" types: The first is coarse - When a variable is encountered in the system,
# check the GC whether this variable is contained in a GC-eviction-job, and evict it. The second
# is fined-grained - Periodically iterate over all the variables in the system and for each
# one check for its existence in each waiting GC-eviction-job.
class MultiVersionGC:
    GCJob = namedtuple('GCJob', ['variables_to_check',
                                 'remove_versions_in_timespan',
                                 'promised_newer_version_in_timespan'])

    def __init__(self):
        # List of GCJob instances. Eviction jobs waiting to be performed.
        # The actual eviction is perform by `run_waiting_gc_jobs()`, but
        # the versions to evict are registered during the scheduler iterations.
        self._gc_jobs_queue = []
        # This set stores all of the variables that has been committed by an update
        # transaction, since the younger read-only-transaction has born. When the
        # next reader will be born, this set will be assigned to that reader, and
        # a new empty set will be assigned to this pointer instead of the former one.
        self._committed_variables_set_since_younger_reader_born = NoFalseNegativeVariablesSet()

    # Called by the scheduler each time a new read-only transaction is born.
    def new_read_only_transaction(self, transaction: ROMVTransaction):
        transaction.committed_variables_set_since_last_reader_born = self._committed_variables_set_since_younger_reader_born
        self._committed_variables_set_since_younger_reader_born = NoFalseNegativeVariablesSet()
        # TODO: should we do anything else here?

    def _read_only_transaction_committed(self, committed_read_transaction: ROMVTransaction, scheduler):
        # Hierarchic order of mentioned items:
        # ######### | ------------------------------- | ########## | ------------------------------- | ####### |
        #  Youngest |  Time interval between readers  |  Current   | Time interval between readers   | Oldest  |
        #  Older    |  The responsibility area of the |  Committed | The responsibility area of the  | Younger |
        #  Reader   |  current committed reader       |  Reader    | oldest younger reader if exists | Reader  |
        #  Birth    |     { left variables set }      |  Birth     |     { right variables set }     | Birth   |
        left_variables_set = committed_read_transaction.committed_variables_set_since_last_reader_born
        right_variables_set, right_reader_responsibility_timespan = self._get_committed_variables_set_after_reader_and_before_next(
            committed_read_transaction, scheduler)

        # The just-committed-read-transaction has indeed ended. Hence the returned timespan of the younger alive
        # reader ALREADY includes the timespan of this just-committed-reader. We want to have the timespan of the
        # right reader, as it was just before the current transaction ended.
        right_reader_responsibility_timespan = Timespan(from_ts=committed_read_transaction.timestamp,
                                                        to_ts=right_reader_responsibility_timespan.to_ts)

        # This is the time-span of the responsibility of the just-committed-reader.
        # Reminder: The responsibility time is from the birth of youngest-older reader,
        #           and until the birth of the just-committed-reader.
        committed_reader_responsibility_timespan = self._get_responsibility_timespan_of_read_transaction(
            committed_read_transaction, scheduler)

        # This is the set of variables that has been committed in the responsibility
        # time-span of the just-committed-reader and also committed in the responsibility
        # time-span of the next younger reader. It means that now no reader needs them.
        intersection = left_variables_set.intersection(right_variables_set)

        # Register the relevant versions (from_ts, to_ts) of the variables in the intersection for eviction.
        # We don't evict it now, but we add it as a GC eviction job, to the GC eviction jobs queue.
        gc_job = MultiVersionGC.GCJob(variables_to_check=intersection,
                                      remove_versions_in_timespan=committed_reader_responsibility_timespan,
                                      promised_newer_version_in_timespan=right_reader_responsibility_timespan)
        self._gc_jobs_queue.append(gc_job)

        # For the old versions we could not remove now, pass the responsibility to the next younger reader.
        # These versions would be eventually in the responsibility time-span of a reader that will evict
        # them when it commits.
        unhandled = left_variables_set.difference(intersection)
        right_variables_set.add_variables(unhandled)

    def _update_transaction_committed(self, committed_update_transaction: UMVTransaction, scheduler):
        previous_versions = committed_update_transaction.committed_variables_latest_versions_before_update
        for variable, prev_version in previous_versions.items():
            if scheduler.are_there_read_transactions_after_ts_and_before_transaction(prev_version,
                                                                                     committed_update_transaction):
                # If there exists a read-only transaction younger than the new committed version, but older
                # then the previous version (of the current examined variable), than that reader is responsible
                # for that previous version, and we should do nothing here.
                continue
            # We revealed that there is no reader that is responsible for the previous version of that variable.
            # New readers would be responsible for the new-just-created version. So the previous version can
            # be evicted. Hence, we add it as a GC eviction job.
            remove_versions_in_timespan = Timespan(from_ts=prev_version, to_ts=prev_version)
            promised_newer_version_in_timespan = Timespan(from_ts=committed_update_transaction.timestamp,
                                                          to_ts=committed_update_transaction.timestamp)
            gc_job = MultiVersionGC.GCJob(variables_to_check={variable},
                                          remove_versions_in_timespan=remove_versions_in_timespan,
                                          promised_newer_version_in_timespan=promised_newer_version_in_timespan)
            self._gc_jobs_queue.append(gc_job)

    # Called by the scheduler each time a transaction is committed.
    def transaction_committed(self, transaction: Transaction, scheduler):
        if transaction.is_read_only:
            assert isinstance(transaction, ROMVTransaction)
            self._read_only_transaction_committed(transaction, scheduler)
        else:
            assert isinstance(transaction, UMVTransaction)
            self._update_transaction_committed(transaction, scheduler)

    def run_waiting_gc_jobs(self, scheduler):
        nr_gc_jobs = len(self._gc_jobs_queue)
        for job_nr in range(nr_gc_jobs):
            gc_job = self._gc_jobs_queue.pop(0)
            # remove the relevant versions (in the timespan to remove) of the variables to check!
            for variable in gc_job.variables_to_check:
                versions_to_remove_exist, promised_newer_versions_exist = \
                    scheduler.mv_data_manager.delete_old_versions_in_interval(
                    variable, gc_job.remove_versions_in_timespan, gc_job.promised_newer_version_in_timespan)
                if versions_to_remove_exist and not promised_newer_versions_exist:
                    # It might happen because we use No-False-Negatives set.
                    # So a false positive might happen for the "right-variables-set"
                    # (see method _read_only_transaction_committed to understand notation)
                    # TODO: impl. handle it! add it to another reader responsibility or remove it.
                    # We didn't handled this case because we currently use naive variables-set implementation,
                    # that has neither FP nor FN. In real-life case it can be handled here, by finding
                    # another reader to pass this responsibility to, or if there is no such a reader, just
                    # remove this version now anyway if it is not the latest. In that case, the readers have
                    # to be stored in a tree rather than a list, in order to efficiently search there.
                    # Alternatively these ones can be added to a dedicated list of coarse-eviction, that is
                    # being checked periodically for eviction, less efficient than the main mechanism. This
                    # handling is possible because we may assume a certain expected ratio of false-positives.
                    assert False

    def _get_responsibility_timespan_of_read_transaction(self, read_transaction: ROMVTransaction, scheduler):
        # Reminder: The responsibility time-span of reader A is from the birth of the
        #           youngest-older reader (born before A), and until the birth of reader A.
        older_reader = scheduler.get_read_transaction_older_than(read_transaction)
        responsibility_from_ts = older_reader.timestamp if older_reader is not None else 0  # FIXME: is that correct?
        responsibility_to_ts = read_transaction.timestamp
        responsibility_timespan = Timespan(from_ts=responsibility_from_ts,
                                           to_ts=responsibility_to_ts)
        return responsibility_timespan

    def _get_committed_variables_set_after_reader_and_before_next(self, after_reader: ROMVTransaction, scheduler):
        younger_transaction = scheduler.get_read_transaction_younger_than(after_reader)
        if younger_transaction is None:
            responsibility_timespan = Timespan(from_ts=after_reader.timestamp,
                                               to_ts=scheduler.get_current_ts())
            return self._committed_variables_set_since_younger_reader_born, responsibility_timespan
        assert isinstance(younger_transaction, ROMVTransaction)
        responsibility_timespan = self._get_responsibility_timespan_of_read_transaction(younger_transaction, scheduler)
        assert (after_reader.is_finished and responsibility_timespan.from_ts < after_reader.timestamp) or \
               (not after_reader.is_finished and responsibility_timespan.from_ts == after_reader.timestamp)
        return younger_transaction.committed_variables_set_since_last_reader_born, responsibility_timespan


# TODO: fully doc it!
# When the `Scheduler` encounters a deadlock, it chooses a victim transaction and aborts it.
# It means that the scheduler would have to store the operations in a redo-log. (is that correct?)
class ROMVScheduler(Scheduler):
    ROTransaction = ROMVTransaction
    UTransaction = UMVTransaction
    SchedulingSchemes = {'RR', 'serial'}

    def __init__(self, scheduling_scheme):
        super().__init__(scheduling_scheme)
        assert scheduling_scheme in ROMVScheduler.SchedulingSchemes
        self._scheduling_scheme = scheduling_scheme
        self._locks_manager = LocksManager()
        self._mv_data_manager = MultiVersionDataManager()
        self._mv_gc = MultiVersionGC()
        self._timestamps_manager = TimestampsManager()

    @property
    def mv_data_manager(self):
        return self._mv_data_manager

    def on_add_transaction(self, transaction: Transaction):
        if transaction.is_read_only:
            assert isinstance(transaction, ROMVTransaction)
            # Behaviour changed: We now assign timestamp only when the first read operation is being tried to be performed.
            ## Assign new timestamp for RO transactions.
            ## self.assign_timestamp_to_transaction(transaction)
            ## self._mv_gc.new_read_only_transaction(transaction)
        else:
            assert isinstance(transaction, UMVTransaction)
            # TODO: should we do here something for update transaction?

    def get_current_ts(self):
        return self._timestamps_manager.peek_next_ts()

    # TODO: doc!
    def run(self):
        for transaction in self.iterate_over_transactions_by_tid_and_safely_remove_marked_to_remove_transactions():

            # The user haven't yet not assigned the next operation to perform for that transaction.
            if not transaction.has_waiting_operation_to_perform(self):
                continue

            # Assign new timestamp for RO transactions when trying to perform the first read.
            if transaction.is_read_only and not transaction.has_timestamp:
                assert isinstance(transaction, ROMVTransaction)
                self.assign_timestamp_to_transaction(transaction)
                self._mv_gc.new_read_only_transaction(transaction)

            # Try execute next operation
            transaction.try_perform_next_operation(self)
            # Invariant: At any point in time, there is no cycle in the "wait-for" graph.
            #            Each time a transaction attempts to lock a variable, the conflict graph
            #            is updated. When a conflict cycle is firstly created it is immediately
            #            detected and then immediately broken by the scheduler.
            assert not self._locks_manager.is_deadlock()
            if transaction.is_aborted:
                # Note: the transaction already has been marked to remove by the scheduler.
                assert isinstance(transaction, UMVTransaction)
                continue
            if transaction.is_completed:
                self.mark_transaction_to_remove(transaction)  # TODO: does it have to be after handling the completed transaction?
                if not transaction.is_read_only:
                    assert isinstance(transaction, UMVTransaction)
                    self.assign_timestamp_to_transaction(transaction)
                    transaction.complete_writes(self._mv_data_manager)
                    self._locks_manager.release_all_locks(transaction.transaction_id)
                self._mv_gc.transaction_committed(transaction, self)  # TODO: should it be before releasing locks?
                self._mv_gc.run_waiting_gc_jobs(self)

    def assign_timestamp_to_transaction(self, transaction: Transaction):
        transaction.timestamp = self._timestamps_manager.get_next_ts()
        self.serialization_point(transaction.transaction_id)

    # TODO: doc!
    def try_write(self, transaction_id, variable, value):
        transaction = self.get_transaction_by_id(transaction_id)
        assert transaction is not None
        assert not transaction.is_read_only
        assert isinstance(transaction, UMVTransaction)

        got_lock = self._locks_manager.try_acquire_lock(transaction_id, variable, 'write')
        if got_lock == 'DEADLOCK':
            # Invariant: At any point in time, there is no cycle in the "wait-for" graph.
            #            Each time a transaction attempts to lock a variable, the conflict graph
            #            is updated. When a conflict cycle is firstly created it is immediately
            #            detected and then immediately broken by the scheduler.
            # TODO: maybe abort another transaction?
            self.abort_transaction(transaction)  # FIXME: is it ok to call it here?
            assert not self._locks_manager.is_deadlock()
            return False  # failed
        elif got_lock == 'WAIT':
            return False  # failed

        assert got_lock == 'GOT_LOCK'
        # FIXME: is this "local-storing" mechanism ok?
        transaction.local_write(variable, value)
        return True  # succeed writing the new value

    # TODO: doc!
    # Use the `_locks_manager` only for update transaction.
    def try_read(self, transaction_id, variable):
        transaction = self.get_transaction_by_id(transaction_id)
        assert transaction is not None
        if transaction.is_read_only:
            assert isinstance(transaction, ROMVTransaction)
            return self._mv_data_manager.read_older_version_than(variable, transaction.timestamp)

        assert isinstance(transaction, UMVTransaction)
        got_lock = self._locks_manager.try_acquire_lock(transaction_id, variable, 'read')
        if got_lock == 'DEADLOCK':
            # Invariant: At any point in time, there is no cycle in the "wait-for" graph.
            #            Each time a transaction attempts to lock a variable, the conflict graph
            #            is updated. When a conflict cycle is firstly created it is immediately
            #            detected and then immediately broken by the scheduler.
            # TODO: maybe abort another transaction?
            self.abort_transaction(transaction)  # FIXME: is it ok to call it here?
            assert not self._locks_manager.is_deadlock()
            return None  # failed
        elif got_lock == 'WAIT':
            return None  # failed

        assert got_lock == 'GOT_LOCK'
        # FIXME: is this "local-storing" mechanism ok?
        value = transaction.local_read(variable)
        if value is not None:
            return value
        return self._mv_data_manager.read_latest_version(variable)

    # TODO: doc!
    def abort_transaction(self, transaction: Transaction):
        assert not transaction.is_read_only
        assert isinstance(transaction, UMVTransaction)
        self._locks_manager.release_all_locks(transaction.transaction_id)
        self.mark_transaction_to_remove(transaction)  # TODO: is it ok?
        # Notice: The call to `transaction.abort(..)` invokes the user-callback that might
        # add a new transaction with the same transaction id as of the aborted transaction.
        # It is ok because we already marked the aborted transaction to remove, and by side
        # effect it has been removed from the ongoing_transactions_mapping.
        # Additionally, note that the potentially new added transaction (with the same
        # transaction id) has been added before the aborted transaction in the transactions
        # list sorted by transaction_id. So that the iteration in `run(..)` won't encounter
        # this transaction again until next loop (in RR scheduling scheme).
        transaction.abort(self)
        # TODO: Undo the transaction. We currently storing the updates locally on the transaction itself only.

    # For the GC mechanism, when a read-only transaction commits, we need to find the
    # intersection of variables that the just-committed reader is responsible for, with
    # the variables that the oldest younger read-only transaction is responsible for.
    # This method helps us finding this read-only transaction.
    def get_read_transaction_younger_than(self, transaction: Transaction):
        if transaction.is_read_only:
            current_node = transaction.ro_transactions_by_arrival_list_node.next_node
        else:
            current_node = transaction.transactions_by_arrival_list_node.next_node
        while current_node is not None and (not current_node.data.is_read_only or current_node.data.is_finished
                                            or not current_node.data.has_timestamp):
            current_node = current_node.next_node
        return None if current_node is None else current_node.data

    # For the GC mechanism, when a read-only transaction commits, we need to find the
    # responsibility time-span of the just-committed reader. This time-span starts
    # when the youngest older reader was born.
    # This method helps us finding this read-only transaction.
    def get_read_transaction_older_than(self, transaction: Transaction):
        if transaction.is_read_only:
            current_node = transaction.ro_transactions_by_arrival_list_node.prev_node
        else:
            current_node = transaction.transactions_by_arrival_list_node.prev_node
        while current_node is not None and (not current_node.data.is_read_only or current_node.data.is_finished
                                            or not current_node.data.has_timestamp):
            current_node = current_node.prev_node
        return None if current_node is None else current_node.data

    # For the GC mechanism, when an updater transaction commits, we need to decide
    # whether to evict the previous version of the updated variables. If there exists
    # a read-only transaction that is responsible for the previous version, we should
    # do nothing there. But it there is no such reader, the update transaction should
    # mark the previous version for eviction.
    # This method helps us finding whether this read-only transaction exists.
    def are_there_read_transactions_after_ts_and_before_transaction(self, after_ts: Timestamp, before_transaction: Transaction):
        reader = self.get_read_transaction_younger_than(before_transaction)
        if reader is None:
            return False
        return reader.timestamp > after_ts  # FIXME: should be `>=` inequality or just `>`?

    # Used for printing all of the variables to the run-log for debugging purposes.
    def get_variables(self):
        # TODO: if we use inner disk storage of a write transaction - do not do this!
        variables = copy.deepcopy(dict(self._mv_data_manager.get_variables()))
        for transaction in self._ongoing_u_transactions_by_arrival:
            assert isinstance(transaction, UMVTransaction)
            if transaction.is_aborted:
                continue
            for var, new_uncommitted_version in transaction._local_written_values.items():
                if var not in variables:
                    variables[var] = []
                variables[var].append((new_uncommitted_version, 'uncommitted'))
        return variables.items()

