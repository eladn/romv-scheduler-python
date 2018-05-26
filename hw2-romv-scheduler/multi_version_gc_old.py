from collections import namedtuple
from logger import Logger
from no_false_negative_variables_set import NoFalseNegativeVariablesSet
from timestamps_manager import Timespan
from transaction import Transaction
from romv_transaction import ROMVTransaction, UMVTransaction


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
        younger_reader, right_variables_set, right_reader_responsibility_timespan = \
            self._get_committed_variables_set_after_reader_and_before_next(committed_read_transaction, scheduler)

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

        # Print state to log.
        Logger().log('     GC: The right younger reader: {}'.format(
            'No such yet' if younger_reader is None else younger_reader.transaction_id), log_type_name='gc')
        Logger().log('     GC: Responsibility timespan of just-committed-reader: {}'.format(
            committed_reader_responsibility_timespan), log_type_name='gc')
        Logger().log('     GC: Responsibility timespan of right younger reader:  {}'.format(
            right_reader_responsibility_timespan), log_type_name='gc')
        Logger().log('     GC: Variables under the responsibility of just-committed-reader: {}'.format(
            str(set(left_variables_set))), log_type_name='gc')
        Logger().log('     GC: Variables under the responsibility of right younger reader:  {}'.format(
            str(set(right_variables_set))), log_type_name='gc')

        if intersection.might_be_not_empty():
            # Register the relevant versions (from_ts, to_ts) of the variables in the intersection for eviction.
            # We don't evict it now, but we add it as a GC eviction job, to the GC eviction jobs queue.
            gc_job = MultiVersionGC.GCJob(variables_to_check=intersection,
                                          remove_versions_in_timespan=committed_reader_responsibility_timespan,
                                          promised_newer_version_in_timespan=right_reader_responsibility_timespan)
            Logger().log('     GC: Add GC job because of reader committed, and the intersection between the just-committed reader responsibility and the younger reader responsibility is not empty.', log_type_name='gc')
            Logger().log('     GC: Variables at intersaction: {}'.format(str(set(intersection))), log_type_name='gc')
            self._gc_jobs_queue.append(gc_job)

        # For the old versions we could not remove now, pass the responsibility to the next younger reader.
        # These versions would be eventually in the responsibility time-span of a reader that will evict
        # them when it commits.
        unhandled = left_variables_set.difference(intersection)
        if unhandled.might_be_not_empty():
            right_variables_set.add_variables(unhandled)
            Logger().log(
                '     GC: The just-committed-reader passes variables under its responsibility to its right younger reader.'.format(
                    unhandled),
                log_type_name='gc')
            Logger().log('     GC: Passed variables: {}'.format(str(set(unhandled))), log_type_name='gc')

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
            Logger().log('     GC: Add GC job because of updater committed variable `{variable}` with version ({new_version}) and there is no active reader since previous version ({prev_version}) of {variable}.'.format(variable=variable, new_version=committed_update_transaction.timestamp, prev_version=prev_version), log_type_name='gc')
            self._gc_jobs_queue.append(gc_job)

        # Add the committed variables to the responsibility set of the youngest older read-only transaction.
        committed_variables_set = self._get_committed_variables_set_after_transaction_and_before_next(
            committed_update_transaction, scheduler)
        Logger().log('     GC: New committed variables: {variables} are added to the responsibility set of the youngest older read-only transaction (or to the global one).'.format(
                variables=str(set(committed_update_transaction.committed_variables))),
            log_type_name='gc')
        for variable in committed_update_transaction.committed_variables:
            committed_variables_set.add_variable(variable)

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
            return younger_transaction, self._committed_variables_set_since_younger_reader_born, responsibility_timespan
        assert isinstance(younger_transaction, ROMVTransaction)
        responsibility_timespan = self._get_responsibility_timespan_of_read_transaction(younger_transaction, scheduler)
        assert (after_reader.is_finished and responsibility_timespan.from_ts < after_reader.timestamp) or \
               (not after_reader.is_finished and responsibility_timespan.from_ts == after_reader.timestamp)
        return younger_transaction, younger_transaction.committed_variables_set_since_last_reader_born, responsibility_timespan

    def _get_committed_variables_set_after_transaction_and_before_next(self, after_transaction: Transaction, scheduler):
        younger_transaction = scheduler.get_read_transaction_younger_than(after_transaction)
        if younger_transaction is None:
            return self._committed_variables_set_since_younger_reader_born
        assert isinstance(younger_transaction, ROMVTransaction)
        return younger_transaction.committed_variables_set_since_last_reader_born
