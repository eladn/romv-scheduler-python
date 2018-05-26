from collections import namedtuple, defaultdict
from timestamps_manager import Timestamp, Timespan
from doubly_linked_list import DoublyLinkedList


# The class is responsible for managing the versions of values of the variables in the system.
class MultiVersionDataManager:
    Version = namedtuple('Version', ['value', 'ts'])

    class Disk:
        def __init__(self):
            # a dictionary - for each variable we map a Version (consisted by value and ts).
            self.mapping_from_variable_to_versions_list = defaultdict(DoublyLinkedList)

        def access_versions_list_for_variable(self, variable_name):
            return self.mapping_from_variable_to_versions_list[variable_name]

        def is_variable_stored_on_disk(self, variable_name):
            return variable_name in self.mapping_from_variable_to_versions_list

        def find_node_of_certain_version(self, variable, ts: Timestamp):
            if not self.is_variable_stored_on_disk(variable):
                return None
            for version_node in self.access_versions_list_for_variable(variable).iter_over_nodes():
                version = version_node.data
                if version.ts != ts:
                    continue
                return version_node
            return None

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
               or self._disk.access_versions_list_for_variable(variable).peek_back().ts < new_ts
        self._disk.access_versions_list_for_variable(variable).push_back(new_version)
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
        assert len(self._disk.access_versions_list_for_variable(variable)) > 0
        latest_ts_from_disk = self._disk.access_versions_list_for_variable(variable).peek_back().ts

        # Lets update the cache now.
        self._cache_mapping_from_variable_to_its_lastest_ts[variable] = latest_ts_from_disk
        return latest_ts_from_disk

    # Returns the value of the latest version of the given variable.
    # Returns `None` if there wasn't any write to this variable yet.
    def read_latest_version(self, variable):
        # Lets access the disk to search for it.
        if not self._disk.is_variable_stored_on_disk(variable):
            return None
        assert len(self._disk.access_versions_list_for_variable(variable)) > 0
        return self._disk.access_versions_list_for_variable(variable).peek_back().value

    # This method is used by the GC eventual eviction mechanism.
    # Removes the version of the given variable with the given ts.
    def delete_old_version(self, variable_to_remove, timestamp_to_remove: Timestamp):
        print('delete_old_version: ', variable_to_remove, timestamp_to_remove)
        version_node = self._disk.find_node_of_certain_version(variable_to_remove, timestamp_to_remove)
        assert version_node is not None
        assert version_node.next_node is not None
        self._disk.access_versions_list_for_variable(variable_to_remove).remove_node(version_node)
        assert len(self._disk.access_versions_list_for_variable(variable_to_remove)) > 0

    # Used for printing all of the variables to the run-log for debugging purposes.
    def get_variables(self):
        for variable, versions_list in self._disk.mapping_from_variable_to_versions_list.items():
            yield variable, [tuple(version) for version in versions_list]
