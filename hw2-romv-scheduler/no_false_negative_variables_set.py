

# For the GC mechanism we sometimes have to store a set of variables. We see later
# why and how we do it. The catch is that we only relay on the No-False-Negative
# property of that variables set. Here we just show a naive implementation of such
# set data structure. In real-life we might want to use a more space-efficient data
# structure instead. The bloom-filter data structure is an example for one. More
# details about it under the documentation of the garbage-collector implementation
# (in "multi_version_gc.py").
class NoFalseNegativeVariablesSet:
    def __init__(self):
        self._variables_set = set()

    def add_variable(self, variable):
        self._variables_set.add(variable)

    def add_variables(self, variables_set_to_add):
        assert isinstance(variables_set_to_add, NoFalseNegativeVariablesSet)
        self._variables_set = self._variables_set.union(variables_set_to_add._variables_set)

    def is_variable_exists(self, variable):
        return variable in self._variables_set

    def intersection(self, another_variables_set):
        assert isinstance(another_variables_set, NoFalseNegativeVariablesSet)
        new_variables_set = NoFalseNegativeVariablesSet()
        new_variables_set._variables_set = self._variables_set.intersection(another_variables_set._variables_set)
        return new_variables_set

    def difference(self, another_variables_set):
        assert isinstance(another_variables_set, NoFalseNegativeVariablesSet)
        new_variables_set = NoFalseNegativeVariablesSet()
        new_variables_set._variables_set = self._variables_set.difference(another_variables_set._variables_set)
        return new_variables_set

    def might_be_not_empty(self):
        return len(self._variables_set) > 0

    def __iter__(self):
        return iter(self._variables_set)
