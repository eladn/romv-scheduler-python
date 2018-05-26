import networkx as nx
assert int(nx.__version__.split('.')[0]) >= 2


# class meant for deadlock detecting using dependency graph
class DeadlockDetector:
    def __init__(self):
        self._wait_for_graph = nx.DiGraph()  # create a new directed graph (using networx lib).

    # Returns whether a dependency cycle has been created because of this new waiting.
    # If not, add the constrain to (add the matching edge to the graph).
    # Add the edge and check if it creates deadlock-cycle.
    # If so, remove edge and return such a cycle; Otherwise return None.
    def wait_for(self, waiting_transaction_id, waiting_for_transaction_id):
        if not(self._wait_for_graph.has_node(waiting_transaction_id)):
            self._wait_for_graph.add_node(waiting_transaction_id)
        if not(self._wait_for_graph.has_node(waiting_for_transaction_id)):
            self._wait_for_graph.add_node(waiting_for_transaction_id)

        self._wait_for_graph.add_edge(waiting_transaction_id, waiting_for_transaction_id)
        deadlock_cycle = self.find_deadlock_cycle()
        if deadlock_cycle is not None:
            self._wait_for_graph.remove_edge(waiting_transaction_id, waiting_for_transaction_id)
            return deadlock_cycle
        return None

    # delete this transaction and the relevant edges when a certain transaction ends.
    def transaction_ended(self, ended_transaction_id):
        if self._wait_for_graph.has_node(ended_transaction_id):
            # should remove all the connected edges to the ended_transaction_id
            self._wait_for_graph.remove_node(ended_transaction_id)

    # checks if there is a cycle in the graph - is so returns true, else return false.
    def find_deadlock_cycle(self):
        try:
            cycle = nx.find_cycle(self._wait_for_graph, orientation='original')
            return cycle
        except nx.NetworkXNoCycle:
            return None
