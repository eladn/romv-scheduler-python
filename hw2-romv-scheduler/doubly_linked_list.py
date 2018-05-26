
class DoublyLinkedList:
    class Node:
        def __init__(self, in_list, data, prev_node=None, next_node=None):
            self.in_list = in_list
            self.data = data
            self.prev_node = prev_node
            self.next_node = next_node

    def __init__(self):
        self._first = None
        self._last = None
        self._count = 0

    def clear(self):
        self._first = None
        self._last = None
        self._count = 0

    def peek_front(self):
        assert self._first is not None
        return self._first.data

    def peek_back(self):
        assert self._last is not None
        return self._last.data

    def push_front(self, data):
        # create a new node
        new_node = self.Node(self, data, None, None)
        if self._count == 0:
            self._first = new_node
            self._last = self._first
        elif self._count > 0:
            # set the new node to point to self._first
            new_node.next_node = self._first
            # have self._first point back to the new node
            self._first.prev_node = new_node
            # finally point to the new node as the self._first
            self._first = new_node
        self._count += 1
        return new_node

    def push_back(self, data):
        if self._count == 0:
            return self.push_front(data)
        new_node = self.Node(self, data, self._last, None)
        self._last.next_node = new_node
        self._last = self._last.next_node
        self._count += 1
        return new_node

    def insert_after_node(self, data, after_node: Node):
        assert(after_node is None or isinstance(after_node, self.Node))
        if after_node is None:
            return self.push_front(data)

        assert(after_node.in_list is self)
        new_node = self.Node(self, data, after_node, after_node.next_node)
        if after_node.next_node is not None:
            after_node.next_node.prev_node = new_node
        else:
            assert(self._last == after_node)
            self._last = new_node
        after_node.next_node = new_node
        self._count += 1
        return new_node

    def pop_front(self):
        if self._count == 0:
            raise RuntimeError("Cannot pop from an empty linked list")
        node_to_pop = self._first
        self.remove_node(node_to_pop)
        return node_to_pop.data

    def pop_back(self):
        if self._count == 0:
            raise RuntimeError("Cannot pop from an empty linked list")
        node_to_pop = self._last
        self.remove_node(node_to_pop)
        return node_to_pop.data

    def remove_node(self, node: Node):
        assert(node.in_list == self)
        self._count -= 1
        self._detach_node(node)
        node.in_list = None

    def _detach_node(self, node):
        if node.prev_node is not None:
            assert node.prev_node.in_list is self
            node.prev_node.next_node = node.next_node
        else:
            self._first = node.next_node
        if node.next_node is not None:
            assert node.next_node.in_list is self
            node.next_node.prev_node = node.prev_node
        else:
            self._last = node.prev_node

    def __repr__(self):
        result = ""
        if self._count == 0:
            return "..."
        cursor = self._first
        for i in range(self._count):
            result += "{}".format(cursor.data)
            cursor = cursor.next_node
            if cursor is not None:
                result += " --- "
        return result

    def __iter__(self):
        current_node = self._first
        while current_node is not None:
            yield current_node.data
            current_node = current_node.next_node

    def iter_over_nodes(self):
        current_node = self._first
        while current_node is not None:
            yield current_node
            current_node = current_node.next_node

    def __reversed__(self):
        current_node = self._last
        while current_node is not None:
            yield current_node.data
            current_node = current_node.prev_node

    def reversed_over_nodes(self):
        current_node = self._last
        while current_node is not None:
            yield current_node
            current_node = current_node.prev_node

    def __len__(self):
        return self._count

    def assert_valid(self):
        assert (self._first is None and self._last is None) or (self._first is not None and self._last is not None)
        cur = self._first
        prev = None
        size = 0
        while cur:
            assert (cur.in_list is self)
            size += 1
            assert prev == cur.prev_node
            prev = cur
            cur = cur.next_node
        assert prev == self._last
        assert self._count == size
