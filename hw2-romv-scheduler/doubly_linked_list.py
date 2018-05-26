
class DoublyLinkedList:
    class Node:
        def __init__(self, in_list, data, prev_node=None, next_node=None):
            self.in_list = in_list
            self.data = data
            self.prev_node = prev_node
            self.next_node = next_node

    def __init__(self):
        self.first = None
        self.last = None
        self.count = 0

    def clear(self):
        self.first = None
        self.last = None
        self.count = 0

    def peek_front(self):
        assert self.first is not None
        return self.first.data

    def peek_back(self):
        assert self.last is not None
        return self.last.data

    def push_front(self, data):
        # create a new node
        new_node = self.Node(self, data, None, None)
        if self.count == 0:
            self.first = new_node
            self.last = self.first
        elif self.count > 0:
            # set the new node to point to self.first
            new_node.next_node = self.first
            # have self.first point back to the new node
            self.first.prev_node = new_node
            # finally point to the new node as the self.first
            self.first = new_node
        self.count += 1
        return new_node

    def push_back(self, data):
        if self.count == 0:
            return self.push_front(data)
        new_node = self.Node(self, data, self.last, None)
        self.last.next_node = new_node
        self.last = self.last.next_node
        self.count += 1
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
            assert(self.last == after_node)
            self.last = new_node
        after_node.next_node = new_node
        self.count += 1
        return new_node

    def pop_front(self):
        if self.count == 0:
            raise RuntimeError("Cannot pop from an empty linked list")
        node_to_pop = self.first
        self.remove_node(node_to_pop)
        return node_to_pop.data

    def pop_back(self):
        if self.count == 0:
            raise RuntimeError("Cannot pop from an empty linked list")
        node_to_pop = self.last
        self.remove_node(node_to_pop)
        return node_to_pop.data

    def remove_node(self, node: Node):
        assert(node.in_list == self)
        self.count -= 1
        self._detach_node(node)
        node.in_list = None

    def _detach_node(self, node):
        if node.prev_node is not None:
            assert node.prev_node.in_list is self
            node.prev_node.next_node = node.next_node
        else:
            self.first = node.next_node
        if node.next_node is not None:
            assert node.next_node.in_list is self
            node.next_node.prev_node = node.prev_node
        else:
            self.last = node.prev_node

    def __repr__(self):
        result = ""
        if self.count == 0:
            return "..."
        cursor = self.first
        for i in range(self.count):
            result += "{}".format(cursor.data)
            cursor = cursor.next_node
            if cursor is not None:
                result += " --- "
        return result

    def __iter__(self):
        current_node = self.first
        while current_node is not None:
            yield current_node.data
            current_node = current_node.next_node

    def __reversed__(self):
        current_node = self.last
        while current_node is not None:
            yield current_node.data
            current_node = current_node.prev_node

    def __len__(self):
        return self.count

    def assert_valid(self):
        assert (self.first is None and self.last is None) or (self.first is not None and self.last is not None)
        cur = self.first
        prev = None
        size = 0
        while cur:
            assert (cur.in_list is self)
            size += 1
            assert prev == cur.prev_node
            prev = cur
            cur = cur.next_node
        assert prev == self.last
        assert self.count == size
