class SharedQueue:
    def __init__(self):
        self._storage = []

    def push(self, value):
        self._storage.append(value)

    def get(self, idx):
        return self._storage[idx]


class QueueConsumer:
    def __init__(self, shared_queue: SharedQueue):
        self._shared_queue = shared_queue
        self._idx = 0

    def push(self, value):
        self._shared_queue.push(value)

    def pop(self):
        # wait for a value if not exists
        return self._shared_queue.get(self._idx)


class Domain:
    def test(self, data):
        raise NotImplemented


class DictDomain(Domain):
    def __init__(self, description):
        self._description = description
        if self._description:
            pass

    def test(self, data):
        if not isinstance(data, dict):
            return False

        return all(k in data and data[k] == v for k, v in self._description.items())
