import requests
from assemply.node import node_class


@node_class('!Request')
class RequestNode:
    """HTTP client node"""
    def __init__(self, request=None, response=None, exception=None):
        """
        Request entry, request output.
        """
        self._request_queue = request
        self._response_queue = response
        self._exception_queue = exception

    async def run(self):
        """
        Loop execution to pop request from queue and push response if not any error.
        Exceptions will being pushed on exception queue.

        :return: None
        :rtype: None
        """
        async with self._exception_queue as e, self._response_queue as r:
            async for method, url, params in self._request_queue:
                try:
                    response = requests.request(method, url, **params)
                except Exception as x:
                    await e.put(x)
                else:
                    await r.put(response)
