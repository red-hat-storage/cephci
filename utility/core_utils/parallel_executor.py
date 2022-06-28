import asyncio
import nest_asyncio


class ParallelExecutor:
    def __init__(self):
        pass

    def run_until_complete(self, *args):
        """
        This method uses  asyncio's run_until_method.
        Args:
            args (tuple) : arguments are received in the form of tuple.
        Returns:
            result : result obtained after execution.
        """
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        future = asyncio.Future()
        asyncio.ensure_future(self.set_result(future, args))
        result = loop.run_until_complete(future)
        loop.close()
        return result

    def run(self, methods_list):
        # loop = asyncio.get_event_loop()
        # results = loop.create_task(self.run_async(methods_list))
        # return results
        loop = asyncio.new_event_loop()
        nest_asyncio.apply(loop)
        future = loop.create_future()
        asyncio.ensure_future(self.run_async(future, methods_list), loop=loop)
        results = loop.run_until_complete(future)
        loop.close()
        return results
        # results = asyncio.(self.run_async(methods_list))
        # loop = asyncio.get_event_loop()
        # future = asyncio.Future()
        # asyncio.ensure_future(self.run_async(future, methods_list))
        # results = loop.run_until_complete(future)
        # asyncio.
        # return results

    async def run_async(self, future_arg, methods_list):
        """
        Runs all the tasks asynchronously given in arguments.
        Args:
            methods_list (list) : list of tuples, each tuple contains the first element as method reference
                                  and the remaining elements of the tuple are method arguments
        Returns:
            result (list) : list of results obtained after execution of parallel tasks
        """
        results = []
        tasks = []
        futures_list = []

        for method_args in methods_list:
            future = asyncio.Future()
            futures_list.append(future)
            tasks.append(asyncio.ensure_future(self.set_result(future, method_args)))
        await asyncio.gather(*tasks)
        for future_iter in futures_list:
            results.append(future_iter.result())
        # return results
        future_arg.set_result(results)

    async def set_result(self, future_arg, args):
        """
        This is the method to run asynchronously and returns the result in the form of future_arg.
        Args:
            future_arg (future) : used for return purpose
            args (tuple) : tuple that should contain the first element as method reference
                           and the rest are the method arguments
        Returns:
            result : result obtained after method execution
        """
        args = list(args)
        method = args.pop(0)
        result = method(*args)
        future_arg.set_result(result)
