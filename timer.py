import time


class Timer:
    def __init__(self, enter_message=None, exit_message=None):
        """
        Construct a simple timer class.

        This can also be used as a context manager in a with block, eg:
            with Timer():
                pass

        Args:
            enter_message (str): If used as context manager, this message is printed at enter
            exit_message (str): If used as a context manager, this message is prepended to the context exit message
        """
        self.reference_time = None
        self.reset()
        if exit_message is None:
            self.exit_message = ""
        else:
            self.exit_message = str(exit_message) + ": "

        if enter_message is None:
            self.enter_message = None
        else:
            self.enter_message = str(enter_message)

    def elapsed(self):
        """
        Return the time elapsed between when this object was instantiated (or last reset) and now

        Returns:
            (float): Time elapsed in seconds
        """
        return time.perf_counter() - self.reference_time

    def reset(self):
        """
        Reset the reference timepoint to now

        Returns:
            None
        """
        self.reference_time = time.perf_counter()

    def __enter__(self):
        self.reset()
        if self.enter_message:
            print(self.enter_message)

    def __exit__(self, exc_type, exc_val, exc_tb):
        print(f"{self.exit_message}Process took {self.elapsed():.1f}s")
