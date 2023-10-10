from abc import abstractmethod
from typing import Callable, TypeVar, List, Protocol, Generic, Union, Iterable, Optional, Generator, runtime_checkable


class PipelineError(Exception):
    pass


# Generic type which can hold anything as a context for passing through the pipeline
DataContext = TypeVar("DataContext")

# Generic Function for holding the subsequent pipeline step. Making a generic callable
# enables passing any complexity of collections including pre-process steps and breaking
# into multi-branch work streams
NextStep = Callable[[DataContext], Iterable[Union[Exception, DataContext]]]

ErrorHandler = Callable[[Exception, DataContext, NextStep], None]


@runtime_checkable  # Enables checking using the isinstance function
class PipelineStep(Protocol[DataContext]):
    """
        Acts as an individual step in the data formatting pipeline work stream. Is intended to be called as
        a function with the data context holding the in-flight data object along with the remainder of the Pipeline to
        be executed
    """
    @abstractmethod
    def __call__(self, data_context: DataContext, next_step: NextStep) -> Generator[DataContext]:
        ...


def _default_error_handler(error: Exception, data_context: DataContext, next_step: NextStep) -> None:
    raise error


class PipelineCursor(Generic[DataContext]):
    """
        Cursor object for orchestrating execution of Pipeline logic while also handling errors and tracking other
        metadata about a particular pipeline execution. Each instance stores the remaining pipeline steps to be executed
        upon instantiation and takes in the data context as it's calling input
    """
    def __init__(self, steps: List[PipelineStep], error_handler: ErrorHandler):
        self.queue = steps
        self.error_handler: ErrorHandler = error_handler

    def __call__(self, data_context: DataContext) -> None:
        if not self.queue:
            return
        current_step = self.queue[0]  # first step remaining in queue is current step
        next_step = PipelineCursor(self.queue[1:], self.error_handler)  # remaining steps are separated to be passed to
                                                                        # the next cursor execution to proceed
        try:
            current_step(data_context, next_step) # Current PipelineStep is executed with context and rest of Pipeline is passed as subsequent step
        except Exception as error:
            self.error_handler(error, data_context, next_step)


class Pipeline(Generic[DataContext]):
    """
    Holds the Pipeline Steps in their full and complete order
    """
    def __init__(self, *steps: PipelineStep):
        self.queue = [step for step in steps]

    def append(self, step: PipelineStep) -> None:
        self.queue.append(step)

    def __call__(self, data_context: DataContext, error_handler: Optional[ErrorHandler] = None) -> None:
        execute = PipelineCursor(self.queue, error_handler or _default_error_handler)
        return execute(data_context)

    def __len__(self) -> int:
        return len(self.queue)

