from abc import abstractmethod
from typing import Callable, TypeVar, List, Protocol, Generic, Union, Iterable, Optional, Any, Generator, runtime_checkable


class PipelineError(Exception):
    pass


# Generic type which can hold anything as a context for passing through the pipeline
DataContext = TypeVar("DataContext")

# Generic Function for holding the subsequent pipeline step. Making a generic callable
# enables passing any complexity of collections including pre-process steps and breaking
# into multi-branch work streams
NextStep = Callable[[DataContext], Iterable[Union[Exception, DataContext]]]


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


class Pipeline(Generic[DataContext]):
    def __init__(self, *steps: PipelineStep):
        self.queue = [step for step in steps]

    def append(self, step: PipelineStep) -> None:
        self.queue.append(step)

    def __call__(self, data_context: DataContext) -> None:
        # Execute steps
        pass

    def __len__(self) -> int:
        return len(self.queue)


class PipelineCursor(Generic[DataContext]):
    def __init__(self): ...

