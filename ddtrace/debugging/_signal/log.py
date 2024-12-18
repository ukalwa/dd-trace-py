import abc
from dataclasses import dataclass
import typing as t

from ddtrace.debugging._probe.model import FunctionLocationMixin
from ddtrace.debugging._probe.model import LineLocationMixin
from ddtrace.debugging._signal.model import Signal


@dataclass
class LogSignal(Signal):
    """A signal that also emits a log message.

    Some signals might require sending a log message along with the base signal
    data. For example, all the collected errors from expression evaluations
    (e.g. conditions) might need to be reported.
    """

    @property
    @abc.abstractmethod
    def message(self) -> t.Optional[str]:
        """The log message to emit."""
        pass

    @abc.abstractmethod
    def has_message(self) -> bool:
        """Whether the signal has a log message to emit."""
        pass

    @property
    def data(self) -> t.Dict[str, t.Any]:
        """Extra data to include in the snapshot portion of the log message."""
        return {}

    def _probe_details(self) -> t.Dict[str, t.Any]:
        probe = self.probe
        if isinstance(probe, LineLocationMixin):
            location = {
                "file": str(probe.resolved_source_file),
                "lines": [str(probe.line)],
            }
        elif isinstance(probe, FunctionLocationMixin):
            location = {
                "type": probe.module,
                "method": probe.func_qname,
            }
        else:
            return {}

        return {
            "id": probe.probe_id,
            "version": probe.version,
            "location": location,
        }

    @property
    def snapshot(self) -> t.Dict[str, t.Any]:
        full_data = {
            "id": self.uuid,
            "timestamp": int(self.timestamp * 1e3),  # milliseconds
            "evaluationErrors": [{"expr": e.expr, "message": e.message} for e in self.errors],
            "probe": self._probe_details(),
            "language": "python",
        }
        full_data.update(self.data)

        return full_data
