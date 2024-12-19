from dataclasses import dataclass
from dataclasses import field
from typing import Optional
from typing import cast

from ddtrace.debugging._metrics import probe_metrics
from ddtrace.debugging._probe.model import MetricFunctionProbe
from ddtrace.debugging._probe.model import MetricLineProbe
from ddtrace.debugging._probe.model import MetricProbeKind
from ddtrace.debugging._probe.model import MetricProbeMixin
from ddtrace.debugging._signal.log import LogSignal
from ddtrace.debugging._signal.model import probe_to_signal
from ddtrace.internal.metrics import Metrics


@dataclass
class MetricSample(LogSignal):
    """Wrapper for making a metric sample"""

    meter: Metrics.Meter = field(default_factory=lambda: probe_metrics.get_meter("probe"))

    def enter(self, scope) -> None:
        self.sample(scope)

    def exit(self, retval, exc_info, duration, scope) -> None:
        self.sample(scope)

    def line(self, scope) -> None:
        self.sample(scope)

    def sample(self, scope) -> None:
        tags = self.probe.tags
        probe = cast(MetricProbeMixin, self.probe)

        assert probe.kind is not None and probe.name is not None  # nosec

        value = float(probe.value(scope)) if probe.value is not None else 1

        # TODO[perf]: We know the tags in advance so we can avoid the
        # list comprehension.
        if probe.kind == MetricProbeKind.COUNTER:
            self.meter.increment(probe.name, value, tags)
        elif probe.kind == MetricProbeKind.GAUGE:
            self.meter.gauge(probe.name, value, tags)
        elif probe.kind == MetricProbeKind.HISTOGRAM:
            self.meter.histogram(probe.name, value, tags)
        elif probe.kind == MetricProbeKind.DISTRIBUTION:
            self.meter.distribution(probe.name, value, tags)

    @property
    def message(self) -> Optional[str]:
        return f"Evaluation errors for probe id {self.probe.probe_id}" if self.errors else None

    def has_message(self) -> bool:
        return bool(self.errors)


@probe_to_signal.register
def _(probe: MetricFunctionProbe, frame, thread, trace_context, meter):
    return MetricSample(probe=probe, frame=frame, thread=thread, trace_context=trace_context, meter=meter)


@probe_to_signal.register
def _(probe: MetricLineProbe, frame, thread, trace_context, meter):
    return MetricSample(probe=probe, frame=frame, thread=thread, trace_context=trace_context, meter=meter)
