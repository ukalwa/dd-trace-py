import os

import mock
import ddtrace.llmobs
import ddtrace.llmobs._evaluators
import ddtrace.llmobs._evaluators.ragas
import pytest

import ddtrace
from ddtrace.internal.utils.http import Response
from ddtrace.llmobs import LLMObs as llmobs_service
from tests.llmobs._utils import logs_vcr
from tests.utils import DummyTracer
from tests.utils import override_global_config
from tests.utils import request_token


@pytest.fixture(autouse=True)
def vcr_logs(request):
    marks = [m for m in request.node.iter_markers(name="vcr_logs")]
    assert len(marks) < 2
    if marks:
        mark = marks[0]
        cass = mark.kwargs.get("cassette", request_token(request).replace(" ", "_").replace(os.path.sep, "_"))
        with logs_vcr.use_cassette("%s.yaml" % cass):
            yield
    else:
        yield


def pytest_configure(config):
    config.addinivalue_line("markers", "vcr_logs: mark test to use recorded request/responses")


@pytest.fixture
def mock_llmobs_span_writer():
    patcher = mock.patch("ddtrace.llmobs._llmobs.LLMObsSpanWriter")
    LLMObsSpanWriterMock = patcher.start()
    m = mock.MagicMock()
    LLMObsSpanWriterMock.return_value = m
    yield m
    patcher.stop()


@pytest.fixture
def mock_llmobs_span_agentless_writer():
    patcher = mock.patch("ddtrace.llmobs._llmobs.LLMObsSpanWriter")
    LLMObsSpanWriterMock = patcher.start()
    m = mock.MagicMock()
    LLMObsSpanWriterMock.return_value = m
    yield m
    patcher.stop()


@pytest.fixture
def mock_llmobs_eval_metric_writer():
    patcher = mock.patch("ddtrace.llmobs._llmobs.LLMObsEvalMetricWriter")
    LLMObsEvalMetricWriterMock = patcher.start()
    m = mock.MagicMock()
    LLMObsEvalMetricWriterMock.return_value = m
    yield m
    patcher.stop()


@pytest.fixture
def mock_llmobs_ragas_evaluator():
    patcher = mock.patch("ddtrace.llmobs._evaluations.ragas.faithfulness.evaluator.run")
    RagasEvaluator = patcher.start()
    m = mock.MagicMock()
    RagasEvaluator.return_value = m
    yield m
    patcher.stop()


@pytest.fixture
def mock_http_writer_send_payload_response():
    with mock.patch(
        "ddtrace.internal.writer.HTTPWriter._send_payload",
        return_value=Response(
            status=200,
            body="{}",
        ),
    ):
        yield


@pytest.fixture
def mock_http_writer_put_response_forbidden():
    with mock.patch(
        "ddtrace.internal.writer.HTTPWriter._put",
        return_value=Response(
            status=403,
            reason=b'{"errors":[{"status":"403","title":"Forbidden","detail":"API key is invalid"}]}',
        ),
    ):
        yield


@pytest.fixture
def mock_writer_logs():
    with mock.patch("ddtrace.llmobs._writer.logger") as m:
        yield m


@pytest.fixture
def mock_evaluator_logs():
    with mock.patch("ddtrace.llmobs._evaluators.runner.logger") as m:
        yield m


@pytest.fixture
def mock_http_writer_logs():
    with mock.patch("ddtrace.internal.writer.writer.log") as m:
        yield m


@pytest.fixture
def ddtrace_global_config():
    config = {}
    return config


def default_global_config():
    return {"_dd_api_key": "<not-a-real-api_key>", "_llmobs_ml_app": "unnamed-ml-app"}


@pytest.fixture
def LLMObs(mock_llmobs_span_writer, mock_llmobs_eval_metric_writer, ddtrace_global_config):
    global_config = default_global_config()
    global_config.update(ddtrace_global_config)
    with override_global_config(global_config):
        dummy_tracer = DummyTracer()
        llmobs_service.enable(_tracer=dummy_tracer)
        yield llmobs_service
        llmobs_service.disable()


@pytest.fixture
def AgentlessLLMObs(mock_llmobs_span_agentless_writer, mock_llmobs_eval_metric_writer, ddtrace_global_config):
    global_config = default_global_config()
    global_config.update(ddtrace_global_config)
    global_config.update(dict(_llmobs_agentless_enabled=True))
    with override_global_config(global_config):
        dummy_tracer = DummyTracer()
        llmobs_service.enable(_tracer=dummy_tracer)
        yield llmobs_service
        llmobs_service.disable()


@pytest.fixture
def LLMObsWithRagas(monkeypatch, mock_llmobs_span_writer, mock_llmobs_eval_metric_writer, ddtrace_global_config):
    global_config = default_global_config()
    global_config.update(ddtrace_global_config)
    monkeypatch.setenv("_DD_LLMOBS_EVALUATORS", "ragas_faithfulness")
    monkeypatch.setenv("_DD_LLMOBS_EVALUATOR_INTERVAL", "0.1")
    with override_global_config(global_config):
        dummy_tracer = DummyTracer()
        llmobs_service.enable(_tracer=dummy_tracer)
        yield llmobs_service
        llmobs_service.disable()


@pytest.fixture
def RagasFaithfulnessEvaluator(LLMObs):
    yield RagasFaithfulnessEvaluator(
        llmobs_service=LLMObs,
    )


@pytest.fixture
def mock_ragas_dependencies_not_pressent():
    previous = ddtrace.llmobs._evaluators.ragas.faithfulness.RAGAS_DEPENDENCIES_PRESENT
    ddtrace.llmobs._evaluators.ragas.faithfulness.RAGAS_DEPENDENCIES_PRESENT = False
    yield
    ddtrace.llmobs._evaluators.ragas.faithfulness.RAGAS_DEPENDENCIES_PRESENT = previous
