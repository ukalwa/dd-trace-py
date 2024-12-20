from enum import IntEnum
import re


SUITE = "suite"
TEST = "test"
BENCHMARK = "benchmark"

EVENT_TYPE = "type"


# Test Session ID
SESSION_ID = "test_session_id"

# Test Module ID
MODULE_ID = "test_module_id"

# Test Suite ID
SUITE_ID = "test_suite_id"

# Event type signals for CI Visibility
SESSION_TYPE = "test_session_end"

MODULE_TYPE = "test_module_end"

SUITE_TYPE = "test_suite_end"

# Agentless and EVP-specific constants
COVERAGE_TAG_NAME = "test.coverage"

EVP_PROXY_AGENT_BASE_PATH = "/evp_proxy/v2"
EVP_PROXY_AGENT_ENDPOINT = "{}/api/v2/citestcycle".format(EVP_PROXY_AGENT_BASE_PATH)
AGENTLESS_ENDPOINT = "api/v2/citestcycle"
AGENTLESS_COVERAGE_ENDPOINT = "api/v2/citestcov"
AGENTLESS_API_KEY_HEADER_NAME = "dd-api-key"
AGENTLESS_APP_KEY_HEADER_NAME = "dd-application-key"
EVP_NEEDS_APP_KEY_HEADER_NAME = "X-Datadog-NeedsAppKey"
EVP_NEEDS_APP_KEY_HEADER_VALUE = "true"
EVP_PROXY_COVERAGE_ENDPOINT = "{}/{}".format(EVP_PROXY_AGENT_BASE_PATH, AGENTLESS_COVERAGE_ENDPOINT)
EVP_SUBDOMAIN_HEADER_API_VALUE = "api"
EVP_SUBDOMAIN_HEADER_COVERAGE_VALUE = "citestcov-intake"
EVP_SUBDOMAIN_HEADER_EVENT_VALUE = "citestcycle-intake"
EVP_SUBDOMAIN_HEADER_NAME = "X-Datadog-EVP-Subdomain"
AGENTLESS_BASE_URL = "https://citestcycle-intake"
AGENTLESS_COVERAGE_BASE_URL = "https://citestcov-intake"
AGENTLESS_DEFAULT_SITE = "datadoghq.com"
GIT_API_BASE_PATH = "/api/v2/git"
SETTING_ENDPOINT = "/api/v2/libraries/tests/services/setting"
SKIPPABLE_ENDPOINT = "/api/v2/ci/tests/skippable"
UNIQUE_TESTS_ENDPOINT = "/api/v2/ci/libraries/tests"
DETAILED_TESTS_ENDPOINT = "/api/v2/ci/libraries/tests/detailed"

# Intelligent Test Runner constants
ITR_UNSKIPPABLE_REASON = "datadog_itr_unskippable"
SKIPPED_BY_ITR_REASON = "Skipped by Datadog Intelligent Test Runner"
ITR_CORRELATION_ID_TAG_NAME = "itr_correlation_id"

# Tracer configuration defaults:
TRACER_PARTIAL_FLUSH_MIN_SPANS = 1


class REQUESTS_MODE(IntEnum):
    AGENTLESS_EVENTS = 0
    EVP_PROXY_EVENTS = 1
    TRACES = 2


# Miscellaneous constants
CUSTOM_CONFIGURATIONS_PREFIX = "test.configuration"

CIVISIBILITY_LOG_FILTER_RE = re.compile(
    "|".join(
        [
            r"^ddtrace\.contrib\.(coverage|pytest|unittest)",
            r"ddtrace\.internal\.(ci_visibility|gitmetadata).*",
            r"ddtrace\.ext\.(git|ci_visibility|test)",
        ]
    )
)

CIVISIBILITY_SPAN_TYPE = "ci_visibility"

# EFD and auto retries
TEST_IS_NEW = "test.is_new"
TEST_IS_RETRY = "test.is_retry"
TEST_IS_QUARANTINED = "test.quarantine.is_quarantined"
TEST_EFD_ABORT_REASON = "test.early_flake.abort_reason"
TEST_EFD_ENABLED = "test.early_flake.enabled"
TEST_HAS_FAILED_ALL_RETRIES = "test.has_failed_all_retries"

TEST_SESSION_QUARANTINE_ENABLED = "test_session.quarantine.enabled"
