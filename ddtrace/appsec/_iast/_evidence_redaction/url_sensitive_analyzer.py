import re

from ddtrace.internal.logger import get_logger


log = get_logger(__name__)
AUTHORITY_PATTERN = re.compile(r"https?://([^@]+)(?=@)", re.IGNORECASE | re.MULTILINE)
QUERY_FRAGMENT_PATTERN = re.compile(r"[?#&]([^=&;]+)=([^?#&]+)", re.IGNORECASE | re.MULTILINE)


def find_authority(ranges, evidence):
    regex_result = AUTHORITY_PATTERN.search(evidence.value)
    while regex_result is not None:
        if isinstance(regex_result.group(1), str):
            start = regex_result.start(1)
            end = regex_result.start(1) + (len(regex_result.group(1)))
            ranges.append({"start": start, "end": end})

        regex_result = AUTHORITY_PATTERN.search(evidence.value, regex_result.end())


def find_query_fragment(ranges, evidence):
    regex_result = QUERY_FRAGMENT_PATTERN.search(evidence.value)
    while regex_result is not None:
        if isinstance(regex_result.group(2), str):
            start = regex_result.start(2)
            end = regex_result.start(2) + (len(regex_result.group(2)))
            ranges.append({"start": start, "end": end})
        regex_result = QUERY_FRAGMENT_PATTERN.search(evidence.value, regex_result.end())


def url_sensitive_analyzer(evidence, name_pattern=None, value_pattern=None):
    ranges = []
    find_authority(ranges, evidence)
    find_query_fragment(ranges, evidence)
    return ranges
