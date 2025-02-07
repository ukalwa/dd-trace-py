#!/usr/bin/env python3

import ast
import codecs
import os
from sys import builtin_module_names
from sys import version_info
import textwrap
from types import ModuleType
from typing import Optional
from typing import Text
from typing import Tuple

from ddtrace.appsec._constants import IAST
from ddtrace.appsec._python_info.stdlib import _stdlib_for_python_version
from ddtrace.internal.logger import get_logger
from ddtrace.internal.module import origin
from ddtrace.internal.utils.formats import asbool

from .visitor import AstVisitor


_VISITOR = AstVisitor()

_PREFIX = IAST.PATCH_ADDED_SYMBOL_PREFIX

# Prefixes for modules where IAST patching is allowed
IAST_ALLOWLIST: Tuple[Text, ...] = ("tests.appsec.iast.",)
IAST_DENYLIST: Tuple[Text, ...] = (
    "flask.",
    "werkzeug.",
    "aiohttp._helpers.",
    "aiohttp._http_parser.",
    "aiohttp._http_writer.",
    "aiohttp._websocket.",
    "aiohttp.log.",
    "aiohttp.tcp_helpers.",
    "asyncio.base_events.",
    "asyncio.base_futures.",
    "asyncio.base_subprocess.",
    "asyncio.base_tasks.",
    "asyncio.constants.",
    "asyncio.coroutines.",
    "asyncio.events.",
    "asyncio.exceptions.",
    "asyncio.futures.",
    "asyncio.locks.",
    "asyncio.log.",
    "asyncio.protocols.",
    "asyncio.queues.",
    "asyncio.runners.",
    "asyncio.selector_events.",
    "asyncio.staggered.",
    "asyncio.subprocess.",
    "asyncio.tasks.",
    "asyncio.threads.",
    "asyncio.transports.",
    "asyncio.trsock.",
    "asyncio.unix_events.",
    "attr._config.",
    "attr._next_gen.",
    "attr.filters.",
    "attr.setters.",
    "backports.",
    "boto3.docs.docstring.",
    "boto3.s3.",
    "botocore.docs.bcdoc.",
    "botocore.retries.",
    "botocore.vendored.requests.",
    "brotli.",
    "brotlicffi.",
    "cchardet.",
    "certifi.",
    "cffi.",
    "chardet.big5freq.",
    "chardet.big5prober.",
    "chardet.charsetgroupprober.",
    "chardet.cp949prober.",
    "chardet.enums.",
    "chardet.escsm.",
    "chardet.eucjpprober.",
    "chardet.euckrfreq.",
    "chardet.euckrprober.",
    "chardet.euctwfreq.",
    "chardet.euctwprober.",
    "chardet.gb2312freq.",
    "chardet.gb2312prober.",
    "chardet.hebrewprober.",
    "chardet.jisfreq.",
    "chardet.langbulgarianmodel.",
    "chardet.langgreekmodel.",
    "chardet.langhebrewmodel.",
    "chardet.langrussianmodel.",
    "chardet.langthaimodel.",
    "chardet.langturkishmodel.",
    "chardet.mbcsgroupprober.",
    "chardet.mbcssm.",
    "chardet.sbcharsetprober.",
    "chardet.sbcsgroupprober.",
    "charset_normalizer.",
    "click.",
    "cmath.",
    "colorama.",
    "concurrent.futures.",
    "configparser.",
    "coreschema.",
    "crispy_forms.",
    "dateutil.",
    "defusedxml.",
    "difflib.",
    "dill.info.",
    "dill.settings.",
    "django.apps.config.",
    "django.apps.registry.",
    "django.conf.",
    "django.contrib.admin.actions.",
    "django.contrib.admin.admin.",
    "django.contrib.admin.apps.",
    "django.contrib.admin.checks.",
    "django.contrib.admin.decorators.",
    "django.contrib.admin.exceptions.",
    "django.contrib.admin.helpers.",
    "django.contrib.admin.image_formats.",
    "django.contrib.admin.options.",
    "django.contrib.admin.sites.",
    "django.contrib.admin.templatetags.",
    "django.contrib.admin.views.autocomplete.",
    "django.contrib.admin.views.decorators.",
    "django.contrib.admin.views.main.",
    "django.contrib.admin.wagtail_hooks.",
    "django.contrib.admin.widgets.",
    "django.contrib.admindocs.utils.",
    "django.contrib.admindocs.views.",
    "django.contrib.auth.admin.",
    "django.contrib.auth.apps.",
    "django.contrib.auth.backends.",
    "django.contrib.auth.base_user.",
    "django.contrib.auth.checks.",
    "django.contrib.auth.context_processors.",
    "django.contrib.auth.decorators.",
    "django.contrib.auth.hashers.",
    "django.contrib.auth.image_formats.",
    "django.contrib.auth.management.",
    "django.contrib.auth.middleware.",
    "django.contrib.auth.password_validation.",
    "django.contrib.auth.signals.",
    "django.contrib.auth.templatetags.",
    "django.contrib.auth.validators.",
    "django.contrib.auth.wagtail_hooks.",
    "django.contrib.contenttypes.admin.",
    "django.contrib.contenttypes.apps.",
    "django.contrib.contenttypes.checks.",
    "django.contrib.contenttypes.fields.",
    "django.contrib.contenttypes.forms.",
    "django.contrib.contenttypes.image_formats.",
    "django.contrib.contenttypes.management.",
    "django.contrib.contenttypes.models.",
    "django.contrib.contenttypes.templatetags.",
    "django.contrib.contenttypes.views.",
    "django.contrib.contenttypes.wagtail_hooks.",
    "django.contrib.humanize.templatetags.",
    "django.contrib.messages.admin.",
    "django.contrib.messages.api.",
    "django.contrib.messages.apps.",
    "django.contrib.messages.constants.",
    "django.contrib.messages.context_processors.",
    "django.contrib.messages.image_formats.",
    "django.contrib.messages.middleware.",
    "django.contrib.messages.storage.",
    "django.contrib.messages.templatetags.",
    "django.contrib.messages.utils.",
    "django.contrib.messages.wagtail_hooks.",
    "django.contrib.sessions.admin.",
    "django.contrib.sessions.apps.",
    "django.contrib.sessions.backends.",
    "django.contrib.sessions.base_session.",
    "django.contrib.sessions.exceptions.",
    "django.contrib.sessions.image_formats.",
    "django.contrib.sessions.middleware.",
    "django.contrib.sessions.templatetags.",
    "django.contrib.sessions.wagtail_hooks.",
    "django.contrib.sites.",
    "django.contrib.staticfiles.admin.",
    "django.contrib.staticfiles.apps.",
    "django.contrib.staticfiles.checks.",
    "django.contrib.staticfiles.finders.",
    "django.contrib.staticfiles.image_formats.",
    "django.contrib.staticfiles.models.",
    "django.contrib.staticfiles.storage.",
    "django.contrib.staticfiles.templatetags.",
    "django.contrib.staticfiles.utils.",
    "django.contrib.staticfiles.wagtail_hooks.",
    "django.core.cache.backends.",
    "django.core.cache.utils.",
    "django.core.checks.async_checks.",
    "django.core.checks.caches.",
    "django.core.checks.compatibility.",
    "django.core.checks.compatibility.django_4_0.",
    "django.core.checks.database.",
    "django.core.checks.files.",
    "django.core.checks.messages.",
    "django.core.checks.model_checks.",
    "django.core.checks.registry.",
    "django.core.checks.security.",
    "django.core.checks.security.base.",
    "django.core.checks.security.csrf.",
    "django.core.checks.security.sessions.",
    "django.core.checks.templates.",
    "django.core.checks.translation.",
    "django.core.checks.urls",
    "django.core.exceptions.",
    "django.core.mail.",
    "django.core.management.base.",
    "django.core.management.color.",
    "django.core.management.sql.",
    "django.core.paginator.",
    "django.core.signing.",
    "django.core.validators.",
    "django.dispatch.dispatcher.",
    "django.template.autoreload.",
    "django.template.backends.",
    "django.template.base.",
    "django.template.context.",
    "django.template.context_processors.",
    "django.template.defaultfilters.",
    "django.template.defaulttags.",
    "django.template.engine.",
    "django.template.exceptions.",
    "django.template.library.",
    "django.template.loader.",
    "django.template.loader_tags.",
    "django.template.loaders.",
    "django.template.response.",
    "django.template.smartif.",
    "django.template.utils.",
    "django.templatetags.",
    "django.test.",
    "django.urls.base.",
    "django.urls.conf.",
    "django.urls.converters.",
    "django.urls.exceptions.",
    "django.urls.resolvers.",
    "django.urls.utils.",
    "django.utils.",
    "django_filters.compat.",
    "django_filters.conf.",
    "django_filters.constants.",
    "django_filters.exceptions.",
    "django_filters.fields.",
    "django_filters.filters.",
    "django_filters.filterset.",
    "django_filters.rest_framework.",
    "django_filters.rest_framework.backends.",
    "django_filters.rest_framework.filters.",
    "django_filters.rest_framework.filterset.",
    "django_filters.utils.",
    "django_filters.widgets.",
    "crypto.",  # This module is patched by the IAST patch methods, propagation is not needed
    "deprecated.",
    "api_pb2.",  # Patching crashes with these auto-generated modules, propagation is not needed
    "api_pb2_grpc.",  # Patching crashes with these auto-generated modules, propagation is not needed
    "asyncpg.pgproto.",
    "blinker.",
    "bytecode.",
    "cattrs.",
    "ddsketch.",
    "ddtrace.",
    "envier.",
    "exceptiongroup.",
    "freezegun.",  # Testing utilities for time manipulation
    "hypothesis.",  # Testing utilities
    "importlib_metadata.",
    "inspect.",  # this package is used to get the stack frames, propagation is not needed
    "itsdangerous.",
    "moto.",  # used for mocking AWS, propagation is not needed
    "opentelemetry-api.",
    "packaging.",
    "pip.",
    "pkg_resources.",
    "pluggy.",
    "protobuf.",
    "psycopg.",  # PostgreSQL adapter for Python (v3)
    "_psycopg.",  # PostgreSQL adapter for Python (v3)
    "psycopg2.",  # PostgreSQL adapter for Python (v2)
    "pycparser.",  # this package is called when a module is imported, propagation is not needed
    "pytest.",  # Testing framework
    "_pytest.",
    "setuptools.",
    "sklearn.",  # Machine learning library
    "sqlalchemy.orm.interfaces.",  # Performance optimization
    "typing_extensions.",
    "unittest.mock.",
    "uvloop.",
    "urlpatterns_reverse.tests.",  # assertRaises eat exceptions in native code, so we don't call the original function
    "wrapt.",
    "zipp.",
    # This is a workaround for Sanic failures:
    "websocket.",
    "h11.",
    "aioquic.",
    "httptools.",
    "sniffio.",
    "sanic.",
    "rich.",
    "httpx.",
    "websockets.",
    "uvicorn.",
    "anyio.",
    "httpcore.",
    "google.auth.",
    "googlecloudsdk.",
    "umap.",
    "pynndescent.",
    "numba.",
)


if IAST.PATCH_MODULES in os.environ:
    IAST_ALLOWLIST += tuple(os.environ[IAST.PATCH_MODULES].split(IAST.SEP_MODULES))

if IAST.DENY_MODULES in os.environ:
    IAST_DENYLIST += tuple(os.environ[IAST.DENY_MODULES].split(IAST.SEP_MODULES))


ENCODING = ""

log = get_logger(__name__)


def get_encoding(module_path: Text) -> Text:
    """
    First tries to detect the encoding for the file,
    otherwise, returns global encoding default
    """
    global ENCODING
    if not ENCODING:
        try:
            ENCODING = codecs.lookup("utf-8-sig").name
        except LookupError:
            ENCODING = codecs.lookup("utf-8").name
    return ENCODING


_NOT_PATCH_MODULE_NAMES = _stdlib_for_python_version() | set(builtin_module_names)


def _in_python_stdlib(module_name: str) -> bool:
    return module_name.split(".")[0].lower() in [x.lower() for x in _NOT_PATCH_MODULE_NAMES]


def _should_iast_patch(module_name: Text) -> bool:
    """
    select if module_name should be patch from the longest prefix that match in allow or deny list.
    if a prefix is in both list, deny is selected.
    """
    # TODO: A better solution would be to migrate the original algorithm to C++:
    # max_allow = max((len(prefix) for prefix in IAST_ALLOWLIST if module_name.startswith(prefix)), default=-1)
    # max_deny = max((len(prefix) for prefix in IAST_DENYLIST if module_name.startswith(prefix)), default=-1)
    # diff = max_allow - max_deny
    # return diff > 0 or (diff == 0 and not _in_python_stdlib_or_third_party(module_name))
    dotted_module_name = module_name.lower() + "."
    if dotted_module_name.startswith(IAST_ALLOWLIST):
        log.debug("IAST: allowing %s. it's in the IAST_ALLOWLIST", module_name)
        return True
    if dotted_module_name.startswith(IAST_DENYLIST):
        log.debug("IAST: denying %s. it's in the IAST_DENYLIST", module_name)
        return False
    if _in_python_stdlib(module_name):
        log.debug("IAST: denying %s. it's in the _in_python_stdlib", module_name)
        return False
    return True


def visit_ast(
    source_text: Text,
    module_path: Text,
    module_name: Text = "",
) -> Optional[ast.Module]:
    parsed_ast = ast.parse(source_text, module_path)
    _VISITOR.update_location(filename=module_path, module_name=module_name)
    modified_ast = _VISITOR.visit(parsed_ast)

    if not _VISITOR.ast_modified:
        return None

    ast.fix_missing_locations(modified_ast)
    return modified_ast


_DIR_WRAPPER = textwrap.dedent(
    f"""


def {_PREFIX}dir():
    orig_dir = globals().get("{_PREFIX}orig_dir__")

    if orig_dir:
        # Use the original __dir__ method and filter the results
        results = [name for name in orig_dir() if not name.startswith("{_PREFIX}")]
    else:
        # List names from the module's __dict__ and filter out the unwanted names
        results = [
            name for name in globals()
            if not (name.startswith("{_PREFIX}") or name == "__dir__")
        ]

    return results

def {_PREFIX}set_dir_filter():
    if "__dir__" in globals():
        # Store the original __dir__ method
        globals()["{_PREFIX}orig_dir__"] = __dir__

    # Replace the module's __dir__ with the custom one
    globals()["__dir__"] = {_PREFIX}dir

{_PREFIX}set_dir_filter()

    """
)


def astpatch_module(module: ModuleType) -> Tuple[str, Optional[ast.Module]]:
    module_name = module.__name__

    module_origin = origin(module)
    if module_origin is None:
        log.debug("astpatch_source couldn't find the module: %s", module_name)
        return "", None

    module_path = str(module_origin)
    try:
        if module_origin.stat().st_size == 0:
            # Don't patch empty files like __init__.py
            log.debug("empty file: %s", module_path)
            return "", None
    except OSError:
        log.debug("astpatch_source couldn't find the file: %s", module_path, exc_info=True)
        return "", None

    # Get the file extension, if it's dll, os, pyd, dyn, dynlib: return
    # If its pyc or pyo, change to .py and check that the file exists. If not,
    # return with warning.
    _, module_ext = os.path.splitext(module_path)

    if module_ext.lower() not in {".pyo", ".pyc", ".pyw", ".py"}:
        # Probably native or built-in module
        log.debug("extension not supported: %s for: %s", module_ext, module_path)
        return "", None

    with open(module_path, "r", encoding=get_encoding(module_path)) as source_file:
        try:
            source_text = source_file.read()
        except UnicodeDecodeError:
            log.debug("unicode decode error for file: %s", module_path, exc_info=True)
            return "", None

    if len(source_text.strip()) == 0:
        # Don't patch empty files like __init__.py
        log.debug("empty file: %s", module_path)
        return "", None

    if not asbool(os.environ.get(IAST.ENV_NO_DIR_PATCH, "false")) and version_info > (3, 7):
        # Add the dir filter so __ddtrace stuff is not returned by dir(module)
        # does not work in 3.7 because it enters into infinite recursion
        source_text += _DIR_WRAPPER

    new_ast = visit_ast(
        source_text,
        module_path,
        module_name=module_name,
    )
    if new_ast is None:
        log.debug("file not ast patched: %s", module_path)
        return "", None

    return module_path, new_ast
