"""Resolution of the module key of a test suite."""

import os
import sys

import mock
import pytest

from cephci.orchestration.loader import TEST_SEARCH_PATHS, TestModuleLoader


@pytest.fixture
def checkout(tmp_path):
    """A checkout holding two files of the same name in different directories."""
    for directory in ("tests", "tests/cephadm", "tests/nvmeof"):
        (tmp_path / directory).mkdir(parents=True, exist_ok=True)

    (tmp_path / "tests/cephadm/test_host.py").write_text("ran = 'cephadm'\n")
    (tmp_path / "tests/cephadm/test_shared.py").write_text("ran = 'cephadm'\n")
    (tmp_path / "tests/nvmeof/test_shared.py").write_text("ran = 'nvmeof'\n")

    return tmp_path


@pytest.mark.parametrize(
    "module, expected",
    [
        ("test_host.py", "test_host"),
        ("test_host", "test_host"),
        ("tests.cephadm.test_host", "tests.cephadm.test_host"),
        ("tests/cephadm/test_host.py", "tests.cephadm.test_host"),
    ],
)
def test_module_names(module, expected):
    assert TestModuleLoader.module_name(module) == expected


def test_search_paths_are_rooted_in_the_checkout(checkout):
    loader = TestModuleLoader(root=str(checkout))

    assert loader.search_paths[0] == os.path.join(str(checkout), "tests")
    assert len(loader.search_paths) == len(TEST_SEARCH_PATHS)


def test_install_adds_the_paths_once(checkout):
    loader = TestModuleLoader(root=str(checkout))
    original = list(sys.path)

    try:
        loader.install()
        added = len(sys.path) - len(original)
        loader.install()

        assert len(sys.path) - len(original) == added
    finally:
        sys.path[:] = original


def test_candidates_finds_every_definition(checkout):
    loader = TestModuleLoader(root=str(checkout))

    assert len(loader.candidates("test_shared")) == 2
    assert len(loader.candidates("test_host")) == 1
    assert loader.candidates("test_absent") == []


def test_ambiguous_modules_are_reported(checkout):
    loader = TestModuleLoader(root=str(checkout))

    with mock.patch("cephci.orchestration.loader.importlib") as importlib, mock.patch(
        "cephci.orchestration.loader.log"
    ) as logger:
        loader.resolve("test_shared.py")

    importlib.import_module.assert_called_once_with("test_shared")
    assert logger.warning.called
    assert "ambiguous" in logger.warning.call_args.args[0]


def test_unambiguous_modules_are_quiet(checkout):
    loader = TestModuleLoader(root=str(checkout))

    with mock.patch("cephci.orchestration.loader.importlib"), mock.patch(
        "cephci.orchestration.loader.log"
    ) as logger:
        loader.resolve("test_host.py")

    logger.warning.assert_not_called()


def test_dotted_modules_are_imported_as_given(checkout):
    loader = TestModuleLoader(root=str(checkout))

    with mock.patch("cephci.orchestration.loader.importlib") as importlib, mock.patch(
        "cephci.orchestration.loader.log"
    ) as logger:
        loader.resolve("tests.cephadm.test_host")

    importlib.import_module.assert_called_once_with("tests.cephadm.test_host")
    logger.warning.assert_not_called()


def test_a_real_module_is_importable():
    """The modules referenced by the suites of this repository can be loaded."""
    loader = TestModuleLoader()
    original = list(sys.path)

    try:
        loader.install()
        module = loader.resolve("test_cephadm.py")

        assert callable(module.run)
    finally:
        sys.path[:] = original
