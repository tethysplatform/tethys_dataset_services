__author__ = "swainn"

try:
    from ._version import version as __version__
except ImportError:
    # Fallback for development environments
    from setuptools_scm import get_version
    __version__ = get_version(root='..', relative_to=__file__)
