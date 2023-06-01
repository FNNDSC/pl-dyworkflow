from setuptools import setup
import re

_version_re = re.compile(r"(?<=^__version__ = (\"|'))(.+)(?=\"|')")

def get_version(rel_path: str) -> str:
    """
    Searches for the ``__version__ = `` line in a source code file.

    https://packaging.python.org/en/latest/guides/single-sourcing-package-version/
    """
    with open(rel_path, 'r') as f:
        matches = map(_version_re.search, f)
        filtered = filter(lambda m: m is not None, matches)
        version = next(filtered, None)
        if version is None:
            raise RuntimeError(f'Could not find __version__ in {rel_path}')
        return version.group(0)

def get_requires(rel_path:str) -> list:
    l_packages:list     = []
    str_package:str     = ''
    with open (rel_path, 'r') as f:
        str_package     = f.readline()
        if not str_package.startswith("#") and len(str_package):
            l_packages.append(str_package)
    return l_packages

setup(
    name                = 'dyworkflow',
    version             = get_version('dyworkflow.py'),
    description         = 'A ChRIS plugin that "splits" a parent into multiple children and adds a specified workflow to each child',
    author              = 'FNNDSC',
    author_email        = 'dev@babyMRI.org',
    url                 = 'https://github.com/FNNDSC/pl-dyworkflow',
    py_modules          = ['dyworkflow'],
    install_requires    = get_requires('requirements.txt'),
    packages            = ['control', 'logic', 'state'],
    license             = 'MIT',
    entry_points        = {
        'console_scripts': [
            'dyworkflow = dyworkflow:main'
        ]
    },
    classifiers         = [
        'License :: OSI Approved :: MIT License',
        'Topic :: Scientific/Engineering',
        'Topic :: Scientific/Engineering :: Bio-Informatics',
        'Topic :: Scientific/Engineering :: Medical Science Apps.'
    ],
    extras_require      = {
        'none': [],
        'dev': [
            'pytest~=7.1'
        ]
    }
)
