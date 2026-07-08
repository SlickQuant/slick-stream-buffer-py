from setuptools import setup, Extension
import sys
import re
from pathlib import Path

# Read version from slick_stream_buffer_py.py
def get_version():
    with open('slick_stream_buffer_py.py', 'r', encoding='utf-8') as f:
        content = f.read()
        match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]", content, re.M)
        if match:
            return match.group(1)
        raise RuntimeError("Unable to find version string.")

# Read long description from README
def get_long_description():
    readme_path = Path(__file__).parent / 'README.md'
    if readme_path.exists():
        with open(readme_path, 'r', encoding='utf-8') as f:
            return f.read()
    return ''

# Define the C++ extension module (uses std::atomic for cross-platform atomic ops)
ssb_atomic_ops_ext = Extension(
    'ssb_atomic_ops_ext',
    sources=['ssb_atomic_ops_ext.cpp'],
    include_dirs=[],
    libraries=[],
    extra_compile_args=['/std:c++17', '/O2'] if sys.platform == 'win32' else ['-std=c++17', '-O2'],
)

setup(
    name='slick-stream-buffer-py',
    version=get_version(),
    description='Lock-free SPMC byte stream buffer with C++ interoperability via shared memory',
    long_description=get_long_description(),
    long_description_content_type='text/markdown',
    author='Slick Quant',
    author_email='slickquant@slickquant.com',
    url='https://github.com/SlickQuant/slick-stream-buffer-py',
    project_urls={
        'Bug Tracker': 'https://github.com/SlickQuant/slick-stream-buffer-py/issues',
        'Documentation': 'https://github.com/SlickQuant/slick-stream-buffer-py#readme',
        'Source Code': 'https://github.com/SlickQuant/slick-stream-buffer-py',
    },
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: System :: Distributed Computing',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Programming Language :: C++',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: POSIX :: Linux',
        'Operating System :: MacOS',
    ],
    keywords='stream-buffer, lock-free, atomic, shared-memory, ipc, multiprocessing, spmc',
    ext_modules=[ssb_atomic_ops_ext],
    py_modules=['slick_stream_buffer_py', 'ssb_atomic_ops'],
    python_requires='>=3.8',
    license='MIT',
)
