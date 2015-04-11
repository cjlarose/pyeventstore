from distutils.core import setup
setup(
    name = "pyeventstore",
    packages = ["pyeventstore"],
    version = "0.0.1",
    description = "Client library for Event Store",
    author = "Chris LaRose",
    author_email = "cjlarose@gmail.com",
    url = "https://github.com/cjlarose/pyeventstore",
    install_requires = ['aiohttp']
)
