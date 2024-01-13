import nox


PYTHON_TEST_VERSIONS = ("3.8", "3.9", "3.10", "3.11", "3.12")
PYTHON_DEVELOPMENT_VERSION = "3.11"


@nox.session(python=PYTHON_TEST_VERSIONS)
def test(session):
    session.install("-e", ".")
    if session.posargs:
        testfiles = [f"tests/{f}.py" for f in session.posargs]
    else:
        testfiles = []
    session.run("python", "-m", "unittest", *testfiles)


@nox.session
def lint(session):
    session.install("-e", ".")
    session.install("pylint")
    session.run("pylint", "autocron")


@nox.session(python=PYTHON_DEVELOPMENT_VERSION)
def build(session):
    session.install("-e", ".")
    session.run("python", "setup.py", "sdist", "bdist_wheel")


@nox.session(name="check-twine", python=PYTHON_DEVELOPMENT_VERSION)
def check_twine(session):
    session.install("-e", ".")
    session.install("twine")
    session.run("twine", "check", "dist/*")


@nox.session(name="upload-to-pypi", python=PYTHON_DEVELOPMENT_VERSION)
def uppload_to_pypi(session):
    session.install("-e", ".")
    session.install("twine")
    session.run("twine", "upload", "dist/*")  #, "--verbose")

@nox.session
def sphinx(session):
    session.install("-e", ".")
    session.install("pip-tools==7.3.0")
    session.run("pip-compile", "--strip-extras", "-q", "docs/requirements/requirements.in")
    session.install("-r", "docs/requirements/requirements.txt")
    session.run("sphinx-build", "docs", "docs/build")
