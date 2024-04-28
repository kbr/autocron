import nox


PYTHON_TEST_VERSIONS = ("3.8", "3.9", "3.10", "3.11", "3.12", "3.13")
PYTHON_DEVELOPMENT_VERSION = "3.11"


# for position arguments run:
# $ nox -s pytest-3.11 -- testfile
@nox.session(python=PYTHON_TEST_VERSIONS)
def pytest(session):
    session.install("-e", ".")
    session.install("pytest")
    if session.posargs:
        testfiles = [f"tests/{f}.py" for f in session.posargs]
    else:
        testfiles = ["tests"]
    session.run("pytest", *testfiles)


@nox.session
def pylint(session):
    session.install("-e", ".")
    session.install("pylint")
    session.run("pylint", "autocron")


@nox.session
def ruff(session):
    # local development setup:
    # nox runs from a separate conda environment with also ruff installed.
    # so ruff runs external and no session.install() calls are necessary.
    session.run("ruff", "check", "autocron", external=True)


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
