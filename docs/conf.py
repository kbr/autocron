"""
Configuration file for the Sphinx documentation builder.

For a full list of configuration settings see the documentation:
http://www.sphinx-doc.org/en/master/config
"""

import os
import re
from datetime import date
from pathlib import Path


def get_version():
    path = Path("..") / "autocron" / "__init__.py"
    path = path.resolve()  # make it absolute
    with open(path) as file:
        content = file.read()
    mo = re.search(r'\n\s*__version__\s*=\s*[\'"]([^\'"]*)[\'"]', content)
    if mo:
        return mo.group(1)
    raise RuntimeError(f"Unable to find version string in {path}")


project = "autocron"
release = get_version()
version = '.'.join(release.split('.')[:-1])
html_title = f"{project}<br /><small>{release}</small>"

copyright = '2023 - {}, Klaus Bremer'.format(date.today().year)
author = 'Klaus Bremer'

extensions = [
    'sphinx.ext.autodoc',
]

source_suffix = '.rst'
master_doc = 'index'
language = 'en'

exclude_patterns = ['Thumbs.db', '.DS_Store']
pygments_style = "default"

html_theme = 'furo'
html_static_path = ['_static']
html_css_files = ['autocron.css']
html_logo = '_static/cheers-to-autocron-200x200.jpg'


# let the template engine (Jinja2) know if the code runs on readthedocs:
if os.environ.get("READTHEDOCS", "") == "True":
    if "html_context" not in globals():
        html_context = {}
    html_context["READTHEDOCS"] = True
