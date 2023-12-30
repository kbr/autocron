"""
Configuration file for the Sphinx documentation builder.

For a full list of configuration settings see the documentation:
http://www.sphinx-doc.org/en/master/config
"""

from datetime import date
# import autocron


project = "autocron"
# version = autocron.__version__

copyright = '2016 - {}, Klaus Bremer'.format(date.today().year)
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


