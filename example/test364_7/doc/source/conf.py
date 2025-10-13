# Configuration file for the Sphinx documentation builder.

# -- Project information

project = 'mitum-dop7'
copyright = '2025, Spike^ekipS'
author = 'spikeekips@gmail.com'

release = '0.1'
version = '0.1.0'

import datetime
version = datetime.datetime.now().strftime("%Y%m%d.%H%m%S")
released_at = datetime.datetime.now().isoformat()
today_fmt = '%Y-%m-%dT%H:%M:%S%Z'

rst_epilog = '''
.. |released_at| replace:: %s
''' % (released_at,)
highlight_language = ('text')

# -- General configuration

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.doctest',
    'sphinx.ext.duration',
    'sphinx.ext.intersphinx',
    'sphinx_rtd_theme',
    'sphinxnotes.strike',
]

intersphinx_mapping = {
    'python': ('https://docs.python.org/3/', None),
    'sphinx': ('https://www.sphinx-doc.org/en/master/', None),
}
intersphinx_disabled_domains = ['std']

templates_path = ['_templates']

# -- Options for HTML output

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']
html_css_files = ['custom.css']

# -- Options for EPUB output
epub_show_urls = 'footnote'
