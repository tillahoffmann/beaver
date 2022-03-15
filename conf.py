master_doc = "README"
extensions = [
    "sphinx.ext.doctest",
    "sphinx.ext.napoleon",
    "sphinx.ext.autodoc",
    "sphinx_rtd_theme",
]
project = "beaver"
napoleon_custom_sections = [("Returns", "params_style")]
html_theme = "sphinx_rtd_theme"
