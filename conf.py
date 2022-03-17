master_doc = "README"
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosectionlabel",
    "sphinx.ext.doctest",
    "sphinx.ext.intersphinx",
    "sphinx.ext.napoleon",
    "sphinx_rtd_theme",
]
project = "beaver"
napoleon_custom_sections = [("Returns", "params_style")]
html_theme = "sphinx_rtd_theme"
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
}
nitpick_ignore = [
    ("py:class", "asyncio.locks.Semaphore"),
    ("py:func", "asyncio.subprocess.create_subprocess_exec"),
    ("py:func", "asyncio.subprocess.create_subprocess_shell"),
]
add_module_names = False
autosectionlabel_prefix_document = True
