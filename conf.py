master_doc = "README"
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.doctest",
    "sphinx.ext.intersphinx",
    "sphinx.ext.napoleon",
    "sphinxarg.ext",
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
doctest_global_setup = "import beaver_build as bb"
