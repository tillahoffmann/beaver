🦫 Beaver
=========

.. image:: https://github.com/tillahoffmann/beaver/actions/workflows/main.yml/badge.svg
  :target: https://github.com/tillahoffmann/beaver/actions/workflows/main.yml
.. image:: https://readthedocs.org/projects/beaver/badge/?version=latest
  :target: https://beaver.readthedocs.io/en/latest/?badge=latest
.. image:: https://img.shields.io/pypi/v/beaver-build
  :target: https://pypi.org/project/beaver-build

Beaver is a minimal build system geared towards scientific programming and reproducibility. It uses the `python programming language <https://en.wikipedia.org/wiki/Python_(programming_language)>`__ to express how transforms generate outputs from inputs. If you're familiar with python, using Beaver couldn't be easier, as we will demonstrate by example.

.. testcode::

  # A simple example (saved as `beaver.py`) to generate `output.txt` with content `hello`.
  import beaver_build as bb

  transform = bb.Shell(outputs="output.txt", inputs=None, cmd="echo hello > output.txt")

Executing Beaver from the command line generates the desired output.

.. code-block:: bash

  $ beaver output.txt
  🦫 INFO: 🟡 artifacts [output.txt] are stale; schedule transform
  🦫 INFO: ⚙️ execute shell command `echo hello > output.txt`
  🦫 INFO: ✅ generated artifacts [output.txt]
  $ cat output.txt
  hello

This seems like a convoluted way to write :code:`hello` to :code:`output.txt`. So what's going on? The statement :code:`bb.Shell(...)` defines a :class:`Transform <beaver_build.transforms.Transform>` that generates the :class:`Artifact <beaver_build.artifacts.Artifact>` :code:`output.txt` by executing the shell command :code:`echo hello > output.txt`. Executing :code:`beaver output.txt` asks Beaver to generate the artifact--which it gladly does.

Why should we care? Transforms can be chained by using the outputs of one as the inputs for another. Beaver ensures that all transforms are executed in the correct order and parallelizes steps where possible. These are of course the tasks of any build system, but Beaver's unique selling points are (see `Why not use ...?`_ for further details):

- users do not need to learn a `domain-specific language <https://en.wikipedia.org/wiki/Domain-specific_language>`_ but use flexible python syntax to create and chain transforms.
- new transforms can be implemented easily by inheriting from :class:`Transform <beaver_build.transforms.Transform>` and implementing the :meth:`apply <beaver_build.transforms.Transform.apply>` method.
- scheduling of operations is delegated to python's :mod:`asyncio` package which both minimizes the potential for bugs (compared with a custom implementation) and simplifies parallelization.
- defining artifacts and transforms as python objects allows for extensive introspection, such as visualization of the induced `directed acyclic <https://en.wikipedia.org/wiki/Directed_acyclic_graph>`_ `bipartite graph <https://en.wikipedia.org/wiki/Bipartite_graph>`_.

Other features include:

- incremental builds based on artifact digests--whether artifacts are files or not.

Why not use ...?
----------------

- `ant <https://en.wikipedia.org/wiki/Apache_Ant>`_ uses relatively verbose XML syntax and limited in its flexibility, e.g. transforms cannot be easily generated on the fly.
- `bazel <https://en.wikipedia.org/wiki/Bazel_(software)>`_ focuses on speed and correctness--which it does extremely well. Bazel achieves these goals by `"[taking] some power out of the hands of engineers" <https://bazel.build/basics/task-based-builds#difficulty_maintaining_and_debugging_scripts>`_. This is a good compromise for production systems, but, for scientific applications, we want to retain a high degree of flexibility.
- `make <https://en.wikipedia.org/wiki/Make_(software)>`_ is a trusted build tool, but Makefiles can quickly become complex and `modularizing is difficult <https://accu.org/journals/overload/14/71/miller_2004/>`_.
- `maven <https://en.wikipedia.org/wiki/Apache_Maven>`_ is primarily Java focused and relies on conventions to generate artifacts. Well-established conventions are essential for software development, especially in large teams, but are often lacking in the context of investigating a new scientific problem.
- `pydoit <https://pydoit.org>`_ uses standard python syntax to collect task metadata akin to test discovery in `pytest <https://docs.pytest.org>`_. However, :code:`dodo.py` files are sometimes difficult to read because the code does not directly express the tasks to execute.
- `snakemake <https://snakemake.github.io>`_ uses a non-standard python syntax, steepening the learning curve.

.. toctree::
  :hidden:

  docs/interface
