Installation
============

CLI
---

Homebrew (recommended)
......................

.. code-block:: console

   $ brew install yutanagano/tap/nearust

Alternate methods
.................

Check out the releases page `releases page
<https://github.com/yutanagano/nearust/releases>`_ on the project's GitHub.

Python package
--------------

PyPI (recommended)
..................

.. code-block:: console

   $ pip install nearust

From source
...........

.. important:: 

   You must have ``rustc`` installed on your system to be able to compile the
   underlying Rust code.

.. note:: 

   Python bindings are only available from versions 0.3 and onwards.

From your Python environment, run the following replacing ``<VERSION_TAG>``
with the appropriate version specifier (e.g. ``v0.4.0``). The latest release
tags can be found by checking the 'releases' section on the github repository
page.

.. code-block:: console

	$ pip install git+https://github.com/yutanagano/nearust.git@<VERSION_TAG>

You can also clone the repository, and from within your Python environment,
navigate to the project root directory and run:

.. code-block:: bash

	$ pip install .
