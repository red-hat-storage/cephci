Contribution Guidelines
***********************

Being open, we expect contributions from various walks of life. The below guidelines help in keeping the code base uniform irrespective of the person contributing to it.

Coding
------
* Follow the PEP 8 proposal. Specifically, for the below mentioned areas
    - `Naming Conventions`_
    - `Programming recommendations`_
    - `Whitespaces`_
    - Using code comments sparsely but if required then `PEP8`_

* Docstrings is mandatory, it needs to be compact and informative. It is required for each level i.e. functions, methods, classes and modules.
    - Follow the guidelines as stated in `PEP257`_
    - It should be considered as a living document i.e. change them in accordance with the code modifications.
    - All parameters must be documented
    - Return values must be documented
    - Document the exceptions raised if any

* Import modules and its attributes
    - Group them with a separation in the below order,
        - Standard library
        - Third-party library
        - Local library
        - Local module/package
    - Use absolute imports
    - Avoid relative import
    - Import based on requirement and use from statement wherever applicable
    - `PEP328`_ is a good convention that can be used

* Avoid using ``*args **kwargs`` variable length of parameters,

    .. code-block:: python

        def main(*args, **kwargs):
        ....

* Prefer to use key/value pairs in methods over arguments
    - Good: 	``x = wait_till(timeout=300,polling=10)``
    - Bad:      ``x = wait_till(300, 10)``

* Write unit tests for new code that is applicable to the framework.


Testing
-------
- Unit test the changes
- Run system tests if required
- Ensure to run tox before commit

Best Practices
--------------
Let’s apply some of the industry standard practices during our development

* Contributing workflow
    - Open a new issue on our issue tracker
    - Encourage discussion if it is an enhancement or feature
    - Fork the repository
    - Create a new branch from the fork
    - Make your changes
    - Write unit tests wherever appropriate
    - Unit test the changes
    - Commit
        - Meaningful commit
        - Squash commits together if required
        - Meaningful messages for a commit
    - Open a pull request
        - Tag ``DNM`` if required
        - Link to the created issue as part of step 1.
    - Wait for the review


Formatting
----------
An attempt is made to follow a set of style guidelines for clarity.
As we grow and have more contributors, it becomes essential for us to follow some standard practices. We use

- PEP8
- black
- isort

black
=====
.. code-block::

    # Black is installed as part of cephci requirements. One can also install it

    $ python -m pip install black

    # formatting code with black
    $ black <filename>

    where `filename`
    - a relative or absolute path of the file
    - a relative or absolute path of a directory # for running for set of files

    # for the project
    $ black .


isort
=====
.. code-block::

    # isort is installed as part of the cephci requirements. One can also install the
    # package using the below command

    $ python -m pip install isort

    # formatting code with isort
    $ isort <filename>

    where `filename`
    - a relative or absolute path of the file
    - a relative or absolute path of a directory

    # Example
    $ isort mita/openstack.py

Checkers
--------
As a first step to ensure no breakage of the functionality, the following linters
are introduced

- yamllint

yamllint
========
This linter checks all YAML files for syntax issues. One can locally execute it
by doing the following

.. code-block::

    # yamllint is installed either through cephci requirements or using the below command

    $ python -m pip install yamllint

    # Running the checker
    $ yamllint -d relaxed <absolute-path-to-yaml-file>

.. _`PEP328`: https://www.python.org/dev/peps/pep-0328/
.. _`PEP257`: https://www.python.org/dev/peps/pep-0257/
.. _`PEP8`: https://www.python.org/dev/peps/pep-0008/#id30
.. _`Naming Conventions`: https://www.python.org/dev/peps/pep-0008/#naming-conventions
.. _`Programming recommendations`: https://www.python.org/dev/peps/pep-0008/#id51
.. _`Whitespaces`: https://www.python.org/dev/peps/pep-0008/#whitespace-in-expressions-and-statements
