
.. raw:: latex

    \pagebreak

.. _state phase:

State phase
================================================================================

op owner distributes state value to state owner
--------------------------------------------------------------------------------

* :ref:`state owner` collects its assigned :ref:`state values <state value>` from :ref:`op owners <op owner>`.

* :ref:`State owner` must collect a sufficient number of :ref:`state values <state value>` from the :ref:`op owners <op owner>`.

    - If ``F >= NOpShare`` (meaning too many :ref:`op owners <op owner>` have failed), the operation phase fails, which in turn causes the state phase to fail.
    - :ref:`State owner` reports its total number of collected :ref:`state values <state value>` to manager.

* :ref:`state owner` also collects the lists of :ref:`sub state owners <sub state owner>` and :ref:`affected state owners <affected state owner>` and keeps a record of them.


state owner processes state values
--------------------------------------------------------------------------------

* :ref:`state owner` collects information about :ref:`unmerged operations <unmerged operation>` from the :ref:`sub state owners <sub state owner>`.

    - If ``F >= NStateValueShare`` (meaning too many :ref:`sub state owners <sub state owner>` have failed), state phase fails.

    .. note::
        ``F`` is the number of fail :ref:`sub state owners <sub state owner>`

* After :ref:`state owner` collects enough :ref:`state values <state value>`, it can finish processing them.


state owner reports unmerged operations
--------------------------------------------------------------------------------

* :ref:`state owner` reports :ref:`unmerged operation` to both the manager and its :ref:`affected state owners <affected state owner>`.


Manager collects unmerged operations from state owners for operations tree
--------------------------------------------------------------------------------

* Both :ref:`op owners <op owner>` and :ref:`state owners <state owner>` report :ref:`unmerged operations <unmerged operation>` to the manager with the following details:

    - operation hash
    - *operation fact* hash
    - A confirmation of whether the operation was unmerged

* Manager uses the proposal to know the complete list of operations and generates the :ref:`operations tree`.


state owner reports the merged states info to manager
--------------------------------------------------------------------------------

* :ref:`state owner` merges one or more :ref:`state values <state value>` and generates a final :ref:`state`.
* :ref:`state owner` then reports the following :ref:`state` information to manager:

    - :ref:`state` key
    - :ref:`state` hash
    - A list of operations (*seq*) that contributed to the :ref:`state`


Manager collects states info from state owners for states tree
-----------------------------------------------------------------------------------------------------------

* Before checking for failed :ref:`state owners <state owner>`, manager first collects the complete list of :ref:`state owners <state owner>` from the :ref:`op owners <op owner>`.


Manager creates manifest
--------------------------------------------------------------------------------

* Manager creates the :ref:`manifest`, which is composed of the :ref:`operations tree` and the :ref:`states tree`.


.. _checking fail state owners:

Checking fail state owners
................................................................................

* The rule is `F >= NStateValueShare`.
* It means the state phase fails if the number of failed :ref:`state owners <state owner>` (``F``) is greater than or equal to the required share number (``NStateValueShare``).
