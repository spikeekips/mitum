
.. raw:: latex

    \pagebreak

Close phase
================================================================================

New manifest is voted on and confirmed
--------------------------------------------------------------------------------

* Manager creates :ref:`manifest` and its node initiates *ACCEPT* vote

    * If vote fails, node starts new vote round.

* After :ref:`manifest` is approved, manager closes :ref:`work`.


Manager requests that active workers close work
--------------------------------------------------------------------------------

* Manager requests :ref:`active workers` to close :ref:`work`.
* The sets of :ref:`op owners <op owner>` and :ref:`state owners <state owner>` are part of :ref:`active workers`.

    * Any :ref:`active workers` that are not :ref:`op owners <op owner>` or :ref:`state owners <state owner>` must also close their work.


op owner and state owner store the data and close work
--------------------------------------------------------------------------------

* :ref:`op owner` stores its assigned operations.
* :ref:`state owner` stores its assigned states.
* All :ref:`active workers` then close :ref:`work`.


active workers report successful closure of work
--------------------------------------------------------------------------------

* :ref:`active workers` reports to manager that :ref:`work` has been successfully closed.

Manager verifies reports from op owners
................................................................................

* ``F >= NOpShare`` means the close phase fails if there are not enough :ref:`op owners <op owner>` available to store their operations.


Manager verifies reports from states owners
................................................................................

* Similarly, ``F >= NStateValueShare`` means the close phase fails if there are not enough :ref:`state owners <state owner>` available to store their :ref:`states <state>`.


Manager stores new block data
--------------------------------------------------------------------------------

* Manager stores the new block data, which includes:

    - :ref:`manifest`
    - proposal
    - :ref:`operations tree`
    - :ref:`states tree`
    - map of operations data, showing which operation is stored by which :ref:`op owner`.
    - map of states data, showing which state is stored by which :ref:`state owner`.


Manager closes work
--------------------------------------------------------------------------------

* Manager formally closes the :ref:`work`.
* After this, manager is ready to begin a new :ref:`work`.
