
.. raw:: latex

    \pagebreak

Start phase
================================================================================

Manager starts new work
--------------------------------------------------------------------------------

* Manager starts a new :ref:`work` based on a proposal. :ref:`Work` contains the following information:

    * block height
    * voting round
    * :ref:`work id`
    * :ref:`NOps`
    * :ref:`NOpShare`
    * :ref:`NStateValueShare`
    * Manager URL


Manager requests that workers start new work
--------------------------------------------------------------------------------

* Manager requests to the :ref:`workers`, along with work information.

    * Manager collects the responses from workers
    * It then creates a list of :ref:`active workers`, excluding any that are not ready.
    * Finally, manager sends the list of :ref:`active workers` to all participating :ref:`workers`.

.. important::
    * A new :ref:`work` **must be ignored** if its block height and round are older than those of the previous :ref:`work`.
    * If a new :ref:`work` is initiated with a proposal that has already been seen, the previous :ref:`work` associated with that same proposal **must be canceled**.


Worker starts new work
--------------------------------------------------------------------------------

* The designated :ref:`active workers` then start the new :ref:`work`.
