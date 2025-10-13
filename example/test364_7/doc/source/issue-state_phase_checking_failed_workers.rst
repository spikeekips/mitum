ISSUE: Checking for failed workers during the state phase
==========================================================

* Unlike in the operation phase, state values in the state phase are not distributed equally or according to their share to the state owner.
* The state values of an operation are not fixed; they vary based on block states.
* The state values of an operation are scattered across all state owners.
* The owner of a state is not fixed; it varies depending on the active workers, and the list of workers can change for each work unit.
* For example,::

    case #0:
        workers=[w0 w1 w2]
        NStateValueShare=2
        states=[s0 s1 s2]

        state owners:
            s0: w0 w1
            s1: w2 w0
            s2: w1 w2
        owners:                 state values
            w0: s0v0 s1v0       2
            w1: s0v0 s1v2       2
            w2: s1v0 s2v0       2

     case #1:
        workers=[w0 w1 w2]
        NStateValueShare=2
        states=[s0 s1 s2]

        state owners:
            s0: w0 w1
            s1: w0 w1
            s2: w0 w2
        owners:                 state values
            w0: s0v0 s1v0 s2v0  3
            w1: s0v0 s1v0       2
            w2: s2v0            1


``F >= NStateValueShare``
--------------------------------------------------

.. note::
    ``F`` is the number of failed :ref:`state owners <state owner>`

* ``F >= NStateValueShare`` means that if the number of failed owners is greater than or equal to `NStateValueShare`, the state phase fails. This happens because some required state values are missing.


Case #0: ``F == NStateValueShare``, but ok
--------------------------------------------------------------------------------

::

        workers=[w0 w1 w2 w3 w4 w5]
        NStateValueShare=2
        states=[s0 s1 s2]

        state owners:
            s0: w0 w1
            s1: w2 w3
            s2: w4 w5
        owners:                 state values
            w0: s0v0            1
            w1: s0v0            1
            w2: s1v0            1
            w3: s1v0            1
            w4: s2v0            1
            w5: s2v0            1

        fails: [s0v0 s1v0]
            w0: s0v0
            w2: s1v0
        oks: [s0v0 s1v0 s2v0]
            w1: s0v0
            w3: s1v0
            w4: s2v0
            w5: s2v0

* In this case, if workers ``w0`` and ``w2`` fail (where the number of failures equals `NStateValueShare`), the process can still succeed. This is because all unique state values are still available from the other active workers (``w1``, ``w3``, ``w4``, and ``w5``).


Case #0: ``F > NStateValueShare``, but ok
--------------------------------------------------------------------------------

::

        workers=[w0 w1 w2 w3 w4 w5]
        NStateValueShare=2
        states=[s0 s1 s2]

        state owners:
            s0: w0 w1
            s1: w2 w3
            s2: w4 w5
        owners:                 state values
            w0: s0v0            1
            w1: s0v0            1
            w2: s1v0            1
            w3: s1v0            1
            w4: s2v0            1
            w5: s2v0            1

        fails: [s0v0 s1v0 s2v0]
            w0: s0v0
            w2: s1v0
            w4: s2v0
        oks: [s0v0 s1v0 s2v0]
            w1: s0v0
            w3: s1v0
            w5: s2v0

* In this case, even if workers ``w0``, ``w2``, and ``w4`` fail (a number greater than `NStateValueShare`), the process can still succeed because all unique state values are covered by the remaining workers (``w1``, ``w3``, and ``w5``).


Summary
--------------------------------------------------------------------------------

* Although some cases appear to violate the ``F >= NStateValueShare`` rule, this rule represents the minimum condition required to trigger a failure.
