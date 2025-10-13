Terminology
################################################################################

.. _work:

work
====================

* Basic unit of *DOP* processing
* `work`_ is started with proposal
* If the `work`_ fails, new `work`_ is started with the same proposal


.. _work id:

work id
--------------------------------------------------

* Unique identifier for `work`_.
* It must be unique in entire works.
* Usually it is formatted as ``<height>-<round>-<proposal hash>-<UUID>``.


.. _workers:
.. _worker:

workers
====================

* The entire set of available workers.


.. _active workers:
.. _active worker:

active workers
--------------------

* The subset of workers participating in a specific work.
* `active workers`_ should be,
    - ``>=`` `op owners`_
    - ``>=`` `state owners`_
    - ``>=`` expected `state owners`_; expected `state owners`_ is ``number of operations * expected state values per operation``


.. _op owners:
.. _op owner:

op owner
--------------------

* manager distributes operations to some of workers of `active workers`_
* worker of `op owners`_ should belong to `active workers`_
* If NOpShare_ is lesser than `op owners`_, NOpShare_ is readjusted to the number of `op owners`_
* If `op owners`_ is bigger than ``NOps * NOpShare``, `op owners`_ is readjusted to the ``NOps * NOpShare``

* ``op owners < NOpShare``::

    op owners=1 NOps=5 NOpShare=1:
        w0: o0 o1 o2 o3 o4

    op owners=1 NOps=5 NOpShare=2 -> new NOpShare=1:
        w0: o0 o1 o2 o3 o4

    op owners=2 NOps=5 NOpShare=3 -> new NOpShare=2:
        w0: o0 o1 o2 o3 o4
        w1: o0 o1 o2 o3 o4

* ``op owners > NOpShare``::

    op owners=3 NOps=5 NOpShare=1:
        w0: o0 o3
        w1: o1 o4
        w2: o2

    op owners=3 NOps=2 NOpShare=1:
        w0: o0
        w1: o1

    op owners=3 NOps=2 NOpShare=2:
        o0: w0 w1
        o1: w0 w2

* ``op owners > (NOps * NOpShare)``::

    op owners=9 NOps=2 NOpShare=2 -> new op owners=4:
        o0: w0 w1
        o1: w2 w3


.. _state owners:
.. _state owner:

state owner
--------------------

* A worker responsible for handling a specific state value.


.. _sub state owners:
.. _sub state owner:

sub state owner
----------------------------

* The state owner responsible for a `sub state value`_.


.. _affected state owners:
.. _affected state owner:

affected state owner
----------------------------

* The state owner responsible for an `add state value`_.


.. _unmerged operations:
.. _unmerged operation:

unmerged operation
====================

Parameters
====================


.. _NOps:

NOps
--------------------

    * number of operations


.. _NOpShare:

NOpShare
--------------------

    * The number of workers an operation is shared with.
    * It must be `1` or greater.


.. _NStateValueShare:

NStateValueShare
--------------------

    * number to share :ref:`state value` to `active workers`
    * It must be `1` or greater.


.. _states:
.. _state:

state
====================


.. _state values:
.. _state value:

state value
--------------------


.. _sub state values:
.. _sub state value:

sub state value
--------------------

* A state value that, if unmerged, causes all other `add state values`_ in the same operation to be unmerged as well.
* An operation can have zero or one sub state value.


.. _add state values:
.. _add state value:

add state value
--------------------

* A state value that can be unmerged as a consequence of its corresponding `sub state value` being unmerged.
* For example, the operation, ``o0`` is to add or subtract 1 to the state

    ::

        o0=[sv0 sv1]
        states=[s0 s1]

        sv0=s0 - 1
        sv1=s1 + 1

    * If ``sv0`` is unmerged, ``sv1`` should be unmerged


block
============================

.. _manifest:

manifest
----------------------------


.. _operations tree:

operations tree
----------------------------


.. _states tree:

states tree
----------------------------
