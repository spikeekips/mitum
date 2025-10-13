
.. raw:: latex

    \pagebreak

.. _operation phase:

Operation phase
================================================================================

Operation Distribution
--------------------------------------------------------------------------------

* Manager assigns :ref:`op owners <op owner>` from the pool of :ref:`active workers`.


Manager distributes operations to op owners
................................................................................

* Manager distributes operations to :ref:`op owners <op owner>` and provides them with the complete list of all :ref:`op owners <op owner>` involved in the :ref:`work`.

.. note::
    * Operations in a proposal are a list of operation info, formatted as ``[[<operation hash>, <operation fact hash>], ...]``.
    * Manager distributes this operation info, including a sequence number (``seq``), to :ref:`op owners <op owner>` in the format ``[[<seq>, <operation hash>, <operation fact hash>], ...]``.

* Manager sends the complete list of all :ref:`op owners <op owner>` to :ref:`active workers`.


op owner receives the operation info from manager
................................................................................

* An :ref:`op owner` retrieves the full operation data from a local node, the *proposer*, or other nodes.
* If :ref:`op owner` fails to retrieve an operation, it reports that operation to the manager as :ref:`unmerged operation`.
* Manager collects these :ref:`unmerged operation` reports from all :ref:`op owners <op owner>`.
* If one :ref:`op owner` fails to retrieve an operation but another :ref:`op owner` succeeds, manager still considers the operation **valid** for the purpose of generating the :ref:`operations tree`.

* For example ::

    ops w0=[o0 o1 o2]
    ops w1=[o0 o1 o2]

    - w0 fails to get the operations=[o0, o1]
      w1 fails to get the operations=[o1]

    - w0 reports unmerged operations to manager=[o0 o1]
      w1 reports unmerged operations to manager=[o1]

    - manager's unmerged operations=[o1]


op owner extracts state values
................................................................................

op owner distributes state value to state owner
--------------------------------------------------------------------------------

.. note::
    * :ref:`state` has **its** own set of one or more owners. The :ref:`state owner` is determined and fixed for a single :ref:`work` but can change in subsequent :ref:`work`.

.. note::
    * :ref:`state values <state value>` within an operation are not fixed; they depend on the existing *block*.

* :ref:`op owner` distributes the relevant :ref:`state values <state value>` to the workers designated as their owners. ::

    work=#3
    workers=[w0 w1 w2]
    NStateValueShare=2
    state=[s0 s1 s2]
    state owners
        s0=[w0 w1]
        s1=[w1 w2]
        s2=[w2 w0]

    op=[s0v0 s1v0 s2v0]
    state and its state values
        s0=s0v0
        s1=s1v0
        s2=s2v0

    state value owners
        s0v0=[w0 w1]
        s1v0=[w1 w2]
        s2v0=[w2 w0]

    distribution
        w0=[s0v0 s2v0]
        w1=[s0v0 s1v0]
        w2=[s1v0 s2v0]

sub state value, add state value
................................................................................

* :ref:`op owner` distributes two kinds of :ref:`state values <state value>` from a single operation,

    - :ref:`sub state value`
    - :ref:`add state value`

* :ref:`op owner` also distributes the lists of :ref:`sub state owners <sub state owner>` and :ref:`affected state owners <affected state owner>` along with the :ref:`state values <state value>` to ensure all relevant workers are informed. ::

    operation state values=[s0v0 s1v0 s2v0]
    sub state value=[s0v0]
    add state value=[s1v0 s2v0]
    state owners
        s0v0=w0 w1
        s1v0=w1
        s2v0=w2

    sub state owners=[w0 w1]
    add state owners=[w1 w2]

    op owner sends to state owners
        w0
            - state values=[s0v0]
            - affected state owners=[w1 w2]
        w1
            - state values=[s0v0 s1v0]
            - sub state owners=[w0 w1]
            - affected state owners=[w1 w2]
        w2
            - state values=[s2v0]
            - sub state owners=[w0 w1]



op owner reports completion of the operation phase
--------------------------------------------------------------------------------

* :ref:`op owner` reports to manager that operation phase is finished, providing a complete list of all assigned :ref:`state owners <state owner>`.

* If ``F >= NOpShare`` (meaning too many :ref:`op owners <op owner>` failed), the operation phase **fails**.

* Manager then notifies **non-state owners** (:ref:`active workers` that are not :ref:`state owners <state owner>`) that operation distribution is complete.

    - non-state owner is ``active workers - state owners``
    - Non-state owner finishes operation phase and does not enter the state phase.
    - Non-state owner is also not an :ref:`op owner`, it simply closes its :ref:`work` unit and reports this to manager.
