ISSUE: How does manager handle a failed worker?
================================================================================

* Manager must monitor all :ref:`workers` in *near* real time

    - Even when a worker becomes idle, the manager can detect this state via an idle timeout.

* When a worker fails, it should notify the manager of its failure.
* Manager broadcasts the identity of the failed worker to all other :ref:`active workers`.
