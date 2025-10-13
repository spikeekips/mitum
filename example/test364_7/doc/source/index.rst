################################################################################
DOP: Distributed Operations Processing
################################################################################

:version: |version|
:release: |released_at|

----

The main purpose of DOP (Distributed Operations Processing) is performance—specifically, how quickly a vast number of operations can be processed. This document explains the new features and enhancements to the current implementation that support this goal.

* It consists of one control manager and one or more workers.
* Operation is distributed to one or more workers.
* State value is distributed to one or more workers.
* Worker processes its assigned operations and state values.
* Manager collects the results from workers.
* Manager and workers store the processed data.


.. toctree::
    :maxdepth: 3
    :numbered:
    :caption: Table of contents:

    phases
    issues
    terminology
