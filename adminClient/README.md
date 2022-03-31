## topic management
- - -
- All the operations that modify the cluster state - create, delete and alter, are handled by the Controller. Operations that read the cluster state - list and describe,
can be handled by any broker and are directed to the least loaded broker (based
on what the client knows).
