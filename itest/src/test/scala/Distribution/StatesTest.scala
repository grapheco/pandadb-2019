package Distribution

class StatesTest {
/*
    test whether the flag is right in different state and the system behave as supposed.
    State List:
        Serving(locked/locked)   <--
        PreWrite                    |
        Write                       |
        Finish                  ----
    Flag:
        READY_TO_WRITE          true, when there is no served request
        WRITE_TO_FINISHED       true, when all the gnodes finish the write operation
        WAIT_JOIN               (only active in serving state) true, if there is a gnode waits for joining in the cluster

 */
}
