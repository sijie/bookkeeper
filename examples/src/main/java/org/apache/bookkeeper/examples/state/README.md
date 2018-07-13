## Building a Replicated State Mahcine using DistributedLog

This example shows how to use distributedlog to build a replicated set between machines.

## Usage

`ReplicatedStateExample` is a simple program to play with `ReplicatedState` to show how to replicate
a set of strings between machines.

1. Start distributedlog sandbox.

```shell
$ bin/dlog local 2181
```

This command starts a local distributedlog cluster. When you saw
`DistributedLog Sandbox is running now. You could access distributedlog://127.0.0.1:2181`, it means
the distributedlog cluster is up running. Now you can use `distributedlog://127.0.0.1/messaging/distributedlog`
as the dlog uri for accessing dlog.

2. Open a new terminal to run `ReplicatedStateExample`.

```shell
$ bin/examples org.apache.bookkeeper.examples.state.ReplicatedStateExample -u distributedlog://127.0.0.1/messaging/distributedlog -n test_stream
```

This program will run a `ReplicatedState` to replicating a set of values using a dlog stream `test_stream`.

The program is basically periocially adding a bunch of `String`s and remove them.

Example output as below:

```shell
=== Testing Replicated Set ===

--- begin test run : 0 ---

add value : run-0-value-0
add value : run-0-value-1
add value : run-0-value-2
add value : run-0-value-3
add value : run-0-value-4
add value : run-0-value-5
add value : run-0-value-6
add value : run-0-value-7
add value : run-0-value-8
add value : run-0-value-9
add value : run-0-value-10
add value : run-0-value-11
add value : run-0-value-12
add value : run-0-value-13
add value : run-0-value-14
add value : run-0-value-15
remove value 'run-0-value-0' : true
remove value 'run-0-value-1' : true
remove value 'run-0-value-2' : true
remove value 'run-0-value-3' : true
remove value 'run-0-value-4' : true
remove value 'run-0-value-5' : true
remove value 'run-0-value-6' : true
remove value 'run-0-value-7' : true
remove value 'run-0-value-8' : true
remove value 'run-0-value-9' : true
remove value 'run-0-value-10' : true
remove value 'run-0-value-11' : true
remove value 'run-0-value-12' : true
remove value 'run-0-value-13' : true
remove value 'run-0-value-14' : true
remove value 'run-0-value-15' : true

--- end test run : 0 ---
--- begin test run : 1 ---

add value : run-1-value-0
add value : run-1-value-1
add value : run-1-value-2
add value : run-1-value-3
add value : run-1-value-4
add value : run-1-value-5
add value : run-1-value-6
add value : run-1-value-7
add value : run-1-value-8
add value : run-1-value-9
add value : run-1-value-10
add value : run-1-value-11
add value : run-1-value-12
add value : run-1-value-13
add value : run-1-value-14
add value : run-1-value-15
add value : run-1-value-16
add value : run-1-value-17
add value : run-1-value-18
add value : run-1-value-19
add value : run-1-value-20
add value : run-1-value-21
remove value 'run-1-value-0' : true
remove value 'run-1-value-1' : true
remove value 'run-1-value-2' : true
remove value 'run-1-value-3' : true
remove value 'run-1-value-4' : true
remove value 'run-1-value-5' : true
remove value 'run-1-value-6' : true
remove value 'run-1-value-7' : true
remove value 'run-1-value-8' : true
remove value 'run-1-value-9' : true
remove value 'run-1-value-10' : true
remove value 'run-1-value-11' : true
remove value 'run-1-value-12' : true
remove value 'run-1-value-13' : true
remove value 'run-1-value-14' : true
remove value 'run-1-value-15' : true
remove value 'run-1-value-16' : true
remove value 'run-1-value-17' : true
remove value 'run-1-value-18' : true
remove value 'run-1-value-19' : true
remove value 'run-1-value-20' : true
remove value 'run-1-value-21' : true

--- end test run : 1 ---
```

3. Open a new terminal to run another `ReplicatedStateExample`.

```shell
$ bin/examples org.apache.bookkeeper.examples.state.ReplicatedStateExample -u distributedlog://127.0.0.1/messaging/distributedlog -n test_stream
```

This `ReplicatedStateExample` will act as a Slave replicating the set of values by tailing and replaying updates
from dlog stream `test_stream`.

The `ReplicatedStateExample` instance will replay the updates and apply the updates to its in-memory state.

Example output:
```
add value : run-13-value-17
add value : run-13-value-18
add value : run-13-value-19
add value : run-13-value-20
add value : run-13-value-21
add value : run-13-value-22
add value : run-13-value-23
add value : run-13-value-24
add value : run-13-value-25
add value : run-13-value-26
add value : run-13-value-27
add value : run-13-value-28
add value : run-13-value-29
add value : run-13-value-30
remove value : run-13-value-0
remove value : run-13-value-1
remove value : run-13-value-2
remove value : run-13-value-3
remove value : run-13-value-4
remove value : run-13-value-5
remove value : run-13-value-6
remove value : run-13-value-7
remove value : run-13-value-8
remove value : run-13-value-9
remove value : run-13-value-10
remove value : run-13-value-11
```

4. Kill the `ReplicatedStateExample` in the terminal of step 2.

The `ReplicatedStateExample` in terminal of step 3 will be promoted as leader and begin updates the replicated set.

Example output in the terminal of step 3.
```shell
=== Testing Replicated Set ===

--- begin test run : 0 ---

add value : run-0-value-0
add value : run-0-value-1
add value : run-0-value-2
add value : run-0-value-3
```

## Details

The `ReplicatedState` maintains an in-memory set of values. Each `ReplicatedState` instance will attempt to become
the leader for this `ReplicatedState`, by opening a dlog stream with infinite lock timeout. The instance successfully
opens the log stream will become the leader and have the right to update the `replicated state. All the updates will
be first journaled into the log stream, before applied to the in-memory state. 

The instances waiting for the lock of opening the log stream will become Followers. The followers will keep tailing updates
from the dlog stream and apply those updates to their in-memory state.
