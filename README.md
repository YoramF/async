# async
Enable asynchronous state inside a C program in which functions can be launched asynchronously.
These files should be compiled as a static library.

I chose the basic approach of creating new threads per launch request with a limit on how many outstanding
threads I can run. Basically, except for the execution function, all other arguments for async_launch() are
optional and can be replaced by NULL.

In case there is a need to update a global variables or do any synchronous work by the called async function,
it is possible to define a function that will be executed synchronously. This function must be short; otherwise, it
will block the rest of the outstanding function calls that also need to use this synchronous function.

Note, however, that the called function must be defined as specified even though it does not need any arguments.