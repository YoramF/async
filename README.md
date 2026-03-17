# async
Enable asynchronous state inside a C program in which functions can be launched asynchronously.
These files should be compiled as a static library.

I chose the workers/queues approach. There are execution workers and callback workers.
Basically, except for the execution function and async_id variable, which was returned by the async_init() function, all other arguments for async_launch() are optional and can be replaced by NULL.

In this implementation, the role of the callback function is to pass results from the called asynchronous function 
back to the main program. In most cases it is a light function; therefore, as a default, the number of callback workers
that are brought up by default is 1/2 of the number of execution workers. If the callback functions need more time
and the environment needs more callback workers to work properly; it is possible to add the number required
callback workers as the second argument to the async_init() function.

Since both the calling function and callbacks run asynchronously, if the main program needs any of them to update
a global variable, it is possible to use the async_call() function with internally used pthread_mutex_lock() before
It calls the requested function and pthread_mutex_unlock() before it returns to the main program.

Note, however, that the called function must be defined as specified even though it does not need any arguments.