# async
Enable asynchronous state inside a C program in which functions  can be launched asynchronously.

This files should be compiled as a static library.

I chose the basic approach of creating new threads per launch request with a limit on how many outstanding
thread I can run. basically except for the excution function, all other arguments for async_launch() are
optional and can be replaced by NULL. 

Note however, that the called function must be defined as specified even though it does not need any arguments.
