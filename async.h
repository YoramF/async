#ifndef ASYNC_H_
#define ASYNC_H_

/**
 * async_launch allow to execute ex_func() asynchronously. When it cpomletes, callbake cb_func() will be called.
 * The ex_func - ex_res will be callback_func arg. ex_arg, ex_res and cb_res can be NULL depending on the 
 * definitions of ex_func and callback_func. Note that in case variable addresses are sent, they can not be reused
 * until the callback function was called or async_sync() or async_terminate() are called and returned.
 */
int async_launch (void (*ex_func)(void *arg, void *res), void (*cb_func)(void *arg, void *res),
    void *ex_arg, void *ex_res, void *cb_res);

/**
 * Initialize the asynchronous state in the program. max_thread will limit the number of outstanding execution
 * requests the environment will support
 */
int async_init (const int max_threads);

/**
 * Terminate the asyhchronous state. The functioin returns after all outstanding requests are finished
 */
int async_terminate ();

/**
 * Call this function to block main program untill all outstanding requests complete. Unlike async_terminate()
 * This function will not terminate the asynchronous environment, and new asyc requests can be issued.
 */
int async_sync();

#endif /* ASYNC_H_ */