#ifndef ASYNC_H_
#define ASYNC_H_

#include <pthread.h>

// hidden variables to connect to async encironment
typedef void *async_id_t;
typedef void *async_s_func_t;

/**
 * async_launch allow to execute ex_func() asynchronously. When it cpomletes, callbake cb_func() will be called.
 * The ex_func - ex_res will be callback_func arg. ex_arg, ex_res and cb_res can be NULL depending on the 
 * definitions of ex_func and callback_func. Note that in case variable addresses are sent, they can not be reused
 * until the callback function was called or async_sync() or async_terminate() are called and returned.
 */
int async_launch (void (*ex_func)(void *arg, void *res), void (*cb_func)(void *arg, void *res),
    void *ex_arg, void *ex_res, void *cb_res, async_id_t async_id);

/**
 * Initialize the asynchronous state in the program. max_thread will limit the number of outstanding execution
 * requests the environment will support
 */
async_id_t async_init (const int max_threads);

/**
 * Terminate the asyhchronous state. The functioin returns after all outstanding requests are finished
 */
int async_terminate (async_id_t async_id);

/**
 * Call this function to block main program untill all outstanding requests complete. Unlike async_terminate()
 * This function will not terminate the asynchronous environment, and new asyc requests can be issued.
 */
int async_sync(async_id_t async_id);

/**
 * In an asynchrounous state, we need to allow shared resorces to be updated in a synchrounous way.
 * async_sync_func_init() allows us to declare a a function so that when other functions call it
 * the rfunction will run in a locked safe state.
 * It returns a pointer to async_s_func_t.
 * Thw s_func() must have a short exeution time, otherwise it can block other async functions
 */
async_s_func_t async_sync_func_init (void (*s_func)(void *arg, void *res), async_id_t async_id);

/**
 * Call s_func().
 * Return 0 if all ok or -1 if called with false arguments.
 */
int async_call (async_s_func_t p_func, void *arg, void *res);



#endif /* ASYNC_H_ */