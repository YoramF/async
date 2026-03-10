/**
 * Async library allows c program to call functions in an asynchronous environment.
 * This allow to break a task into parallel smaller tasks and utilize all available computing resourcers.
 * To enable the asynchronoue state the program calls async_init(MAX_THREADS).
 * To terminate the asynchronous state the program calls async_terminate(). This call returns only after all autstanding
 * async function calls terminate.
 * The executable function is defined: void exfunc(const void *arg, void *res, const (*callback)(const void *arg)). exfunc's res
 * is sent to callback func is its arg.
 * If the number of outstanding async functions reached MAX_THREADS, the next call to another async funcion will hold untill
 * one or more outstanding fsactions were done. 
 */

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>

#include <async.h>

// Structure to hold the result and identity of a completed thread
typedef struct {
    void    (*callback)(void *arg, void *res);
    void    *callback_arg;
    void    *callback_res;
} thread_result_t;

// Shared Coordinator Structure
typedef struct {
    pthread_mutex_t     mutex;
    pthread_cond_t      cond;
    int                 count; // How many results are currently in the queue
    thread_result_t   *queue;
} result_mailbox_t;

// worker argument
typedef struct {
    void    (*func)(void *arg, void *res);
    void    (*callback)(void *arg, void *res);
    void    *func_arg;
    void    *func_res;
    void    *callback_res;
} worker_arg_t;

// callback caller worker argument
typedef struct callback_arg_s {
    struct callback_arg_s *next;
    void    (*callback)(void *arg, void *res);    
    void    *callback_arg;
    void    *callback_res;    
} callback_arg_t;


// module static variables
static result_mailbox_t s_mailbox = {
    .mutex = PTHREAD_MUTEX_INITIALIZER,
    .cond  = PTHREAD_COND_INITIALIZER,
    .count = 0
};

// variable lenngh structure to track all "async_sync_func_init" calls
// so that we can free all allocated RAM once async_terminate() is called
typedef struct {
    int count;
    async_s_func_t **s_funcs;
} s_func_t;

static int s_max_threads;
static bool s_exit;
static int s_outstanding_requests;
static int s_outstanding_callbacks;
static pthread_t s_main_thread;
static pthread_t s_callback_trd;
static callback_arg_t *s_callback_args_head = NULL;
static callback_arg_t *s_callback_args_tail = NULL;
static pthread_mutex_t s_async_env_mutx = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t s_async_env_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t s_callback_mutx = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t s_callback_cond = PTHREAD_COND_INITIALIZER;
// static pthread_mutex_t s_callback_sync_mutx = PTHREAD_MUTEX_INITIALIZER;
// static pthread_cond_t s_callback_sync_cond = PTHREAD_COND_INITIALIZER;
static s_func_t s_s_funcs = {
    .count = 0,
    .s_funcs = NULL
};

/**
 * add new s_func to s_s_funcs store
 */
void s_s_funcs_add (async_s_func_t *p_s_func) {
    int count = s_s_funcs.count;

    if (count == 0) {
        if ((s_s_funcs.s_funcs = malloc(sizeof(async_s_func_t *))) == NULL) {
            perror("[s_s_funcs_add] Failed to allocate RAM\n");
            exit(EXIT_FAILURE);
        }
    }
    else {
        if ((s_s_funcs.s_funcs = realloc(s_s_funcs.s_funcs, (size_t)(count+1)*sizeof(async_s_func_t *))) == NULL) {
            perror("[s_s_funcs_add] Failed to reallocate RAM\n");
            exit(EXIT_FAILURE);           
        }
    }

    s_s_funcs.s_funcs[count] = p_s_func;
    s_s_funcs.count++;
}

/**
 * Release all saved s_func structures
 */
void s_s_funcs_release () {
    int i;

    // first destroy all allocated mutexes
    for (i = 0; i < s_s_funcs.count; i++)
        pthread_mutex_destroy(&s_s_funcs.s_funcs[i]->lock);

    free(s_s_funcs.s_funcs);    // s_s_funcs.funcs is either a valid poiuner or NULL
    s_s_funcs.count = 0;
    s_s_funcs.s_funcs = NULL;   // to make sure next calling to s_s_funcs_release () will not fail on free(invalid pointer)
}


/**
 * Main worker thread
 */
static void *s_worker(void* arg) {
    worker_arg_t my_wrk = *(worker_arg_t *)arg;

    free(arg);

    #ifdef LOG
    printf("[worker] Thread was created\n");
    #endif

    my_wrk.func(my_wrk.func_arg, my_wrk.func_res);

    // --- SIGNALING PHASE ---
    pthread_mutex_lock(&s_mailbox.mutex);
    
    // "Sign" the result so the parent knows who this is
    s_mailbox.queue[s_mailbox.count].callback_arg = my_wrk.func_res;
    s_mailbox.queue[s_mailbox.count].callback_res = my_wrk.callback_res;
    s_mailbox.queue[s_mailbox.count].callback = my_wrk.callback;
    s_mailbox.count++;

    #ifdef LOG
    printf("[Worker] Finished. Signaling parent...\n");
    #endif
    
    // Wake up the parent
    pthread_cond_signal(&s_mailbox.cond);
    pthread_mutex_unlock(&s_mailbox.mutex);

    return NULL;
}

/**
 * Main callback execution thread
 * This thread is waiting for callbacks to be execute
 */
static void *s_callback (void *arg) {
    #ifdef LOG
    printf("[s_callback_trd] started\n");
    #endif

    while (s_exit == false) {

        pthread_mutex_lock(&s_callback_mutx);
        // Wait specifically until callback args list has something in it
        while (s_outstanding_callbacks == 0) {
            #ifdef LOG
            printf("[s_callback_trd] entering pthread_cond_wait() state\n");
            #endif
            pthread_cond_wait(&s_callback_cond, &s_callback_mutx);

            // in case we were awaken by async_terminate() exit thread;
            if (s_exit == true)
                goto s_callback_trd_done;
        }

        // Process csllbacks
        while (s_callback_args_head != NULL) {
            callback_arg_t *c_arg = s_callback_args_head;

            #ifdef LOG
            printf("[s_callback_trd] about to call calback\n");
            #endif
            
            c_arg->callback(c_arg->callback_arg, c_arg->callback_res);

            // remove callback arg from list
            s_callback_args_head = c_arg->next;
            if (s_callback_args_head == NULL)
                s_callback_args_tail = NULL;

            free(c_arg);

            s_outstanding_callbacks--;
            // signal async_sync() and async_terminate() about callback termination
            pthread_mutex_lock(&s_async_env_mutx);

            #ifdef LOG
            printf("[s_callback_trd] locked s_async_env_mutx\n");
            #endif

            // pthread_cond_broadcast(&s_async_env_cond);
            pthread_cond_signal(&s_async_env_cond);
            pthread_mutex_unlock(&s_async_env_mutx);
        }

s_callback_trd_done:
        pthread_mutex_unlock(&s_callback_mutx);        
    }

    #ifdef LOG
    printf("[s_callback_trd] ended\n");
    #endif

    return NULL;    
}

/**
 * This is the main result thread. It runs as long as the async envirinment is active and as long as there are outstanding
 * async requests active. Per thread working thread completion, it will add a callback workload for callback thread
 * and signal it about new warkload to process
 */
static void *s_main_result_trd (void *arg) {

    #ifdef LOG
    printf("[s_main_result_trd] started\n");
    #endif

    while (s_exit == false) {

        pthread_mutex_lock(&s_mailbox.mutex);
        // Wait specifically until the mailbox has something in it
        while (s_mailbox.count == 0) {
            #ifdef LOG
            printf("[s_main_result_trd] entering pthread_cond_wait() state, outstanding: %d\n", s_outstanding_requests);
            #endif
            pthread_cond_wait(&s_mailbox.cond, &s_mailbox.mutex);

            // in case we were awaken by async_terminate() exit thread;
            if (s_exit == true)
                goto s_main_result_trd_done;
        }

        // Identification: Pull the result from the "mailbox"
        while (s_mailbox.count > 0) {
            s_mailbox.count--;

            thread_result_t res = s_mailbox.queue[s_mailbox.count];

            // check that callback function was specified in async_launch()
            if (res.callback != NULL) {
                callback_arg_t *c_arg;

                pthread_mutex_lock(&s_callback_mutx);
                
                if ((c_arg = malloc(sizeof(callback_arg_t))) == NULL) {
                    perror("[s_main_result_trd] Failed to allocate RAM");
                    exit(EXIT_FAILURE);                    
                }

                #ifdef LOG
                printf("[s_main_result_trd] adding new callback arg to list: %d\n", s_outstanding_requests);
                #endif

                c_arg->callback = res.callback;
                c_arg->callback_arg = res.callback_arg;
                c_arg->callback_res = res.callback_res;
                c_arg->next = NULL;
                if (s_callback_args_tail == NULL) {
                    s_callback_args_tail =  c_arg;
                    s_callback_args_head = c_arg;
                }
                else {
                    s_callback_args_tail->next = c_arg;
                    s_callback_args_tail = c_arg;
                }

                // Wake up the callback_trd in case it was blockec
                s_outstanding_callbacks++;
                pthread_cond_signal(&s_callback_cond);
                pthread_mutex_unlock(&s_callback_mutx);
            }
                 
            // Wake up the launcing function in case it was blockec
            pthread_mutex_lock(&s_async_env_mutx);
            s_outstanding_requests--;
            // pthread_cond_broadcast(&s_async_env_cond);
            pthread_cond_signal(&s_async_env_cond);
            pthread_mutex_unlock(&s_async_env_mutx);

            #ifdef LOG
            printf("[s_main_result_trd] before return. outstanding: %d\n", s_outstanding_requests);
            #endif
        }
s_main_result_trd_done:
        pthread_mutex_unlock(&s_mailbox.mutex);
    }

    #ifdef LOG
    printf("[s_main_result_trd] ended\n");
    #endif

    return NULL;
}

/**
 * This is the main launcher function. It creates new working thread to execute the async function.
 * In case the number of thread exceeds max_threads, it will block until a new thread can be created.
 * callback_func *arg are ex_function *res
 */
int async_launch (void (*ex_func)(void *arg, void *res), void (*cb_func)(void *arg, void *res),
    void *ex_arg, void *ex_res, void *cb_res) {

    pthread_t worker_trd;
    worker_arg_t *wrk_arg;

    if ((wrk_arg = malloc(sizeof(worker_arg_t))) == NULL) {
        perror("[async_launch] Failed to allocate RAM");
        exit(EXIT_FAILURE);
    }
    wrk_arg->callback_res = cb_res;
    wrk_arg->func = ex_func;
    wrk_arg->func_arg = ex_arg;
    wrk_arg->func_res = ex_res;
    wrk_arg->callback = cb_func;

    if (s_exit == false) {
        pthread_mutex_lock(&s_async_env_mutx);
        if (s_outstanding_requests > 0) {
            // wait until it is possible to create new thread
            #ifdef LOG
            printf("[async_launch] s_outstanding_requests = %d\n", s_outstanding_requests);
            #endif

            while (s_outstanding_requests >= s_max_threads) {
                #ifdef LOG
                printf("[async_launch] entering pthread_cond_wait() state\n");
                #endif
                pthread_cond_wait(&s_async_env_cond, &s_async_env_mutx);
            }
        }

        // launch main_result_trd as detached thread
        if (pthread_create(&worker_trd, NULL, s_worker, wrk_arg) < 0) {
            perror("[async_launch]] Failed to launch new thread\n");
            exit(EXIT_FAILURE);
        }

        pthread_detach(worker_trd);
        s_outstanding_requests++;

        #ifdef LOG
        printf("[async_launch] created thread\n");
        #endif
        pthread_mutex_unlock(&s_async_env_mutx);
        return 0;
    }
    else {
        fprintf(stderr, "[async_launch] Called while async sytate is not active\n");
        free(wrk_arg);
        return -1;
    }
}


/**
 * Initialize asynchronous environment.
 * If the initialization failed the function returns -1 otherwise 0
 */
int async_init (const int max_threads) {
    s_max_threads = max_threads;
    s_outstanding_requests = 0;
    s_outstanding_callbacks = 0;
    s_exit = false;

    if ((s_mailbox.queue = malloc(max_threads * sizeof(thread_result_t))) == NULL) {
        perror("[asunc-init] Failed to allocate RAM");
        exit(EXIT_FAILURE);
    }

    // launch main_result_trd as detached thread
    if (pthread_create(&s_main_thread, NULL, s_main_result_trd, NULL) < 0){
        perror("[async_inint] Failed to launch main tread\n");
        exit(EXIT_FAILURE);
    }

    // launch callback_trd as detached thread
    if (pthread_create(&s_callback_trd, NULL, s_callback, NULL) < 0){
        perror("[async_inint] Failed to launch callback tread\n");
        exit(EXIT_FAILURE);
    }

    return 0;
}

/**
 * Exit async state
 */
int async_terminate () {

    if (s_exit == true) {
        fprintf(stderr, "[async_terminate] is already terminated\n");
        return -1;
    }

    #ifdef LOG
    printf("[async_terminate] was called\n");
    #endif

    pthread_mutex_lock(&s_async_env_mutx);

    #ifdef LOG
    printf("[async_terminate] entering callback termination wait loop\n");
    #endif

    // wait untill all outstanding requests and callbacks are done
    while (s_outstanding_requests > 0 || s_outstanding_callbacks > 0) {

        // wait until it is possible to create new thread
        #ifdef LOG
        printf("[async_terminate] s_outstanding_requests = %d, s_outstanding_callbacks = %d\n", s_outstanding_requests, s_outstanding_callbacks);
        #endif
        pthread_cond_wait(&s_async_env_cond, &s_async_env_mutx);   // now wait for all callbacks to complete &s_async_env_mutx);
    }

    pthread_mutex_unlock(&s_async_env_mutx);  // allow s_main_result_trd to resume use of s_sync()

    #ifdef LOG
    printf("[async_terminate] signaling main_tread and callback_thread to exit\n");
    #endif

    pthread_mutex_lock(&s_callback_mutx);
    pthread_mutex_lock(&s_mailbox.mutex);
    s_exit = true;

    pthread_cond_signal(&s_callback_cond);
    pthread_mutex_unlock(&s_callback_mutx);

    pthread_cond_signal(&s_mailbox.cond);
    pthread_mutex_unlock(&s_mailbox.mutex);

    pthread_join(s_main_thread, NULL);
    pthread_join(s_callback_trd, NULL);

    // delete all synchronous functions that were initialzed
    s_s_funcs_release();

    #ifdef LOG
    printf("[async_terminate] Completed\n");
    #endif

    return 0;
}

/**
 * Sync all outstanding request.
 * Return to caller when all outstanding requests have completed
 * Return -1 if async_sync() was called after async_terminated() was called
 */
int async_sync () {

    #ifdef LOG
    printf("[async_sync] was called\n");
    #endif
    
    if (s_exit == false) {

        // complete process all outstanding requests       
        pthread_mutex_lock(&s_async_env_mutx);

        while (s_outstanding_requests > 0 || s_outstanding_callbacks > 0) {

            // wait until it is possible to create new thread
            #ifdef LOG
            printf("[async_sync] s_outstanding_requests = %d, s_outstanding_callbacks = %d\n", s_outstanding_requests, s_outstanding_callbacks);
            #endif

            pthread_cond_wait(&s_async_env_cond, &s_async_env_mutx);
        }

        pthread_mutex_unlock(&s_async_env_mutx);  // allow s_main_result_trd to resume use of s_sync()
        
        #ifdef LOG
        printf("[async_sync] return to MAIN\n");
        #endif

        return 0;
    }
    else {
        fprintf(stderr, "[async_sync] Called while async sytate is not active\n");
        return -1;
    }
}

/**
 * In an asynchrounous state, we need to allow shared resorces to be updated in a synchrounous way.
 * async_sync_func_init() allows us to declare a a function so that when other functions call it
 * the rfunction will run in a locked safe state.
 * It returns a pointer to async_s_func_t
 */
async_s_func_t *async_sync_func_init (void (*s_func)(void *arg, void *res)) {
    async_s_func_t *p_async_s_func;

    if ((p_async_s_func = malloc(sizeof(*p_async_s_func))) == NULL) {
        perror("[async_sync_func_init] Failed to allocate RAM\n");
        exit(EXIT_FAILURE);
    }

    // init function lock
    if (pthread_mutex_init(&(p_async_s_func->lock), NULL) < 0) {
        perror("[async_sync_func_init] Failed to allocate lock\n");
        exit(EXIT_FAILURE);
    }

    p_async_s_func->s_func = s_func;

    // stor s_func stracture for later relese
    s_s_funcs_add(p_async_s_func);

    return p_async_s_func;
}

/**
 * Call s_func().
 * Return 0 if all ok or -1 if called with false arguments
 */
int async_call (async_s_func_t *p_func, void *arg, void *res) {
    
    if (p_func == NULL)
        return -1;

    // lock
    pthread_mutex_lock(&(p_func->lock));

    // call the function
    p_func->s_func(arg, res);

    // release the lock
    pthread_mutex_unlock(&(p_func->lock));

    return 0;
}
