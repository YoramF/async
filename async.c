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
#include <semaphore.h>
#include <stdbool.h>

#include <async.h>

#define TREADS_TO_SLOTS_RATIO 2
#define WARKERS_TO_CALLBACK_RATIO 3

// stack of indexes into worker data pool
typedef struct {
    int *stack;
    int slots;
    int top;
    pthread_mutex_t lock;
    sem_t sem_avialable;     // count how many slots are avialable in pool
} slot_pool_t;

// worker argument
typedef struct {
    void    (*func)(void *arg, void *res);          // worker will call this function, if NULL signal for worker to terminate
    void    (*callback)(void *arg, void *res);      // callback function
    void    *func_arg;                              // argument for function
    void    *func_res;                              // function result and callback argument
    void    *callback_res;                          // callback result
    int     slot_id;                                // track on slot task_t slot id.
} task_t;

// Index Queue (Circular Buffer)
typedef struct {
    int *buffer;
    int head;
    int tail;
    int slots;
    pthread_mutex_t lock;
    sem_t sem_empty;        // count how many slots are avialable in quque for push
    sem_t sem_full;         // count how many slots are full and ready for get
} index_queue_t;

struct internal_async_id;    // Forward declaration

// saynchronous function - internal struct definition
typedef struct {
    void                    (*s_func)(void *arg, void *res);
    struct internal_async_id *async_id;
    pthread_mutex_t          lock;
} internal_async_s_func_t;

// async "global" varibales needed for communication between main program and async environment - internal struct definition
typedef struct internal_async_id {
    task_t                      *tasks_pool;            // memory pool for tasks arguments and results
    pthread_t                   *workers_threads;       // array of worker thread
    pthread_t                   *callback_threads;      // callback thread - option for threads array
    slot_pool_t                 *slots_pool;            // pool of slots into memory pool. 
    index_queue_t               *requests_queue;        // worker requests cyclic queue
    index_queue_t               *results_queue;         // worker results cyclic queue
    index_queue_t               *callback_queue;        // callback cyclic queue
    internal_async_s_func_t     **sync_functions;       // array of pointers to synchronous functions
    int                         workers;                // number of execution workers threads
    int                         callbacks;              // number of callbacks threads
    int                         slots;                  // number of slots per queue
    int                         pool_size;              // number of tasks_t elements in tasks pool
    int                         sync_funcion_count;     // current number of synchronous functions elements
    pthread_t                   main_result_thread;     // main orchestrating thread
    pthread_mutex_t             global_mutex;           // environment global mutex 
    pthread_cond_t              global_cond;            // environment global cond
} internal_async_id_t;

/**
 * Initialize slots pool
 */
static slot_pool_t *s_init_slots_pool (int slots) {
    slot_pool_t *s_p;

    if ((s_p = malloc(sizeof(slot_pool_t))) == NULL) {
        perror("[s_init_slotd_pool] Failed to allocate RAM for slot_pool");
        goto failed1;
    }

    if ((s_p->stack = malloc(slots*sizeof(int))) == NULL) {
        perror("[s_init_slots_pool] Failed to allocate RAM for slot_pool->stack");
        goto failed2;
    }

    if (sem_init(&s_p->sem_avialable, 0, slots) < 0) {
        perror("[s_init_circ_queue] Failed to initialized semaphore");
        goto failed3;
    }

    if (pthread_mutex_init(&s_p->lock, NULL) < 0) {
        perror("[s_init_slots_pool] Failed to initialzed mutex");
        goto failed4;
    }

    s_p->top = slots-1;
    s_p->slots = slots;
    for (int i = 0; i < slots; i++)
        s_p->stack[i] = i;

    return s_p;

failed4:
    sem_destroy(&s_p->sem_avialable);
failed3:
    free(s_p->stack);
failed2:
    free(s_p);
failed1:
    return NULL;
}

/**
 * Initialze a circular queue
 */
static index_queue_t *s_init_circ_queue (int slots) {
    index_queue_t *i_p;

    if ((i_p = malloc(sizeof(index_queue_t))) == NULL) {
        perror("[s_init_circ_queue] Failed to allocate RAM for circular queue");
        goto failed1;
    }

    if ((i_p->buffer = malloc(slots*sizeof(int))) == NULL) {
        perror("[s_init_circ_queue] Failed to allocate RAM for queue buffer");
        goto failed2;
    }

    if (sem_init(&i_p->sem_empty, 0, slots) < 0) {
        perror("[s_init_circ_queue] Failed to initialized semaphore");
        goto failed3;
    }

    if (sem_init(&i_p->sem_full, 0, 0) < 0) {
        perror("[s_init_circ_queue] Failed to initialized semaphore");
        goto failed4;
    }

    if (pthread_mutex_init(&i_p->lock, NULL) < 0) {
        perror("[s_init_circ_queue] Failed to initialized mutex");  
        goto failed5;
    }

    i_p->head = 0;
    i_p->tail = 0;
    i_p->slots = slots;

    return i_p;

failed5:
    sem_destroy(&i_p->sem_full);  
failed4:
    sem_destroy(&i_p->sem_empty);
failed3:
    free(i_p->buffer);
failed2:
    free(i_p);
failed1:
    return NULL;
}


/**
 * Get free slot
 */
static int s_get_free_slot (slot_pool_t *pool) {
    int id;
    sem_wait(&pool->sem_avialable); // Wait for available
    pthread_mutex_lock(&pool->lock);
    if (pool->top < 0) {
        pthread_mutex_unlock(&pool->lock);
        return -1;
    }

    id = pool->stack[pool->top];
    (pool->top)--;

    pthread_mutex_unlock(&pool->lock);

    return id;
}

/**
 * Return slot to slots stack
 */
static int s_return_slot (slot_pool_t *pool, int id) {
    pthread_mutex_lock(&pool->lock);  
    if (pool->top >= pool->slots) {
        fprintf(stderr, "[s_return_slot] Fatal error. slot_pool was overflowed");
        pthread_mutex_unlock(&pool->lock);
        return -1;
    }

    (pool->top)++;
    pool->stack[pool->top] = id;

    pthread_mutex_unlock(&pool->lock);
    sem_post(&pool->sem_avialable); // increase count of avialbale
    return 0;
}

// pull task_id from queue
// since we call sem_trywait(sem_full)/sem_wait(sem_full) before calling function, we don't call sem_wait() at the beginning
static int s_queue_get (index_queue_t *q) {
    int id;
    pthread_mutex_lock(&q->lock);

    id = q->buffer[q->tail];
    q->tail = (q->tail + 1) % q->slots;

    pthread_mutex_unlock(&q->lock);    
    sem_post(&q->sem_empty); // increase count of empty slots
    return id;
}

// push task_id to queue
static void s_queue_push (index_queue_t *q, int id) {
    sem_wait(&q->sem_empty); // Wait for an empty slot
    pthread_mutex_lock(&q->lock);

    // Write directly into the pre-allocated slot
    q->buffer[q->head] = id;
    q->head = (q->head + 1) % q->slots;

    pthread_mutex_unlock(&q->lock);
    sem_post(&q->sem_full); // increase count of full slots ready to be processed 
}

/**
 * add new s_func to s_s_funcs store
 */
static void s_s_funcs_add (internal_async_s_func_t *p_s_func) {
    internal_async_id_t *async_id = p_s_func->async_id;
    int count = async_id->sync_funcion_count;

    if (count == 0) {
        if ((async_id->sync_functions = malloc(sizeof(internal_async_s_func_t *))) == NULL) {
            perror("[s_s_funcs_add] Failed to allocate RAM\n");
            exit(EXIT_FAILURE);
        }
    }
    else {
        if ((async_id->sync_functions = realloc(async_id->sync_functions, (size_t)(count+1)*sizeof(internal_async_s_func_t *))) == NULL) {
            perror("[s_s_funcs_add] Failed to reallocate RAM\n");
            exit(EXIT_FAILURE);           
        }
    }

    async_id->sync_functions[count] = p_s_func;
    async_id->sync_funcion_count++;
}

/**
 * Release all saved s_func structures
 */
static void s_s_funcs_release (internal_async_id_t *async_id) {
    int i;

    // first destroy all allocated mutexes
    for (i = 0; i < async_id->sync_funcion_count; i++)
        pthread_mutex_destroy(&(async_id->sync_functions[i]->lock));

    free(async_id->sync_functions);    // s_s_funcs.funcs is either a valid poiuner or NULL
    async_id->sync_funcion_count = 0;
    async_id->sync_functions = NULL;   // to make sure next calling to s_s_funcs_release () will not fail on free(invalid pointer)
}


/**
 * Main worker thread
 */
static void *s_worker(void* arg) {
    internal_async_id_t *async_id = (internal_async_id_t *)arg;
    int id;
    task_t *task;

    #ifdef LOG
    pthread_t t_id = pthread_self();
    printf("[worker] Thread was created, Thread ID: %lu\n", (unsigned long)t_id);
    #endif

    // get next job
    while (true) {
        // wait for next job request to be ready in queue
        sem_wait(&async_id->requests_queue->sem_full);
        id = s_queue_get(async_id->requests_queue);

        #ifdef LOG
        printf("[worker] got job, slot id: %d\n", id);
        #endif

        // fetch the task information
        task = &async_id->tasks_pool[id];

        // check if this is a kill worker request
        if (task->func == NULL) {
            // return slot id to pool
            if (s_return_slot(async_id->slots_pool, id) < 0)
                fprintf(stderr,"[s_worker] Failed to return task_id to slots pool\n");

            #ifdef LOG
            printf("[Worker] Finished\n");
            #endif

            // send dond signal to async_sync() if was issued
            pthread_cond_signal(&async_id->global_cond);

            return NULL;
        }

        // call the task function
        task->func(task->func_arg, task->func_res);

        // push task id to reult queue for further processing
        s_queue_push(async_id->results_queue, id);

        // send dond signal to async_sync() if was issued
        pthread_cond_signal(&async_id->global_cond);
    }
}

/**
 * Main callback execution thread
 * This thread is waiting for callbacks to be execute
 */
static void *s_callback (void *arg) {
    internal_async_id_t *async_id = (internal_async_id_t *)arg;
    int id;
    task_t *task;

    #ifdef LOG
    pthread_t t_id = pthread_self();
    printf("[callback] Thread was created, Thread ID: %lu\n", (unsigned long)t_id);
    #endif

    // get next job
    while (true) {
        // wait for next job request to be ready in queue
        sem_wait(&async_id->callback_queue->sem_full);
        id = s_queue_get(async_id->callback_queue);

        #ifdef LOG
        printf("[callback] got job, slot id: %d\n", id);
        #endif

        // fetch the task information
        task = &async_id->tasks_pool[id];

        // check if this is a kill worker request
        if (task->callback == NULL) {
            // return slot id to pool
            if (s_return_slot(async_id->slots_pool, id) < 0)
                fprintf(stderr,"[s_callback] Failed to return task_id to slots pool\n");

            // send dond signal to async_sync() if was issued
            pthread_cond_signal(&async_id->global_cond);

            #ifdef LOG
            printf("[callback] Finished\n");
            #endif

            return NULL;
        }

        // call the task callback. function_res is callback arg.
        task->callback(task->func_res, task->callback_res);

        // callback releases the task slot back to tasks pool
        if (s_return_slot(async_id->slots_pool, id) < 0)
            fprintf(stderr,"[s_callback] Failed to return task_id to slots pool\n");

        // send dond signal to async_sync() if was issued
        pthread_cond_signal(&async_id->global_cond);
    }
}

/**
 * return true if a slots pool is full (i.e. no outstanding work/callback request)
 */
static bool s_is_pool_full (slot_pool_t *p) {
    int val;
    pthread_mutex_lock(&p->lock);
    if (sem_getvalue(&p->sem_avialable, &val) < 0) {
        perror("[s_is_pool_full] Failed to call sem_getvalue");
        return false;
    }
    pthread_mutex_unlock(&p->lock);
    return (val == p->slots);
}

/**
 * This is the main result thread. It runs as long as the async envirinment is active and as long as there are outstanding
 * async requests active. Per thread working thread completion, it will add a callback workload for callback thread
 * and signal it about new warkload to process
 */
static void *s_main_result_trd (void *arg) {
    internal_async_id_t *async_id = (internal_async_id_t *)arg;
    int empty;

    #ifdef LOG
    printf("[s_main_result_trd] started\n");
    #endif

    // drain result_queue
    while (sem_wait(&async_id->results_queue->sem_full) == 0) {
        task_t *task;
        int id;

        id = s_queue_get(async_id->results_queue);
        task = &async_id->tasks_pool[id];

        // check if callback is included
        if (task->callback != NULL) {
            // insert task_id to callback queue
            #ifdef LOG
            printf("[s_main_result_trd] insert result_id  %d to callback queue\n", id);
            #endif
            s_queue_push(async_id->callback_queue, id);
        }
        else {
            // return slot back to slot pool
            if (s_return_slot(async_id->slots_pool, id) < 0)
                fprintf(stderr, "[s_main_result_trd] Failed to release task slot\n");
        }
    }

    return NULL;
}

/**
 * Initiate all async environment threads
 */
static int s_launch_threads (internal_async_id_t *async_id) {
    int cb, wr, i;

    // launch callbacks
    for (i = 0; i < async_id->callbacks; i++) {
        if (pthread_create(&async_id->callback_threads[i], NULL, s_callback, async_id) < 0) {
            perror("[s_launch_threads] Failed to launch a callback thread");
            cb = i;
            goto failed1;
        }
    }
    cb = async_id->callbacks;

    // launch workers
    for (i = 0; i < async_id->workers; i++) {
        if (pthread_create(&async_id->workers_threads[i], NULL, s_worker, async_id) < 0) {
            perror("[s_launch_threads] Failed to launch a worker thread");
            wr = i;
            goto failed2;
        }
    }
    wr = async_id->workers;
    
    // launch main results thread
    if (pthread_create(&async_id->main_result_thread, NULL, s_main_result_trd, async_id) < 0) {
        perror("[s_launch_threads] Failed to launch main thread");
        goto failed2;
    }

    return 0;

failed2:
    // cancel all workers threads
    for (i = 0; i < cb; i++)
        pthread_cancel(async_id->workers_threads[i]);

failed1:
    // cancel all callbacks threads
    for (i = 0; i < cb; i++)
        pthread_cancel(async_id->callback_threads[i]);

    return -1;
}

/**
 * Initialize asynchronous environment.
 * If the initialization failed the function returns -1 otherwise 0
 */
async_id_t async_init (const int max_threads) {
    internal_async_id_t *i_a_id;
    int slots, callbacks;
    int pool_size;

    slots = max_threads * TREADS_TO_SLOTS_RATIO;

    // make sure we get at least one callback thread
    callbacks = (max_threads >  WARKERS_TO_CALLBACK_RATIO)? max_threads / WARKERS_TO_CALLBACK_RATIO: 1;

    pool_size = (slots + 1) * 3; // allow one extra memory slot per queue

    if ((i_a_id = malloc(sizeof(internal_async_id_t))) == NULL) {
        perror("[async_init] Failed to allocate RAM for async_id structure");
        return NULL;
    }
    i_a_id->pool_size = pool_size;
    i_a_id->slots = slots;
    i_a_id->workers = max_threads;
    i_a_id->callbacks = callbacks;
    i_a_id->sync_funcion_count = 0;
    i_a_id->sync_functions = NULL;

    if ((i_a_id->tasks_pool = malloc(pool_size*sizeof(task_t))) == NULL) {
        perror("[async_init] Failed to allocate RAM for task pool");
        goto failed1;
    }

    if ((i_a_id->slots_pool = s_init_slots_pool(pool_size)) == NULL)
        goto failed2;

    if ((i_a_id->requests_queue = s_init_circ_queue(slots)) == NULL)
        goto failed3;

    if ((i_a_id->results_queue = s_init_circ_queue(slots)) == NULL)
        goto failed4;

    if ((i_a_id->callback_queue = s_init_circ_queue(slots)) == NULL)
        goto failed5;

    if ((i_a_id->workers_threads = malloc(max_threads*sizeof(pthread_t))) == NULL) {
        perror("[async_init] Failed to allocate RAM for workers threads");
        goto failed6;
    }

    if ((i_a_id->callback_threads = malloc(callbacks*sizeof(pthread_t))) == NULL) {
        perror("[async_init] Failed to allocate RAM for callback threads");
        goto failed7;
    }

    if (pthread_mutex_init(&i_a_id->global_mutex, NULL) < 0) {
        perror("[async_init] Failed to initialized global mutex");
        goto failed8;
    }

    if (pthread_cond_init(&i_a_id->global_cond, NULL) < 0) {
        perror("[async_init] Failed to initialized global condition");
        goto failed9;        
    }

    if (s_launch_threads(i_a_id) < 0) {
        fprintf(stderr, "[async_init] Failed to initiate async environment\n");
        goto failed9;
    }

    return (async_id_t)i_a_id;

failed9:
    pthread_mutex_destroy(&i_a_id->global_mutex);
failed8:
    free(i_a_id->callback_threads);
failed7:
    free(i_a_id->workers_threads);
failed6:
    free(i_a_id->callback_queue);
failed5:
    free(i_a_id->results_queue);
failed4:
    free(i_a_id->requests_queue);
failed3:
    free(i_a_id->slots_pool);
failed2:
    free(i_a_id->tasks_pool);
failed1:
    free(i_a_id);
    return NULL;
}

/**
 * This is the main launcher function. It creates new working thread to execute the async function.
 * In case the number of thread exceeds max_threads, it will block until a new thread can be created.
 * callback_func *arg are ex_function *res
 */
int async_launch (void (*ex_func)(void *arg, void *res), void (*cb_func)(void *arg, void *res),
    void *ex_arg, void *ex_res, void *cb_res, async_id_t async_id) {

    internal_async_id_t *i_async_id = (internal_async_id_t *)async_id;
    int id;
    task_t *task;

    // get a slot in the requests queue. get task memory slot to fill request information
    id = s_get_free_slot(i_async_id->slots_pool);
    task = &i_async_id->tasks_pool[id];

    task->callback = cb_func;
    task->func = ex_func;
    task->func_arg = ex_arg;
    task->func_res = ex_res;
    task->callback_res = cb_res;
    task->slot_id = id;

    // push the task id to request queue
    s_queue_push(i_async_id->requests_queue, id);

    return 0;
}


/**
 * Exit async state
 */
int async_terminate (async_id_t async_id) {
    internal_async_id_t *a_id = (internal_async_id_t *)async_id;
    int i, id;
    task_t *task;

    #ifdef LOG
    printf("[async_terminate] was called\n");
    printf("[async_terminate] wait for all work to complete - sending poison pill to all workers and callbacks\n");
    #endif

    pthread_mutex_lock(&a_id->global_mutex);
    // wait untill all outstanding requests/results/callback are done
    while (!s_is_pool_full(a_id->slots_pool)) {
        pthread_cond_wait(&a_id->global_cond, &a_id->global_mutex);
    }
    pthread_mutex_unlock(&a_id->global_mutex);

    // push poison pill to all workers
    for (i = 0; i < a_id->workers; i++) {
        id = s_get_free_slot(a_id->slots_pool);
        task = &a_id->tasks_pool[id];
        task->func = NULL;
        task->callback = NULL;
        s_queue_push(a_id->requests_queue, id);
    }

    // wait for all workers to exit
    for (i = 0; i < a_id->workers; i++)
        if (pthread_join(a_id->workers_threads[i], NULL) < 0);

    // push poison pill to all callbacks
    for (i = 0; i < a_id->callbacks; i++) {
        id = s_get_free_slot(a_id->slots_pool);
        task = &a_id->tasks_pool[id];
        task->callback = NULL;
        s_queue_push(a_id->callback_queue, id);
    }

    // whait for all callbacks to exit
    for (i = 0; i < a_id->callbacks; i++)
        if (pthread_join(a_id->callback_threads[i], NULL) < 0);

    #ifdef LOG
    printf("[async_terminate] signaling main_tread to exit\n");
    #endif


    // cancel main thread 
    pthread_cancel(a_id->main_result_thread);

    // delete all synchronous functions that were initialzed
    s_s_funcs_release(a_id);

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
int async_sync (async_id_t async_id) {
    internal_async_id_t *a_id = (internal_async_id_t *)async_id;

    #ifdef LOG
    printf("[async_sync] was called\n");
    #endif
    
    // complete process all outstanding requests       
    pthread_mutex_lock(&a_id->global_mutex);
    // what for all queues to be empty
    while (!s_is_pool_full(a_id->slots_pool)) {
        pthread_cond_wait(&a_id->global_cond, &a_id->global_mutex);
    }
    pthread_mutex_unlock(&a_id->global_mutex);

    #ifdef LOG
    printf("[async_sync] return to MAIN\n");
    #endif

    return 0;
}

/**
 * In an asynchrounous state, we need to allow shared resorces to be updated in a synchrounous way.
 * async_sync_func_init() allows us to declare a a function so that when other functions call it
 * the rfunction will run in a locked safe state.
 * It returns a pointer to async_s_func_t
 */
async_s_func_t async_sync_func_init (void (*s_func)(void *arg, void *res), async_id_t async_id) {
    internal_async_s_func_t *p_async_s_func;
    internal_async_id_t *i_async_id = (internal_async_id_t *)async_id;

    if ((p_async_s_func = malloc(sizeof(*p_async_s_func))) == NULL) {
        perror("[async_sync_func_init] Failed to allocate RAM\n");
        return NULL;
    }

    // init function lock
    if (pthread_mutex_init(&(p_async_s_func->lock), NULL) < 0) {
        perror("[async_sync_func_init] Failed to allocate lock\n");
        free(p_async_s_func);
        return NULL;
    }

    p_async_s_func->s_func = s_func;
    p_async_s_func->async_id = i_async_id;

    // stor s_func stracture for later relese
    s_s_funcs_add(p_async_s_func);

    return (async_s_func_t)p_async_s_func;
}

/**
 * Call s_func().
 * Return 0 if all ok or -1 if called with false arguments
 */
int async_call (async_s_func_t p_func, void *arg, void *res) {
    internal_async_s_func_t *p_f = (internal_async_s_func_t *)p_func;    
    
    if (p_func == NULL)
        return -1;

    // lock
    pthread_mutex_lock(&(p_f->lock));

    // call the function
    p_f->s_func(arg, res);

    // release the lock
    pthread_mutex_unlock(&(p_f->lock));

    return 0;
}
