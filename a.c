#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// worker argument
typedef struct {
    void    (*func)(void *arg, void *res);
    void    (*callback)(void *arg, void *res);
    void    *func_arg;
    void    *func_res;
    void    *callback_res;
} m_worker_arg_t;

// workers args entries
typedef struct m_worker_arg_entry_ {
    struct m_worker_arg_entry_ *next_free;
    struct m_worker_arg_entry_ *prev_free;
    m_worker_arg_t arg;
} m_worker_arg_entry_t;

#define arg_entry_add(arg) ((m_worker_arg_entry_t *)(((void *)(arg))-2*sizeof(m_worker_arg_t *)))
#define arg_add(arg_entry) ((m_worker_arg_t *)&((arg_entry)->arg))

static m_worker_arg_entry_t *s_p_workers_args;
static m_worker_arg_entry_t *s_p_free;

/**
 * Init workers args stracture
 * It also update s_p_free
 */
static m_worker_arg_entry_t *s_init_worke_args (const int max_threads) {
    m_worker_arg_entry_t *p_workers_arg;

    // allocate one more element for easy handling
    if ((p_workers_arg = malloc((max_threads+1)*sizeof(*p_workers_arg))) == NULL) {
        perror("[s_init_worke_args] Failed to allocate RAM");
        exit(EXIT_FAILURE);        
    }

    // initialze the args entries
    for (int i = 0; i < max_threads; i++) {
        p_workers_arg[i].next_free = &(p_workers_arg[i+1]);
        p_workers_arg[i+1].prev_free = &p_workers_arg[i];
    }


    p_workers_arg[max_threads].next_free = &p_workers_arg[0];
    p_workers_arg[0].prev_free = &p_workers_arg[max_threads];   
    s_p_free = p_workers_arg;

    return p_workers_arg;
}

/**
 * Get next free arg entry
 * It also update s_p_free
 */
static m_worker_arg_t *s_get_free_arg () {
    m_worker_arg_entry_t *p_workers_arg;

    p_workers_arg = s_p_free;
    s_p_free->prev_free->next_free = s_p_free->next_free;
    s_p_free->next_free->prev_free = s_p_free->prev_free;
    s_p_free = s_p_free->next_free;

    return arg_add(p_workers_arg);
}

/**
 * Return worker arg entry to free entries 
 */
static void s_release_worker_arg (m_worker_arg_t *worker_arg) {
    m_worker_arg_entry_t *p_worker_arg;

    p_worker_arg = arg_entry_add(worker_arg);

    p_worker_arg->next_free = s_p_free;
    p_worker_arg->prev_free = s_p_free->prev_free;
    s_p_free->prev_free->next_free = p_worker_arg;
    s_p_free->prev_free = p_worker_arg;
}

int main () {
    m_worker_arg_t *w_args[4];
    long int i;

    s_p_workers_args = s_init_worke_args(4);

    for (i = 0; i < 4; i++) {
        w_args[i] = s_get_free_arg();
        w_args[i]->func_arg = (void *)i;
    }


    for (i = 3; i >= 0; i--)
        s_release_worker_arg(w_args[i]);

        for (i = 0; i < 4; i++) {
        w_args[i] = s_get_free_arg();
        w_args[i]->func_arg = (void *)(i+10);
    }

    return 0;

}
