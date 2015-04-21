class RowBased(mapping row) {

    mixed `[](mixed key) {
	return row[key];
    }

    mixed `[]=(mixed key, mixed val) {
	return row[key] = val;
    }

    mixed `->=(mixed key, mixed val) {
	return row[key] = val;
    }

    int _mappingp() {
	return 1;
    }
}

class DeletedRow {
    inherit RowBased;
}

mapping(mixed:mapping(string:object)) managers = ([]);

object get_update_manager(mixed db, string table, void|int interval) {
    if (!has_index(managers, db)) {
        managers[db] = set_weak_flag(([]), Pike.WEAK_VALUES);
    }

    mapping m = managers[db];
    object manager = m[table];

    if (!manager) {
        manager = SyncDB.UpdateManager(interval);
        m[table] = manager;
    } else if (interval) {
        int cinterval = manager->interval;
        if (!cinterval || interval < cinterval) {
            manager->create(interval);
        }
    }

    return manager;
}

class ReaderWriterLockKey {
    private ReaderWriterLock lock;
    private Thread.MutexKey key;

    function done_cb;

    protected void create(ReaderWriterLock lock, Thread.MutexKey key) {
        this_program::lock = lock;
        this_program::key = key;
    }

    void destroy() {
        if (done_cb) {
            mixed err = catch(done_cb());
            if (err) master()->handle_error();
        }
        lock->writer_done(key);
    }
}

//! A ReaderWriterLock which prioritizes writers. It is probably not very efficient, but has some special
//! features that we need for Database handling.
class ReaderWriterLock() {
    object mutex = Thread.Mutex();

    int num_writers = 0;
    object wants_to_write = Thread.Condition();
    object wants_to_read = Thread.Condition();

    object actual_mutex = Thread.Mutex();
    array reader_queue = ({ });
    Thread.MutexKey read_key = actual_mutex->lock();

    private Thread.Thread current_writer;

    ReaderWriterLockKey lock_write() {
        object key = mutex->lock();

        num_writers ++;

        if (current_writer == Thread.this_thread())
            error("Recursive write lock.\n");

        if (num_writers > 1) {
            // there is other writers still going
            wants_to_write->wait(key);
        }

        read_key = 0;

        current_writer = Thread.this_thread();

        destruct(key);

        // this looks racy, but in fact noone else will write to read_key
        // while we are at it.

        Thread.MutexKey my_key = read_key = actual_mutex->lock();

        
        return ReaderWriterLockKey(this, my_key);
    }

    void writer_done(Thread.MutexKey read_key) {
        object key = mutex->lock();

        num_writers --;

        current_writer = 0;

        // wake up one writer
        if (num_writers) {
            destruct(read_key);
            wants_to_write->signal();
            destruct(key);
        } else {
            array a = reader_queue;

            if (sizeof(a)) {
                reader_queue = ({ });
            }

            this_program::read_key = read_key;
            destruct(key);
            wants_to_read->broadcast();

            if (sizeof(a)) {
                foreach (a;; array c) {
                    mixed err = catch(c[0](@c[1], read_key));
                    if (err) master()->handle_error(err);
                }
            }
        }
    }

    Thread.MutexKey lock_read() {
        Thread.MutexKey ret;
        object key = mutex->lock();

        while (num_writers) {
            wants_to_read->wait(key);
        }

        ret = read_key;

        destruct(key);

        return ret;
    }

    Thread.MutexKey try_lock_read() {
        Thread.MutexKey ret;
        object key = mutex->lock();

        if (!num_writers) {
            ret = read_key;
        }

        destruct(key);

        return ret;
    }

    Thread.MutexKey lock_read_or_callback(function f, mixed ... args) {
        object key = mutex->lock();

        if (num_writers) {
            reader_queue += ({ ({ f, args }) });
            destruct(key);
            return 0;
        } else {
            Thread.MutexKey ret = read_key;
            destruct(key);
            return ret;
        }
    }

    void call_with_read_key(function f, mixed ... args) {
        object read_key = lock_read_or_callback(f, @args);

        if (read_key) f(@args, read_key);
    }
}

//! A somewhat special thread farm. It is always protected by one mutex and calls to run() must hold
//! a corresponding lock. It can be waited for completion by one thread. This is usually the thread which
//! created the farm (while holding a lock to the mutex).
class Farm {
    private Thread.Mutex mutex;
    array threads = ({ });
    private Thread.Queue jobs = Thread.Queue();
    private Thread.Queue errors = Thread.Queue();
    private Thread.Condition cond = Thread.Condition();

    int max_threads = 5;

    string debug_status() {
        return sprintf("Threads: %O\nJobs: %d\n", threads, jobs->size());
    }

    void create(Thread.Mutex mutex) {
        this_program::mutex = mutex;
    }

    void handle(function(void:array) read) {
        function f, cb;
        array args;
        while (array job = read()) {
            f = job[1];
            cb = job[0];
            args = job[2];
            mixed res;
            mixed err = catch(res = f(@args));

            if (cb) {
                if (err) {
                    cb(0, err);
                } else {
                    cb(1, res);
                }
            }
        }
    }

    void worker() {
        mixed err;
        do {
            err = catch {
                handle(jobs->read);
            };
            if (err) {
                werror("error when processing jobs:\n");
                master()->handle_error(err);
            }
        } while (err);

        object key = mutex->lock();
        threads -= ({ Thread.this_thread() });
        destruct(key);
        cond->signal();
    }

    // thread is expected to hold a lock to mutex
    void run(function cb, function f, mixed ... args) {
        jobs->write(({ cb, f, args }));

        // only start more threads if any have started at all
        if (sizeof(threads)) {
            if (max_threads > sizeof(threads) && sizeof(jobs) > 1) {
                threads += ({ Thread.Thread(worker) });
            }
        }
    }

    Thread.MutexKey wait(Thread.MutexKey key) {
        threads += ({ Thread.this_thread() });
        if (sizeof(threads)) {
            while (max_threads > sizeof(threads) && sizeof(jobs) > sizeof(threads)) {
                threads += ({ Thread.Thread(worker) });
            }
        }
        destruct(key);
        while (1) {
            mixed err = catch(handle(jobs->try_read));
            if (err) {
                werror("error when processing jobs from waiting thread:\n");
                master()->handle_error(err);
                if (jobs->size()) continue;
            }
            key = mutex->lock();
            // shut down one thread
            jobs->write(0);
            if (sizeof(threads) == 1) return key;
            cond->wait(key);
            destruct(key);
        }
    }

    void kill() {
        threads->kill();
    }
}
