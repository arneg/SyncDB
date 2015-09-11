void reader() {
    for (int i = 0; i < 5; i++) {
        object lock = rw->lock_read();
        sleep(2 + random(1.0));
        lock = 0;
    }
}

object rw = SyncDB.ReaderWriterLock();

void writer() {
    for (int i = 0; i < 5; i++) {
        object lock = rw->lock_write();
        sleep(2 + random(1.0));
        lock = 0;
    }
}

int main() {
    array t = ({});
    t += map(allocate(10, reader), Thread.Thread);
    t += map(allocate(4, writer), Thread.Thread);

    t->wait();

    return 0;
}
