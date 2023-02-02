import multiprocessing
import time
def workerA():
    lock = multiprocessing.RLock()
    lock.acquire()
    print("workerA is working")
    time.sleep(2)
    print("workerA is done")
    lock.release()

def workerB():
    lock = multiprocessing.RLock()
    lock.acquire()
    print("workerB is working")
    time.sleep(2)
    print("workerB is done")
    lock.release()

if __name__ == "__main__":
    lock = multiprocessing.Lock()
    p1 = multiprocessing.Process(target=workerA)
    p2 = multiprocessing.Process(target=workerB)
    p1.start()
    p2.start()
    p1.join()
    p2.join()