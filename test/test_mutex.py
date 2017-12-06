import time
import unittest
import base64
import sys
import os
from os import path

from concurrent.futures import ThreadPoolExecutor

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from dyndbmutex import DynamoDbMutex, AcquireLockFailedError, setup_logging


def random_name():
    return base64.b32encode(os.urandom(5))[:7].decode('ascii')


class TestDynamoDbMutex(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        setup_logging()
        super(TestDynamoDbMutex, cls).setUpClass()

    def test_create(self):
        m = DynamoDbMutex(random_name(), holder="myself", expiration=3)
        assert(m.lock())
        m.release()

    def test_create_delete_us_east_1(self):
        m = DynamoDbMutex(random_name(), holder=random_name(), region_name='us-east-1')
        assert(m.lock())
        m.release()
        DynamoDbMutex.delete_table(region_name='us-east-1')

    def test_timeout(self):
        m = DynamoDbMutex(random_name(), holder="myself", expiration=3)
        m.lock()
        time.sleep(5)
        assert(m.lock())
        m.release()

    def test_mutual_exclusion(self):
        m = DynamoDbMutex(random_name(), holder=random_name())
        m.lock()
        assert(m.lock() == False)
        m.release()

    def test_with(self):
        class SpecialException(RuntimeError):
            pass
        m = DynamoDbMutex(random_name(), holder=random_name())
        try:
            with m:
                assert m.is_locked()
                time.sleep(3)
                raise SpecialException
        except SpecialException:
            print("In exception handler")
            assert not m.is_locked()

    def test_with_fail(self):
        name = random_name()
        m1 = DynamoDbMutex(name, holder=random_name())
        m1.lock()
        m2 = DynamoDbMutex(name, holder=random_name())
        exceptionHappened = False
        try:
            with m2:
                time.sleep(3)
        except AcquireLockFailedError:
            print("In exception handler")
            assert(m2.is_locked() == False)
            exceptionHappened = True
        assert(exceptionHappened)

    def test_release_expired(self):
        name = random_name()
        caller = "caller1"
        m1 = DynamoDbMutex(name, holder=caller, expiration=2)
        m1.lock()
        time.sleep(3)
        caller = "caller2"
        m2 = DynamoDbMutex(name, holder=caller, expiration=2)
        assert(m2.lock())
        m1.release()
        assert(m2.is_locked())
        m2.release()

    def test_table_creation_race(self):
        old_table_name = os.environ.get('DD_MUTEX_TABLE_NAME')
        try:
            table_name = 'Mutex-' + random_name()
            os.environ['DD_MUTEX_TABLE_NAME'] = table_name
            cls = DynamoDbMutex

            def create_mutex(i):
                mutex_name = random_name()
                mutex = cls(name=mutex_name, holder='caller%i' % i)
                self.assertEquals(mutex.table.table_name, table_name)
                return mutex

            num_threads = 2
            with ThreadPoolExecutor(max_workers=num_threads) as pool:
                mutexes = pool.map(create_mutex, range(num_threads))
                for method in cls.lock, cls.release, cls.delete_table:
                    pool.map(method, mutexes)
        finally:
            if old_table_name is None:
                del os.environ['DD_MUTEX_TABLE_NAME']
            else:
                os.environ['DD_MUTEX_TABLE_NAME'] = old_table_name


if __name__ == "__main__":
    unittest.main()
