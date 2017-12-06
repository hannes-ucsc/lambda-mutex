from dyndbmutex import setup_logging, DynamoDbMutex, logger


def main(args):
    setup_logging()
    import inspect, argparse
    spec = inspect.getargspec(DynamoDbMutex.__init__)
    defaults = dict(zip(reversed(spec.args), reversed(spec.defaults)))
    parser = argparse.ArgumentParser(description='Mutexes for AWS using conditional PUTs in DynamoDB.')
    # required positionals
    parser.add_argument('mutex', metavar='MUTEX',
                        help='The name of the mutex.')
    parser.add_argument('action', metavar='ACTION', choices=['lock', 'release'],
                        help='The action to take on the mutex.')
    # optional flags
    assert defaults['region_name'] is None
    parser.add_argument('--region', '-r', metavar='NAME',
                        help='The name of the AWS region to host the mutexes in. The default depends on your Boto3 '
                             'configuration: http://boto3.readthedocs.io/en/latest/guide/configuration.html')
    parser.add_argument('--holder', '-H', metavar='NAME',
                        help='A name identifying the lock holder. The default is dynamically generated UUID.')
    parser.add_argument('--expiration', '-e', metavar='SECONDS', type=float, default=defaults['expiration'],
                        help='The number of seconds after which a lock is released automatically.')
    parser.add_argument('--blocking', '-b', action='store_' + ('false' if defaults['blocking'] else 'true'),
                        help='Do not exit until the mutex is locked or a timeout occurs.')
    parser.add_argument('--timeout', '-t', metavar='SECONDS', type=float, default=defaults['timeout'],
                        help='The maximum amount of time to block on attempting to lock a mutex.')
    options = parser.parse_args(args=args)
    mutex = DynamoDbMutex(options.mutex,
                          region_name=options.region,
                          holder=options.holder,
                          expiration=options.expiration,
                          timeout=options.timeout,
                          blocking=options.blocking)
    if options.action == 'lock':
        return mutex.lock()
    elif options.action == 'release':
        mutex.release()
        return True
    else:
        assert False


if __name__ == '__main__':
    import sys
    try:
        sys.exit( 0 if main(sys.argv[1:]) else 1)
    except SystemExit:
        raise
    except BaseException as ex:
        logger.exception(ex)
        sys.exit(2)
