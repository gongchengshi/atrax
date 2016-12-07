from atrax.management.aws_env.frontier_controller import FrontierController
from atrax.management.aws_env.instance_accessor import AwsInstanceAccessor


def test_start():
    target = FrontierController('dummy_crawl_job')
    target.start()
    actual = AwsInstanceAccessor('dummy_crawl_job').get_frontier_instance()
    assert(actual is not None)

    start_result = raw_input("Is frontier running? (y/n): ")

    if start_result != 'y':
        exit(-1)


def test_stop():
    target = FrontierController('dummy_crawl_job')
    target.stop()

    stop_result = raw_input("Did the frontier stop? (y/n)")

    if stop_result != 'y':
        exit(-1)

# test_start()
# test_stop()


def test_persist():
    target = FrontierController('siemens27012015')
    target.persist()

test_persist()


def test_restore():
    target = FrontierController('siemens27012015')
    target.restore()

test_restore()
