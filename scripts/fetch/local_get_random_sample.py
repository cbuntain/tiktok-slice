import datetime
import asyncio

import get_random_sample

def main():
    generation_strategy = 'all'
    start_time = datetime.datetime(2025, 5, 10, 0, 0, 0)
    num_time = 1
    time_unit = 's'
    num_workers = 2
    reqs_per_ip = 200
    batch_size = 8000
    task_batch_size = 120
    task_nthreads = 4
    task_timeout = 20
    max_task_tries = 2
    worker_cpu = 256
    worker_mem = 512
    cluster_type = 'raspi'
    method = 'async'
    asyncio.run(get_random_sample.get_random_sample(
        generation_strategy,
        start_time,
        num_time,
        time_unit,
        num_workers,
        reqs_per_ip,
        batch_size,
        task_batch_size,
        task_nthreads,
        task_timeout,
        max_task_tries,
        worker_cpu,
        worker_mem,
        cluster_type,
        method
    ))

if __name__ == '__main__':
    main()
