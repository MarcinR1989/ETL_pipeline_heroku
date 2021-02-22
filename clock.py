import os
from apscheduler.schedulers.blocking import BlockingScheduler
import main

sched = BlockingScheduler()

minutes = 5


# @sched.scheduled_job('interval', minutes=minutes)
# def timed_job():
#     print(f'This job is run every {minutes} minutes.')
#     main.execute_etl_pipeline(os.environ.get("DOWNLOAD_DIR"))


@sched.scheduled_job('cron', hour=1)
def scheduled_job_1():
    print('This job is run every day at 2am.')
    for n in range(10):
        try:
            main.execute_etl_pipeline(os.environ.get("DOWNLOAD_DIR"))
        except:
            print(f"SOMETHING WENT WRONG. I'LL TRY AGAIN. TRIES: {n}")
        else:
            print(f"UPLOAD COMPLETED! (in {n} run)")
            break


sched.start()



