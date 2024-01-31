import sys


def run_job(job_day: str):
    print("Running job for day: ", job_day)


if __name__ == "__main__":
    day = sys.argv[1]
    run_job(day)
