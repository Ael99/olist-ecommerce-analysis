import schedule
import time
from loguru import logger
from main import run_pipeline


# WHY: We use the 'schedule' library instead of Windows Task Scheduler
# because it keeps everything in Python — no need to configure external tools.
# The tradeoff is this script must be running continuously in the background.
# For a real production project you'd use Task Scheduler or a cloud scheduler,
# but for a learning project this is perfectly fine.


def run_with_error_handling():
    # WHY: We wrap run_pipeline() in a try/except so that if the pipeline
    # crashes (e.g. SQL Server is down, Kaggle is unreachable), the scheduler
    # doesn't crash with it. It logs the error and waits for the next run.
    # Without this, one failed run would stop ALL future runs permanently.
    try:
        run_pipeline()
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        logger.error("Will retry on next scheduled run.")


# WHY: We schedule for Monday 7am so the "weekly" update happens at the
# start of the business week — a common pattern in real data teams.
# You can change .monday to .sunday / .friday etc, and "07:00" to any time.
schedule.every().monday.at("07:00").do(run_with_error_handling)

logger.info("Scheduler started — pipeline runs every Monday at 07:00.")
logger.info("Running pipeline now for the first time...")

# WHY: We run once immediately on startup so you don't have to wait
# until Monday to see data. Every time you start the scheduler,
# it loads the next batch right away, then waits for the schedule.
run_with_error_handling()

while True:
    # WHY: This loop runs forever, checking every 60 seconds whether
    # a scheduled job is due. If it's Monday 07:00, it fires run_pipeline().
    # The script must stay running for the schedule to work —
    # closing the terminal stops all future runs.
    schedule.run_pending()
    time.sleep(60)