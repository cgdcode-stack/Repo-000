import logging
import sys

from weather_app.pipeline import run_pipeline


def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


if __name__ == "__main__":
    configure_logging()
    try:
        run_pipeline()
    except Exception:
        logging.exception("Weather pipeline failed")
        raise