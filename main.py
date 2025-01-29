import datetime
import json
import logging
from logging.handlers import RotatingFileHandler

import websocket

# Basic configuration for logging
logging.basicConfig(
    level=logging.DEBUG,  # Set log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",  # Format the log messages
    handlers=[logging.StreamHandler()],
)  # Log to console

# Create a logger
logger = logging.getLogger(__name__)
# Add a file handler to log to a file
file_handler = RotatingFileHandler(
    f"logs/price-index-{datetime.datetime.now(datetime.UTC).strftime('%Y-%m-%d %H:%M:%S')}-UTC.log",
    maxBytes=1024 * 1024,  # 1MB max file size
    backupCount=1,  # Keep up to 5 backup files
)
file_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
)
logger.addHandler(file_handler)


url = (
    "wss://fstream.binance.com/stream?streams=btcusdt@markPrice@1s/ethusdt@markPrice@1s"
)

btc_csv = f"data/BTC-price-index-{datetime.datetime.now(datetime.UTC).strftime('%Y-%m-%d %H:%M:%S')}-UTC.csv"
eth_csv = f"data/ETH-price-index-{datetime.datetime.now(datetime.UTC).strftime('%Y-%m-%d %H:%M:%S')}-UTC.csv"


def on_message(ws, msg):
    msg = json.loads(msg)
    data = msg["data"]
    if data["e"] != "markPriceUpdate":
        logger.warning(f"unknown message event type: {msg}")
    if data["s"] == "BTCUSDT":
        output_file = btc_csv
    elif data["s"] == "ETHUSDT":
        output_file = eth_csv
    else:
        raise ValueError(f"unknown symbol: {msg}")
    with open(output_file, "a") as f:
        f.write(f"{data["E"]},{data["i"]}\n")


def run():
    wsapp = websocket.WebSocketApp(
        url,
        on_message=on_message,
        on_error=lambda ws, err: logger.error(err),
        on_close=lambda a, b, c: logger.info("Connection closed"),
        on_open=lambda ws: logger.info("Connection opened"),
    )
    wsapp.run_forever(reconnect=0)


if __name__ == "__main__":
    run()
