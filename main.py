import datetime
import json
import logging

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
file_handler = logging.FileHandler(
    f"logs/price-index-{datetime.datetime.now(datetime.UTC).strftime('%Y-%m-%d %H:%M:%S')}-UTC.log"
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
        on_error=lambda ws, err: print(err),
        on_close=lambda ws: print("Connection closed"),
        on_open=lambda ws: print("Connection opened"),
    )
    wsapp.run_forever()


def main():
    while True:
        try:
            run()
        except Exception as e:
            logger.warning(e)
