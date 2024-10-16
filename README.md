# Threaded Heartbeat Decorator Python

Runs heartbeats in a background thread alongside an activity thread via the `concurrent.futures.ThreadPoolExecutor`. 
Shows that cancellation reaches both the primary activity thread and heartbeat thread via print statements.

## Run

`poetry run python threaded_heartbeat_python/main.py`