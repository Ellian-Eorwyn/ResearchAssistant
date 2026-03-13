"""Entry point: starts the FastAPI server and opens the browser."""

import threading
import time
import webbrowser

import uvicorn

from backend.app import create_app

app = create_app()

HOST = "127.0.0.1"
PORT = 8000


def open_browser():
    time.sleep(1.5)
    webbrowser.open(f"http://{HOST}:{PORT}")


if __name__ == "__main__":
    threading.Thread(target=open_browser, daemon=True).start()
    uvicorn.run(app, host=HOST, port=PORT, log_level="info")
