import asyncio
from regional.routing_server import serve


if __name__ == "__main__":
    print("[Chicago Regional] Starting EMBER routing server...")
    asyncio.run(serve())
