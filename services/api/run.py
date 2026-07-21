import os

import uvicorn

from radar.runtime import validate_local_access_configuration


if __name__ == "__main__":
    validate_local_access_configuration()
    uvicorn.run(
        "radar.main:app",
        host=os.getenv("RADAR_API_HOST", "127.0.0.1"),
        port=int(os.getenv("RADAR_API_PORT", "8017")),
        reload=os.getenv("RADAR_API_RELOAD", "false").lower() == "true",
    )
