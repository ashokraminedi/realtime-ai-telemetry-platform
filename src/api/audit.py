import json
import logging
import time
import uuid

from fastapi import Request


logger = logging.getLogger("telemetry_api.audit")


async def audit_middleware(
    request: Request,
    call_next,
):
    request_id = str(uuid.uuid4())
    started_at = time.perf_counter()

    response = await call_next(request)

    duration_ms = round(
        (time.perf_counter() - started_at) * 1000,
        2,
    )

    audit_event = {
        "request_id": request_id,
        "method": request.method,
        "path": request.url.path,
        "status": response.status_code,
        "duration_ms": duration_ms,
    }

    logger.info(json.dumps(audit_event))

    response.headers["X-Request-ID"] = request_id

    return response