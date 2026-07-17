# Release builds must provide an immutable repository@sha256 base image.
ARG PYTHON_BASE_IMAGE
FROM ${PYTHON_BASE_IMAGE}

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app
WORKDIR /app
COPY services/api/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY services/api/radar /app/radar
COPY config /app/config
USER 65532:65532
CMD ["python", "-m", "uvicorn", "radar.main:app", "--host", "0.0.0.0", "--port", "8017"]
