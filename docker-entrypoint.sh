#!/bin/sh
set -eu

max_attempts="${DB_STARTUP_MAX_ATTEMPTS:-30}"
attempt=1

while true; do
    if alembic upgrade head; then
        break
    fi

    if [ "$attempt" -ge "$max_attempts" ]; then
        echo "Failed to apply database migrations after ${attempt} attempt(s)" >&2
        exit 1
    fi

    echo "Database not ready for migrations yet, retrying (${attempt}/${max_attempts})..." >&2
    attempt=$((attempt + 1))
    sleep 2
done

exec "$@"
