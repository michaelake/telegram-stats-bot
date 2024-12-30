#!/usr/bin/env bash

# Para gerar o script de migração
# ./alembic.sh revision --autogenerate -m "calendar table"
poetry run alembic "$@"
