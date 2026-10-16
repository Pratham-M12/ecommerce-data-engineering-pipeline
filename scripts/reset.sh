#!/usr/bin/env bash
# ==============================================================================
# scripts/reset.sh
# Safely stop containers, delete volumes, and reset the local pipeline stack
# Compatible with macOS Terminal (bash/zsh) and Linux
# ==============================================================================

set -euo pipefail

# ANSI Color Codes
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${RED}====================================================================${NC}"
echo -e "${RED}      E-COMMERCE DATA ENGINEERING PIPELINE - STACK RESET TOOL       ${NC}"
echo -e "${RED}====================================================================${NC}"
echo ""

FORCE=false
if [[ "${1:-}" == "--force" || "${1:-}" == "-f" ]]; then
    FORCE=true
fi

if [ "$FORCE" = false ]; then
    echo -e "${YELLOW}WARNING: This action will:${NC}"
    echo "  1. Stop all running pipeline containers"
    echo "  2. REMOVE all persistent database volumes (Postgres, Mongo, RabbitMQ, Elasticsearch, NiFi, Airflow)"
    echo "  3. Delete orphaned Docker containers and networks"
    echo ""
    read -r -p "Are you sure you want to completely reset the infrastructure? (y/N): " CONFIRM
    if [[ ! "$CONFIRM" =~ ^[yY]([eE][sS])?$ ]]; then
        echo -e "${BLUE}Reset cancelled. No changes were made.${NC}"
        exit 0
    fi
fi

echo -e "\n${BLUE}Stopping containers and purging docker volumes...${NC}"
docker compose down -v --remove-orphans

echo -e "\n${BLUE}Cleaning temporary logs and run state (preserving directory structure)...${NC}"
rm -rf logs/* 2>/dev/null || true

echo ""
echo -e "${GREEN}====================================================================${NC}"
echo -e "${GREEN}✓ Infrastructure reset complete. You can now start with a fresh state:${NC}"
echo -e "   docker compose up -d"
echo -e "${GREEN}====================================================================${NC}"
