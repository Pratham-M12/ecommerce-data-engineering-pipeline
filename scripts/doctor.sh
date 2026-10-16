#!/usr/bin/env bash
# ==============================================================================
# scripts/doctor.sh
# Diagnostic and Health Inspection Script for Local Infrastructure
# Compatible with macOS Terminal (bash/zsh) and Linux
# ==============================================================================

set -uo pipefail

# ANSI Color Codes
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo -e "${BLUE}====================================================================${NC}"
echo -e "${BLUE}   E-COMMERCE DATA ENGINEERING PIPELINE - INFRASTRUCTURE DOCTOR    ${NC}"
echo -e "${BLUE}====================================================================${NC}"
echo ""

TOTAL_CHECKS=0
PASSED_CHECKS=0
FAILED_CHECKS=0

report_pass() {
    echo -e "  [${GREEN} PASS ${NC}] $1"
    PASSED_CHECKS=$((PASSED_CHECKS + 1))
    TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
}

report_fail() {
    echo -e "  [${RED} FAIL ${NC}] $1"
    echo -e "         ${YELLOW}↳ Remediation: $2${NC}"
    FAILED_CHECKS=$((FAILED_CHECKS + 1))
    TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
}

report_warn() {
    echo -e "  [${YELLOW} WARN ${NC}] $1"
    echo -e "         ${YELLOW}↳ Note: $2${NC}"
}

# 1. Check Docker Engine
echo -e "${CYAN}1. Checking Docker Engine & CLI...${NC}"
if command -v docker >/dev/null 2>&1; then
    report_pass "Docker CLI is installed ($(docker --version))"
else
    report_fail "Docker CLI is not found in PATH" "Install Docker Desktop from https://www.docker.com/products/docker-desktop"
fi

if docker info >/dev/null 2>&1; then
    report_pass "Docker daemon is running"
else
    report_fail "Docker daemon is unreachable" "Start Docker Desktop application and wait for the engine icon to turn green."
fi

# 2. Check Docker Compose
echo -e "\n${CYAN}2. Checking Docker Compose...${NC}"
if docker compose version >/dev/null 2>&1; then
    report_pass "Docker Compose is installed ($(docker compose version))"
else
    report_fail "Docker Compose is not available" "Ensure Docker Desktop with Compose v2 is active."
fi

# 3. Check Configuration Files
echo -e "\n${CYAN}3. Checking Environment Configuration...${NC}"
if [ -f ".env" ]; then
    report_pass "Found .env configuration file"
else
    report_fail "Missing .env file" "Run 'cp .env.example .env' in the project root."
fi

# 4. Check Docker Compose File Syntax
echo -e "\n${CYAN}4. Validating Compose Syntax...${NC}"
if docker compose config --quiet >/dev/null 2>&1; then
    report_pass "docker-compose.yml configuration syntax is valid"
else
    report_fail "docker-compose.yml has syntax errors" "Run 'docker compose config' to view validation errors."
fi

# 5. Service Status and Healthchecks (if containers are running)
echo -e "\n${CYAN}5. Inspecting Container Runtime States...${NC}"
if ! docker info >/dev/null 2>&1; then
    report_warn "Skipping runtime checks" "Docker daemon is stopped."
else
    echo -e "--- Container Status Table ---"
    docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null || true
    echo "------------------------------"

    # Function to check port reachability
    check_tcp_port() {
        local name=$1
        local host=$2
        local port=$3
        if nc -z -w2 "$host" "$port" 2>/dev/null || (echo > /dev/tcp/"$host"/"$port") 2>/dev/null; then
            report_pass "$name port $port is listening and accessible"
        else
            report_fail "$name port $port is not responding" "Verify container is running with 'docker compose logs $name'"
        fi
    }

    # Function to check HTTP/HTTPS endpoint
    check_http() {
        local name=$1
        local url=$2
        local extra_flags=${3:-""}
        local http_code
        http_code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 3 $extra_flags "$url" 2>/dev/null || echo "000")
        if [[ "$http_code" =~ ^(200|302|401)$ ]]; then
            report_pass "$name endpoint returned HTTP $http_code ($url)"
        else
            report_fail "$name endpoint failed ($url) - code: $http_code" "Container may still be starting or in error. Check 'docker compose logs $name'."
        fi
    }

    echo -e "\n${CYAN}6. Verifying Service Network Endpoints...${NC}"
    check_tcp_port "PostgreSQL" "localhost" 5432
    check_tcp_port "MongoDB" "localhost" 27017
    check_tcp_port "RabbitMQ AMQP" "localhost" 5672
    check_http "RabbitMQ Management UI" "http://localhost:15672"
    check_http "Elasticsearch REST API" "http://localhost:9200"
    check_http "Apache NiFi HTTPS UI" "https://localhost:8443/nifi/" "-k"
    check_http "Airflow Webserver Health" "http://localhost:8080/health"
fi

# 6. Check JDBC Driver for NiFi
echo -e "\n${CYAN}7. Checking NiFi Supporting Drivers...${NC}"
JDBC_COUNT=$(find nifi/drivers -name "*.jar" 2>/dev/null | wc -l | tr -d ' ')
if [ "$JDBC_COUNT" -gt 0 ]; then
    report_pass "Found PostgreSQL JDBC driver in nifi/drivers/"
else
    report_warn "No JDBC driver JAR found in nifi/drivers/" "Download postgresql-42.7.3.jar before Phase 3: curl -o nifi/drivers/postgresql-42.7.3.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar"
fi

echo ""
echo -e "${BLUE}====================================================================${NC}"
echo -e "Doctor Summary: ${GREEN}$PASSED_CHECKS Passed${NC}, ${RED}$FAILED_CHECKS Failed${NC} (Total: $TOTAL_CHECKS checks)"
echo -e "${BLUE}====================================================================${NC}"

if [ "$FAILED_CHECKS" -eq 0 ]; then
    echo -e "${GREEN}✓ All inspected checks passed! Your infrastructure is healthy.${NC}"
    exit 0
else
    echo -e "${YELLOW}! Some checks failed or are waiting for containers to boot.${NC}"
    exit 1
fi
