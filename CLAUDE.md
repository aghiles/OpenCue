# CLAUDE.md - OpenCue Development Guide

This document provides comprehensive guidance for AI assistants working with the OpenCue codebase.

## Project Overview

OpenCue is an open-source render management system used in visual effects and animation production. Developed by Sony Pictures Imageworks, it breaks down complex rendering jobs into individual tasks and allocates computational resources through a configurable dispatch queue.

**Key Resources:**
- Website: https://www.opencue.io
- Documentation: https://www.opencue.io/docs/
- Slack: https://academysoftwarefdn.slack.com/archives/CMFPXV39Q

## Repository Structure

```
OpenCue/
├── cuebot/          # Java/Gradle - Central management server
├── rqd/             # Python - Render Queue Daemon (runs on render hosts)
├── pycue/           # Python - OpenCue Python API/SDK
├── cuegui/          # Python/Qt - Desktop monitoring GUI
├── cuesubmit/       # Python/Qt - Job submission GUI
├── pyoutline/       # Python - Job construction library
├── cueadmin/        # Python/CLI - Command-line admin tool
├── cueweb/          # Node.js/React - Web-based interface
├── rest_gateway/    # Go - REST/gRPC bridge
├── proto/           # Protocol Buffers - API definitions
├── sandbox/         # Local development environment
├── ci/              # CI/CD scripts
├── connectors/      # Prometheus metrics exporter
├── samples/         # Example code and Docker samples
├── api_docs/        # Sphinx documentation
└── tsc/             # Technical Steering Committee notes
```

## Component Details

### Cuebot (Java)
- **Purpose:** Central server managing jobs, dispatching work to RQD nodes
- **Build:** Gradle 7.6.4 with Spring Boot 2.2.1
- **Java Version:** 11+
- **Database:** PostgreSQL with Flyway migrations
- **Ports:** 8443 (gRPC for clients), 8444 (gRPC for RQD)
- **Entry point:** `com.imageworks.spcue.CuebotApplication`

### RQD (Python)
- **Purpose:** Daemon running on render hosts, executes work from Cuebot
- **Entry point:** `rqd.__main__:main`
- **Key modules:** `rqcore.py`, `rqmachine.py`, `rqnetwork.py`, `rqdservicers.py`

### PyCue (Python)
- **Purpose:** Python API for interacting with Cuebot
- **Packages:** `opencue`, `FileSequence`
- **Core files:** `api.py`, `wrappers/`, `search.py`, `cuebot.py`

### CueGUI (Python/Qt)
- **Purpose:** Desktop GUI for job monitoring/management
- **Entry points:** `cuegui`, `cuetopia`, `cuecommander`
- **UI Framework:** PySide6/PySide2 via QtPy

### CueSubmit (Python/Qt)
- **Purpose:** Job submission GUI
- **Entry point:** `cuesubmit`

### PyOutline (Python)
- **Purpose:** Programmatic job construction
- **Key modules:** `outline/`, `modules/shell.py`, `cuerun.py`

### CueAdmin (Python/CLI)
- **Purpose:** Command-line administration
- **Entry point:** `cueadmin`

### CueWeb (Node.js/React)
- **Purpose:** Web-based job management interface
- **Framework:** Next.js 14, React 18, TypeScript, Tailwind CSS
- **Port:** 3000
- **Auth:** NextAuth.js (Google, GitHub, Okta)

### REST Gateway (Go)
- **Purpose:** HTTP REST to gRPC bridge for Cuebot
- **Auth:** JWT

## Build Commands

### Building All Python Packages
```bash
pip install build
python -m build ./proto
python -m build ./pycue
python -m build ./pyoutline
python -m build ./cueadmin
python -m build ./cuesubmit
python -m build ./rqd
python -m build ./cuegui
```

### Cuebot (Java)
```bash
cd cuebot
./gradlew build              # Build and test
./gradlew bootJar            # Create executable JAR
./gradlew spotlessApply      # Format code
```

### CueWeb (Node.js)
```bash
cd cueweb
npm install
npm run dev      # Development server
npm run build    # Production build
npm test         # Run tests
```

## Testing

### Python Tests
```bash
# Install test dependencies
pip install ./pycue[test]

# Run tests for each component
python -m pytest pycue
python -m pytest pyoutline
python -m pytest cueadmin
python -m pytest cuesubmit
python -m pytest rqd

# Or use the CI script
ci/run_python_tests.sh
```

### Python Linting
```bash
# Uses pylint with custom configs
python -m pylint --rcfile=ci/pylintrc_main <module>
python -m pylint --rcfile=ci/pylintrc_test tests

# Or use the CI script
ci/run_python_lint.sh
```

### Cuebot Tests (Java)
```bash
cd cuebot
./gradlew test
./gradlew jacocoTestReport   # Generate coverage
```

### CueWeb Tests
```bash
cd cueweb
npm test
npm run coverage
```

### Integration Tests
```bash
ci/run_integration_test.sh
```

## Local Development (Sandbox)

### Start Services with Docker
```bash
# Create required directories
mkdir -p /tmp/rqd/logs /tmp/rqd/shots

# Build Cuebot image
docker build -t opencue/cuebot -f cuebot/Dockerfile .

# Start all services
docker compose up

# Stop services
docker compose down
```

### Install Client Libraries (Development)
```bash
python3 -m venv sandbox-venv
source sandbox-venv/bin/activate
./sandbox/install-client-sources.sh
```

### Test Installation
```bash
cueadmin -lh    # List hosts
cuegui &        # Launch GUI
cuesubmit &     # Launch submission tool
```

## Code Style & Conventions

### Python
- **Style:** PEP 8 with pylint enforcement
- **Naming:** snake_case for functions/variables, CamelCase for classes
- **Linting:** pylint 2.15.10 with configs in `ci/pylintrc_main` and `ci/pylintrc_test`
- **Testing:** pytest with mock and pyfakefs
- **Versioning:** Git-based via versioningit

### Java (Cuebot)
- **Style:** Eclipse formatter config in `cuebot/jdtls.xml`
- **Formatting:** Spotless plugin (`./gradlew spotlessApply`)
- **Compiler:** Warnings treated as errors (`-Xlint:all -Werror`)
- **Testing:** JUnit 4 with Spring test context

### JavaScript/TypeScript (CueWeb)
- **Style:** ESLint + Prettier
- **Testing:** Jest with @testing-library/react

## Architecture Patterns

### gRPC Communication
- All inter-component communication uses gRPC defined in `proto/src/`
- Proto files define services for Jobs, Hosts, Shows, Frames, Layers, etc.
- Python clients use generated stubs via `grpcio`
- Java Cuebot uses generated Java stubs

### Database (Cuebot)
- PostgreSQL with Flyway migrations
- Migrations in: `cuebot/src/main/resources/conf/ddl/postgres/migrations/`
- Migration format: `V{version}__Description.sql`
- DAO pattern with Spring JDBC

### Layered Architecture (Cuebot)
1. **Servlet/gRPC Layer** - Request handling
2. **Service Layer** - Business logic (Dispatcher, Scheduler)
3. **DAO Layer** - Database access
4. **Entity Layer** - Domain objects

## Key Files

### Configuration
- `cuebot/src/main/resources/opencue.properties` - Cuebot main config
- `cuebot/src/main/resources/application.properties` - Spring Boot config
- `cuebot/src/main/resources/log4j2.properties` - Logging config
- `rqd.conf` - RQD configuration (created at runtime)

### Build Configuration
- `cuebot/build.gradle` - Java build config
- `*/pyproject.toml` - Python package configs
- `cueweb/package.json` - Node.js dependencies
- `docker-compose.yml` - Local development orchestration

### CI/CD
- `.github/workflows/testing-pipeline.yml` - Main test pipeline
- `ci/run_python_tests.sh` - Python test runner
- `ci/run_python_lint.sh` - Python linting
- `ci/check_database_migrations.py` - Migration validation

## Dependencies

### Python Components
- Python 3.7+ (tested with 3.7, 3.11, 3.12)
- grpcio ~1.71.0
- psutil 5.9.8 (RQD)
- PySide6/PySide2 (GUI components)
- PyYAML 6.0.1
- pytest 8.3.3

### Java/Cuebot
- Java 11+
- Spring Boot 2.2.1
- gRPC 1.47.0
- Protobuf 3.21.2
- PostgreSQL 15+

### CueWeb
- Node.js (latest LTS)
- Next.js 14
- React 18
- TypeScript

## Environment Variables

### Cuebot
- `CUE_FRAME_LOG_DIR` - Directory for frame logs

### RQD
- `CUEBOT_HOSTNAME` - Cuebot server hostname

### CueWeb
- `NEXT_PUBLIC_OPENCUE_ENDPOINT` - REST Gateway URL
- `NEXT_PUBLIC_URL` - CueWeb application URL
- `NEXT_JWT_SECRET` - JWT secret for auth

## Common Development Tasks

### Adding a New Database Migration
1. Create file: `cuebot/src/main/resources/conf/ddl/postgres/migrations/V{next_version}__Description.sql`
2. Run `ci/check_database_migrations.py` to validate

### Updating Proto Definitions
1. Edit files in `proto/src/`
2. Rebuild proto package: `python -m build ./proto`
3. Reinstall dependent packages

### Adding Python Tests
- Place tests in `tests/` directory of each component
- Name files `test_*.py` or `*_test.py`
- Use `mock` and `pyfakefs` for mocking

## Git Workflow

### Branch Strategy
- Main branch: `master`
- Feature branches for development
- PRs require tests to pass

### CI Checks (GitHub Actions)
- Build all Python packages
- Test with Python 3.7, 3.11, 3.12
- Build and test Cuebot (CY2023, CY2024)
- Integration tests with Docker
- Pylint code quality
- Sphinx documentation build
- Database migration validation

## Troubleshooting

### Proto Compilation Issues
Proto files are compiled at build time. If you see import errors:
```bash
pip uninstall opencue_proto
python -m build ./proto
pip install ./proto/dist/opencue_proto-*.whl
```

### Database Connection (Sandbox)
Ensure PostgreSQL container is running:
```bash
docker compose ps
docker compose logs db
```

### GUI Display Issues
Ensure Qt bindings are installed:
```bash
pip install PySide6  # or PySide2
pip install QtPy
```

## Files to Ignore

The `.gitignore` excludes:
- `__pycache__/`, `*.pyc` - Python bytecode
- `venv*/`, `.venv/` - Virtual environments
- `build/`, `*.egg-info/` - Build artifacts
- `*/compiled_proto/` - Generated proto code
- `.coverage`, `htmlcov/` - Coverage reports
- IDE configs (`.idea/`, `.vscode/`)
