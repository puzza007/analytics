# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Plausible Analytics is an open-source, privacy-focused web analytics application that provides an alternative to Google Analytics. It's a full-stack Elixir/Phoenix application with React dashboard components and a lightweight JavaScript tracker.

## Key Technologies

- **Backend**: Elixir 1.18.3 with Phoenix Framework 1.7.0
- **Databases**: PostgreSQL (application data) + ClickHouse (time-series analytics data)
- **Frontend**: React 18.3.1, TypeScript, TailwindCSS, AlpineJS
- **Testing**: ExUnit (Elixir), Jest (JavaScript), Playwright (E2E)
- **Build Tools**: Mix (Elixir), npm, esbuild, Docker

## Essential Commands

### Development Setup
```bash
make postgres && make clickhouse  # Start databases
make install                      # Install all dependencies and setup
make server                       # Start Phoenix server on localhost:8000
```

### Running Tests
```bash
# Elixir tests
mix test                                    # Basic test run
mix test test/path/to/test.exs             # Single file
mix test test/path/to/test.exs:42          # Single test by line number

# JavaScript tests
npm run test --prefix assets                # Frontend tests
npm run test --prefix tracker               # Tracker tests
```

### Code Quality
```bash
# Elixir
mix format                                  # Format code
mix format --check-formatted                # Check formatting
mix credo                                   # Static analysis
mix dialyzer                                # Type checking

# JavaScript/TypeScript
npm run lint --prefix assets                # Run all linters
npm run format --prefix assets              # Format code
npm run typecheck --prefix assets           # TypeScript checking
```

### Building Assets
```bash
mix assets.build                            # Development build
mix assets.deploy                           # Production build
npm run deploy --prefix tracker             # Build tracker variants
```

## Architecture Overview

### Directory Structure
- `lib/plausible/` - Core business logic and domain models
- `lib/plausible_web/` - Phoenix web layer (controllers, views, LiveView)
- `assets/js/` - React dashboard components and TypeScript code
- `tracker/src/` - JavaScript tracker source code
- `priv/repo/migrations/` - PostgreSQL migrations
- `priv/ingest_repo/migrations/` - ClickHouse migrations
- `extra/` - Enterprise edition features

### Data Flow
1. JavaScript tracker (`tracker/`) collects pageviews/events from websites
2. Events are ingested via Phoenix controllers into ClickHouse
3. Dashboard queries ClickHouse for real-time analytics
4. PostgreSQL stores user accounts, sites configuration, and application data
5. Oban handles background jobs (imports, exports, email reports)

### Key Architectural Patterns
- **Multi-tenancy**: Each site has isolated analytics data
- **Event-driven**: Analytics events are processed asynchronously
- **Real-time**: LiveView provides real-time dashboard updates
- **Privacy-first**: No cookies, no personal data collection by default

### Testing Strategy
- Unit tests for business logic in `lib/plausible/`
- Integration tests for database operations
- Controller tests for API endpoints
- LiveView tests for real-time features
- E2E tests for critical user flows using Playwright

### Common Development Tasks

**Adding a new stats query**:
1. Implement query logic in `lib/plausible/stats/`
2. Add controller action in `lib/plausible_web/controllers/api/stats_controller.ex`
3. Update React components in `assets/js/dashboard/`

**Modifying the tracker**:
1. Edit source in `tracker/src/`
2. Run `npm run deploy --prefix tracker` to rebuild variants
3. Test with `npm run test --prefix tracker`
4. Update `tracker/CHANGELOG.md` for releases

**Database migrations**:
- PostgreSQL: `mix ecto.gen.migration name` then `mix ecto.migrate`
- ClickHouse: Create in `priv/ingest_repo/migrations/` then `mix ecto.migrate -r Plausible.IngestRepo`

### Important Notes
- Always check existing patterns in similar files before implementing new features
- The tracker is highly optimized for size - avoid adding dependencies
- Enterprise features (`extra/`) require additional setup
- Use test account `user@plausible.test` / `plausible` for local development
- ClickHouse queries should be optimized for large datasets
- Consider privacy implications for any new data collection