# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a full-stack web application for scraping and managing job postings from multiple career page platforms (Lever, Greenhouse, Getro).

**Architecture Components:**
1. **Python Scraper** (`job_scraper/`) - Scrapy-based crawlers for job platforms
2. **Backend** (`backend/`) - Node.js/TypeScript GraphQL API with PostgreSQL
3. **Frontend** (`frontend/`) - React/TypeScript web interface

The original Python scraper uses a controller pattern to orchestrate multiple scrapers, and the new backend integrates with it to provide persistent storage, historical tracking, and a web UI for managing jobs.

## Quick Start (Web Application)

### Prerequisites
- Node.js 18+, Python 3.13+ with Poetry, PostgreSQL 14+

### Setup and Run

1. **Database**: `createdb jobscraper`

2. **Backend**:
```bash
cd backend
npm install
cp .env.example .env  # Configure DATABASE_URL
npm run prisma:generate
npm run prisma:migrate
npm run dev  # Runs at http://localhost:4000/graphql
```

3. **Frontend**:
```bash
cd frontend
npm install
npm run dev  # Runs at http://localhost:5173
```

4. **Python Scraper Setup**:
```bash
poetry install
# Create companies.csv in project root
```

See `WEB_APP_README.md` for detailed setup instructions.

## Python Scraper Setup (Standalone)

**Package Management**: Uses Poetry and pyenv

```bash
# Install dependencies
poetry install

# Activate environment
cd job_scraper
eval $(poetry env activate)
```

**Python Version**: Requires Python ^3.13.2

## Running Commands

### Individual Spider Execution

Run a single spider for testing or debugging:

```bash
scrapy crawl lever_jobs -o jobs.json -L DEBUG --logfile ./debug.log
```

Available spiders:
- `lever_jobs` - Scrapes Lever.co career pages
- `greenhouse_jobs` - Scrapes Greenhouse.io job boards
- `getro_jobs` - Scrapes Getro job boards (can delegate to Lever/Greenhouse for full job details)

### Controller Execution

Run the full scraper controller to process multiple companies:

```bash
poetry run python job_scraper_controller.py
```

The controller requires two CSV files in the project root:
- `companies.csv` - Companies to scrape (columns: `company_name`, `company_slug`, `domain`, `career_page_vendor`)
- `ignore_urls.csv` - Job URLs to mark as ignored (column: `url`)

## Architecture

### Spider Architecture

All spiders inherit from `scrapy.Spider` and follow a two-phase parse pattern:

1. **Parse method**: Extracts job listing URLs from the main page
2. **Parse job details method**: Extracts structured data from individual job pages

Each spider accepts dynamic parameters via constructor:
- `company` - Company slug/identifier
- `domain` - Company domain for allowed_domains

### Controller Architecture (job_scraper_controller.py)

The `JobScraperController` class orchestrates all scraping operations with these key features:

**Job Tracking Across Runs**:
- Maintains `previous_jobs.jsonl` to track job history
- Adds `first_scraped_at` and `last_scraped_at` timestamps
- Identifies archived jobs (in previous run but not current)
- Marks ignored jobs based on `ignore_urls.csv`

**Spider Mapping**:
```python
spider_mapping = {
    'lever': 'lever_jobs',
    'greenhouse': 'greenhouse_jobs',
    'getro': 'getro_jobs',
}
```

**Output Files** (in `output/` directory):
- `all_jobs.jsonl` - Raw scraped data from current run
- `all_jobs.csv` - Processed CSV with all jobs (current + archived)
- `previous_jobs.jsonl` - Combined dataset for next run comparison
- `scraper.log` - Execution logs

**Processing Flow**:
1. Load previous jobs from `previous_jobs.jsonl`
2. Load companies from `companies.csv`
3. Normalize domains (remove www, protocols, paths)
4. Run each spider sequentially with delay
5. Load current scraped jobs
6. Add timestamps (preserve `first_scraped_at` for existing jobs)
7. Mark ignored jobs from `ignore_urls.csv`
8. Identify archived jobs (in previous but not current)
9. Combine current + archived jobs into CSV
10. Save combined dataset as new `previous_jobs.jsonl`

### Getro Spider Delegation Pattern

The `getro_scraper.py` implements a unique delegation pattern because Getro aggregates jobs from multiple platforms:

1. Scrapes Getro page for basic info
2. Detects the apply URL's platform (Greenhouse, Lever, etc.)
3. Delegates to the appropriate spider's `parse_job_details()` method
4. Merges Getro metadata with detailed platform data
5. Creates combined company names (e.g., `"4pt0 / secondarycompany"`)

When adding new platform support to Getro, follow the checklist at the top of `getro_scraper.py:10-17`.

### JobItem Structure

All spiders yield `JobItem` objects with these fields:
- `title`, `employment_type`, `workplace_type`, `location`, `department`
- `url` (unique identifier for tracking across runs)
- `description`, `requirements`
- `company`, `source` (platform identifier)
- `scraped_at` (timestamp, replaced by controller with `first_scraped_at`/`last_scraped_at`)

Note: Greenhouse doesn't consistently expose `employment_type`, `workplace_type`, `department`, or `requirements` fields.

## Development Notes

**Domain Normalization**: The controller normalizes domains to clean hostnames (e.g., `"https://www.406ventures.com/"` → `"406ventures.com"`). See `job_scraper_controller.py:44-84`.

**Error Handling**: The controller treats failure to load `previous_jobs.jsonl` as a fatal error (exits with SystemExit). This prevents data inconsistencies from corrupted history files.

**Respectful Scraping**:
- `ROBOTSTXT_OBEY = True` in settings
- Configurable delay between companies (default 1 second)
- All Scrapy throttling settings available in `job_scraper/job_scraper/settings.py`

**Logging**: Comprehensive logging at INFO and DEBUG levels. Individual spider logs can be enabled by uncommenting line 266 in `job_scraper_controller.py`.

## Web Application Architecture

### Backend (Node.js/TypeScript/GraphQL)

**Tech Stack**: Apollo Server, Prisma ORM, PostgreSQL, Express

**Key Files:**
- `backend/prisma/schema.prisma` - Database schema (Job, Company models)
- `backend/src/schema/typeDefs.ts` - GraphQL schema
- `backend/src/schema/resolvers.ts` - GraphQL resolvers
- `backend/src/services/scraperService.ts` - Python scraper integration
- `backend/src/index.ts` - Server entry point

**Database Schema:**
- **Job**: Stores all job data with tracking fields (`firstScrapedAt`, `lastScrapedAt`, `archivedAt`, `ignored`)
- **Company**: Stores scraper configuration for each company

**GraphQL API:**

Key queries:
- `jobs(includeArchived, includeIgnored, company)` - Fetch jobs with filters
- `activeJobsCount`, `archivedJobsCount`, `ignoredJobsCount` - Statistics

Key mutations:
- `runScraper` - Execute Python scraper, process results into database
- `toggleIgnoreJob(jobId, ignore)` - Mark job as ignored (stores 'checked' string, not boolean)
- `exportActiveJobsCSV` - Generate CSV of active jobs

**Scraper Integration Flow:**
1. `runScraper` mutation executes `poetry run python job_scraper_controller.py`
2. Reads `job_scraper/output/all_jobs.jsonl`
3. For each job:
   - If new: Create with `firstScrapedAt = now`
   - If existing: Update with `lastScrapedAt = now`, clear `archivedAt`
4. Jobs in DB but not in scrape output: Set `archivedAt = now`
5. Preserves manual `ignored` flags across runs

**Important**: The `ignored` field stores the string `'checked'` (not a boolean) to match the Python scraper's behavior. The GraphQL mutation accepts a boolean for convenience but maps it to `'checked'` or `null`.

### Frontend (React/TypeScript)

**Tech Stack**: Vite, Apollo Client, React 18, date-fns

**Key Files:**
- `frontend/src/apollo/client.ts` - Apollo Client configuration
- `frontend/src/apollo/queries.ts` - GraphQL queries and mutations
- `frontend/src/components/JobsList.tsx` - Jobs table with ignore functionality
- `frontend/src/components/ScraperControls.tsx` - Run scraper, export CSV, filters
- `frontend/src/App.tsx` - Main app with stats dashboard

**Features:**
- Real-time updates (polls every 5 seconds)
- Filter jobs by status (active/archived/ignored) and company
- Click-to-run scraper with status feedback
- One-click CSV export
- Inline job ignore/unignore

**Data Flow:**
```
User clicks "Run Scraper"
  → GraphQL mutation `runScraper`
  → Backend executes Python scraper
  → Backend processes JSONL output
  → Backend updates PostgreSQL
  → Frontend refetches queries
  → UI updates with new data
```

## Historical Tracking

The web application maintains full job history in PostgreSQL (unlike the Python-only version which used `previous_jobs.jsonl`):

- **firstScrapedAt**: When job first appeared, never changes
- **lastScrapedAt**: Updated every scrape where job is found
- **archivedAt**: Set when job no longer found, can be cleared if job reappears
- **ignored**: Manual flag, preserved across all scraper runs

This enables:
- Track how long jobs have been open
- See when positions were removed
- Identify jobs that come and go
- Build custom filters and views

## Testing

Dev dependencies include pytest, black, and isort. No tests currently exist in the repository.
