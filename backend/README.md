# Job Scraper Backend

GraphQL backend for the Job Scraper application.

## Prerequisites

- Node.js 18+ and npm
- PostgreSQL 14+
- Python 3.13+ with Poetry (for running the Python scraper)

## Setup

1. **Install dependencies:**

```bash
npm install
```

2. **Set up PostgreSQL:**

```bash
# Create a PostgreSQL database
createdb jobscraper

# Or using psql
psql -U postgres -c "CREATE DATABASE jobscraper;"
```

3. **Configure environment variables:**

```bash
cp .env.example .env
```

Edit `.env` and update with your database credentials:

```
DATABASE_URL="postgresql://username:password@localhost:5432/jobscraper?schema=public"
PORT=4000
PYTHON_SCRAPER_PATH="../job_scraper"
```

4. **Run Prisma migrations:**

```bash
npm run prisma:generate
npm run prisma:migrate
```

This will create the database tables and generate the Prisma client.

## Running the Server

**Development mode (with hot reload):**

```bash
npm run dev
```

**Production mode:**

```bash
npm run build
npm start
```

The GraphQL server will be available at `http://localhost:4000/graphql`

## Database Management

**Open Prisma Studio (database GUI):**

```bash
npm run prisma:studio
```

**Create a new migration:**

```bash
npm run prisma:migrate
```

**Reset database (WARNING: deletes all data):**

```bash
npm run prisma:reset
```

## GraphQL API

Once the server is running, you can explore the API at `http://localhost:4000/graphql` using the Apollo Studio interface.

### Key Queries

- `jobs` - Get all jobs with optional filters
- `companies` - Get all companies
- `activeJobsCount`, `archivedJobsCount`, `ignoredJobsCount` - Get job statistics

### Key Mutations

- `runScraper` - Execute the Python scraper
- `toggleIgnoreJob` - Mark a job as ignored or unignored
- `exportActiveJobsCSV` - Export active jobs as CSV
- `addCompany`, `updateCompany`, `deleteCompany` - Manage companies

## How It Works

The backend orchestrates the Python scraper and manages job data:

1. **Running Scraper**: Executes `poetry run python job_scraper_controller.py` in the Python scraper directory
2. **Processing Results**: Reads the `output/all_jobs.jsonl` file produced by the scraper
3. **Database Updates**:
   - Creates new job records with `firstScrapedAt` timestamp
   - Updates existing jobs with new `lastScrapedAt` timestamp
   - Archives jobs that were scraped previously but not in current run
   - Preserves manual ignore flags
4. **CSV Export**: Generates CSV from active (non-archived, non-ignored) jobs

## Project Structure

```
backend/
├── prisma/
│   └── schema.prisma          # Database schema
├── src/
│   ├── schema/
│   │   ├── typeDefs.ts        # GraphQL schema
│   │   └── resolvers.ts       # GraphQL resolvers
│   ├── services/
│   │   └── scraperService.ts  # Python scraper integration
│   └── index.ts               # Server entry point
├── package.json
└── tsconfig.json
```
