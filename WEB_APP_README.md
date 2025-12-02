# Job Scraper Web Application

A full-stack web application for scraping and managing job postings from multiple career page platforms (Lever, Greenhouse, Getro).

## Architecture

The application consists of three main components:

1. **Python Scraper** (`job_scraper/`) - Scrapy-based job scraper
2. **Backend** (`backend/`) - Node.js/TypeScript GraphQL API with PostgreSQL
3. **Frontend** (`frontend/`) - React/TypeScript web interface

## Quick Start

### Prerequisites

- Node.js 18+
- Python 3.13+ with Poetry and pyenv
- PostgreSQL 14+

### 1. Set Up PostgreSQL

```bash
# Create database
createdb jobscraper
```

### 2. Set Up Python Scraper

```bash
# Install Python dependencies
poetry install

# Set up companies.csv with your companies to scrape
# Format: company_name,company_slug,domain,career_page_vendor
```

Create `companies.csv` in the project root:
```csv
company_name,company_slug,domain,career_page_vendor
Immuta,immuta,immuta.com,lever
Nomad Health,nomadhealth,nomadhealth.com,greenhouse
```

### 3. Set Up Backend

```bash
cd backend

# Install dependencies
npm install

# Configure environment
cp .env.example .env
# Edit .env with your PostgreSQL credentials

# Run database migrations
npm run prisma:generate
npm run prisma:migrate

# Start the backend
npm run dev
```

Backend runs at `http://localhost:4000/graphql`

### 4. Set Up Frontend

```bash
cd frontend

# Install dependencies
npm install

# Configure environment (optional)
cp .env.example .env

# Start the frontend
npm run dev
```

Frontend runs at `http://localhost:5173`

## Using the Application

1. **Open the web interface** at `http://localhost:5173`

2. **Click "Run Scraper"** to execute the Python scraper
   - The scraper will crawl all companies in `companies.csv`
   - Results are automatically imported into the database
   - New jobs get `firstScrapedAt` timestamp
   - Existing jobs get updated `lastScrapedAt` timestamp
   - Jobs no longer found get `archivedAt` timestamp

3. **View and manage jobs**:
   - Filter by active, archived, or ignored status
   - Search by company name
   - Click job titles to view original postings
   - Mark jobs as ignored to hide from active list

4. **Export data**:
   - Click "Export Active Jobs (CSV)" to download all non-archived, non-ignored jobs
   - CSV includes: title, company, location, type, source, timestamps

## Data Flow

```
┌─────────────────┐
│ Python Scraper  │
│ (Scrapy)        │
└────────┬────────┘
         │ Writes all_jobs.jsonl
         ▼
┌─────────────────┐
│ Backend Service │
│ (Node.js)       │
└────────┬────────┘
         │ Reads JSONL
         │ Updates PostgreSQL
         │ Tracks history
         ▼
┌─────────────────┐
│   PostgreSQL    │
│   Database      │
└────────┬────────┘
         │ GraphQL queries
         ▼
┌─────────────────┐
│ React Frontend  │
│ (Apollo Client) │
└─────────────────┘
```

## Job Tracking

Jobs are tracked across scraper runs:

- **firstScrapedAt**: Set when job is first discovered, never changes
- **lastScrapedAt**: Updated every time the job is found in a scrape
- **archivedAt**: Set when a job is no longer found (job posting removed)
- **ignored**: Manual flag to hide jobs you're not interested in

This allows you to:
- See when jobs were first posted
- Track how long jobs have been open
- Identify recently removed positions
- Maintain a clean active jobs list

## Database Schema

### Jobs Table
- Core fields: title, company, location, employment_type, workplace_type, etc.
- Tracking: firstScrapedAt, lastScrapedAt, archivedAt, ignored
- Unique identifier: url

### Companies Table
- Stores scraper configuration
- Fields: companyName, companySlug, domain, careerPageVendor

## Development

### Backend Development

```bash
cd backend
npm run dev              # Start with hot reload
npm run prisma:studio    # Open database GUI
```

### Frontend Development

```bash
cd frontend
npm run dev              # Start with hot reload
```

### Python Scraper Development

```bash
cd job_scraper
scrapy crawl lever_jobs -o jobs.json -L DEBUG --logfile debug.log
```

## Project Structure

```
jobscraper-main/
├── job_scraper/              # Python Scrapy project
│   ├── job_scraper/
│   │   ├── spiders/          # Scrapers for each platform
│   │   └── items.py
│   ├── job_scraper_controller.py
│   └── output/               # Scraper output files
│
├── backend/                  # Node.js GraphQL API
│   ├── prisma/
│   │   └── schema.prisma     # Database schema
│   └── src/
│       ├── schema/           # GraphQL types & resolvers
│       ├── services/         # Business logic
│       └── index.ts
│
├── frontend/                 # React web interface
│   └── src/
│       ├── apollo/           # GraphQL client
│       ├── components/       # React components
│       └── App.tsx
│
├── companies.csv             # Companies to scrape
└── CLAUDE.md                 # Architecture documentation
```

## Troubleshooting

### Scraper fails to run

- Ensure Poetry environment is activated
- Check `PYTHON_SCRAPER_PATH` in backend `.env`
- Verify `companies.csv` exists and has correct format

### Database connection fails

- Verify PostgreSQL is running: `pg_isready`
- Check `DATABASE_URL` in backend `.env`
- Ensure database exists: `psql -l | grep jobscraper`

### No jobs appearing after scrape

- Check backend logs for errors
- Verify `output/all_jobs.jsonl` exists and has data
- Open Prisma Studio to inspect database: `npm run prisma:studio`

### Frontend can't connect to backend

- Verify backend is running at `http://localhost:4000/graphql`
- Check browser console for CORS errors
- Verify `VITE_GRAPHQL_URL` in frontend `.env`

## Next Steps

- Add more spiders for additional job platforms
- Implement company management UI
- Add email notifications for new jobs
- Create job match scoring based on preferences
- Add authentication and multi-user support
