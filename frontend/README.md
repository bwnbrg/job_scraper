# Job Scraper Frontend

React frontend for the Job Scraper application.

## Prerequisites

- Node.js 18+ and npm
- Backend server running at `http://localhost:4000/graphql`

## Setup

1. **Install dependencies:**

```bash
npm install
```

2. **Configure environment variables:**

```bash
cp .env.example .env
```

The default configuration points to `http://localhost:4000/graphql`. Update if your backend is running elsewhere.

## Running the Application

**Development mode:**

```bash
npm run dev
```

The application will be available at `http://localhost:5173`

**Production build:**

```bash
npm run build
npm run preview
```

## Features

### Dashboard

- **Job Statistics**: View counts of active, archived, and ignored jobs
- **Real-time Updates**: Dashboard polls for updates every 5 seconds

### Scraper Controls

- **Run Scraper**: Click button to execute the Python scraper
- **Export CSV**: Download all active jobs as a CSV file
- **Filters**:
  - Show/hide archived jobs
  - Show/hide ignored jobs
  - Filter by company name

### Jobs Table

- View all jobs with title, company, location, type, source
- See job status (Active, Archived, Ignored)
- See when each job was last scraped
- Click job titles to open original posting
- Ignore/unignore individual jobs

## Tech Stack

- **React 18** - UI framework
- **TypeScript** - Type safety
- **Apollo Client** - GraphQL client
- **Vite** - Build tool and dev server
- **date-fns** - Date formatting

## Project Structure

```
frontend/
├── src/
│   ├── apollo/
│   │   ├── client.ts       # Apollo Client setup
│   │   └── queries.ts      # GraphQL queries/mutations
│   ├── components/
│   │   ├── JobsList.tsx    # Jobs table component
│   │   └── ScraperControls.tsx  # Controls component
│   ├── types/
│   │   └── index.ts        # TypeScript type definitions
│   ├── App.tsx             # Main application component
│   ├── App.css             # Global styles
│   └── main.tsx            # Application entry point
├── index.html
├── package.json
├── tsconfig.json
└── vite.config.ts
```

## Development

The application uses Apollo Client's cache-and-network fetch policy to balance performance with data freshness. Jobs data is automatically refetched after mutations to keep the UI in sync.
