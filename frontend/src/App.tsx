import { useState } from 'react';
import { useQuery } from '@apollo/client';
import { GET_JOBS, GET_JOB_COUNTS } from './apollo/queries';
import { ScraperControls } from './components/ScraperControls';
import { JobsList } from './components/JobsList';
import { Job } from './types';
import './App.css';

function App() {
  const [filters, setFilters] = useState({
    includeArchived: false,
    includeIgnored: false,
    company: '',
  });

  const { data: jobsData, loading: jobsLoading } = useQuery(GET_JOBS, {
    variables: filters,
    pollInterval: 5000, // Poll every 5 seconds for updates
  });

  const { data: countsData } = useQuery(GET_JOB_COUNTS, {
    pollInterval: 5000,
  });

  const jobs: Job[] = jobsData?.jobs || [];

  return (
    <div className="app">
      <header>
        <h1>Job Scraper Dashboard</h1>
        <div className="stats">
          <div className="stat">
            <span className="stat-label">Active Jobs</span>
            <span className="stat-value">
              {countsData?.activeJobsCount || 0}
            </span>
          </div>
          <div className="stat">
            <span className="stat-label">Archived Jobs</span>
            <span className="stat-value">
              {countsData?.archivedJobsCount || 0}
            </span>
          </div>
          <div className="stat">
            <span className="stat-label">Ignored Jobs</span>
            <span className="stat-value">
              {countsData?.ignoredJobsCount || 0}
            </span>
          </div>
        </div>
      </header>

      <ScraperControls onFiltersChange={setFilters} />

      <JobsList jobs={jobs} loading={jobsLoading} />
    </div>
  );
}

export default App;
