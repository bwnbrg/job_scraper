import { useState } from 'react';
import { useMutation } from '@apollo/client';
import { RUN_SCRAPER, EXPORT_JOBS_CSV, GET_JOBS, GET_JOB_COUNTS } from '../apollo/queries';
import { ScraperResult } from '../types';

interface ScraperControlsProps {
  onFiltersChange: (filters: {
    includeArchived: boolean;
    includeIgnored: boolean;
    company: string;
  }) => void;
}

export function ScraperControls({ onFiltersChange }: ScraperControlsProps) {
  const [includeArchived, setIncludeArchived] = useState(false);
  const [includeIgnored, setIncludeIgnored] = useState(false);
  const [companyFilter, setCompanyFilter] = useState('');
  const [scraperStatus, setScraperStatus] = useState<{
    type: 'success' | 'error' | 'info' | null;
    message: string;
  }>({ type: null, message: '' });

  const [runScraper, { loading: scraperLoading }] = useMutation(RUN_SCRAPER, {
    refetchQueries: [{ query: GET_JOBS }, { query: GET_JOB_COUNTS }],
  });

  const [exportCSV, { loading: exportLoading }] = useMutation(EXPORT_JOBS_CSV);

  const handleRunScraper = async () => {
    setScraperStatus({ type: 'info', message: 'Starting scraper...' });
    try {
      const result = await runScraper();
      const data: ScraperResult = result.data.runScraper;

      if (data.success) {
        setScraperStatus({
          type: 'success',
          message: `Scraper completed! ${data.newJobsCount} new jobs, ${data.currentJobsCount} total jobs, ${data.archivedJobsCount} archived`,
        });
      } else {
        setScraperStatus({
          type: 'error',
          message: `Scraper failed: ${data.message}`,
        });
      }
    } catch (error) {
      setScraperStatus({
        type: 'error',
        message: `Error: ${error instanceof Error ? error.message : 'Unknown error'}`,
      });
    }
  };

  const handleExportCSV = async () => {
    try {
      const result = await exportCSV();
      const csvData = result.data.exportActiveJobsCSV;

      // Create blob and download
      const blob = new Blob([csvData], { type: 'text/csv' });
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `active_jobs_${new Date().toISOString().split('T')[0]}.csv`;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      window.URL.revokeObjectURL(url);

      setScraperStatus({
        type: 'success',
        message: 'CSV exported successfully!',
      });
    } catch (error) {
      setScraperStatus({
        type: 'error',
        message: `Export failed: ${error instanceof Error ? error.message : 'Unknown error'}`,
      });
    }
  };

  const handleFilterChange = (key: string, value: boolean | string) => {
    const newFilters = {
      includeArchived,
      includeIgnored,
      company: companyFilter,
      [key]: value,
    };

    if (key === 'includeArchived') setIncludeArchived(value as boolean);
    if (key === 'includeIgnored') setIncludeIgnored(value as boolean);
    if (key === 'company') setCompanyFilter(value as string);

    onFiltersChange(newFilters);
  };

  return (
    <>
      {scraperStatus.type && (
        <div className={`status-message status-${scraperStatus.type}`}>
          {scraperStatus.message}
        </div>
      )}

      <div className="controls">
        <div className="controls-row">
          <button
            className="btn-primary"
            onClick={handleRunScraper}
            disabled={scraperLoading}
          >
            {scraperLoading ? 'Running Scraper...' : 'Run Scraper'}
          </button>

          <button
            className="btn-success"
            onClick={handleExportCSV}
            disabled={exportLoading}
          >
            {exportLoading ? 'Exporting...' : 'Export Active Jobs (CSV)'}
          </button>

          <div className="filters">
            <label className="filter-label">
              <input
                type="checkbox"
                checked={includeArchived}
                onChange={(e) => handleFilterChange('includeArchived', e.target.checked)}
              />
              Show Archived
            </label>

            <label className="filter-label">
              <input
                type="checkbox"
                checked={includeIgnored}
                onChange={(e) => handleFilterChange('includeIgnored', e.target.checked)}
              />
              Show Ignored
            </label>

            <input
              type="text"
              className="search-input"
              placeholder="Filter by company..."
              value={companyFilter}
              onChange={(e) => handleFilterChange('company', e.target.value)}
            />
          </div>
        </div>
      </div>
    </>
  );
}
