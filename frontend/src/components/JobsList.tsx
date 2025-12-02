import { useMutation } from '@apollo/client';
import { formatDistanceToNow } from 'date-fns';
import { TOGGLE_IGNORE_JOB, GET_JOBS } from '../apollo/queries';
import { Job } from '../types';

interface JobsListProps {
  jobs: Job[];
  loading: boolean;
}

export function JobsList({ jobs, loading }: JobsListProps) {
  const [toggleIgnoreJob] = useMutation(TOGGLE_IGNORE_JOB, {
    refetchQueries: [{ query: GET_JOBS }],
  });

  const handleToggleIgnore = async (jobId: string, currentlyIgnored: boolean) => {
    try {
      await toggleIgnoreJob({
        variables: {
          jobId,
          ignore: !currentlyIgnored,
        },
      });
    } catch (error) {
      console.error('Failed to toggle ignore:', error);
    }
  };

  if (loading) {
    return <div className="loading">Loading jobs...</div>;
  }

  if (jobs.length === 0) {
    return <div className="empty">No jobs found. Try running the scraper!</div>;
  }

  return (
    <div className="jobs-container">
      <table className="jobs-table">
        <thead>
          <tr>
            <th>Status</th>
            <th>Title</th>
            <th>Company</th>
            <th>Location</th>
            <th>Type</th>
            <th>Source</th>
            <th>Last Scraped</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>
          {jobs.map((job) => {
            const isIgnored = !!job.ignored;
            const isArchived = !!job.archivedAt;

            return (
              <tr key={job.id}>
                <td>
                  {isArchived && <span className="badge badge-archived">Archived</span>}
                  {isIgnored && !isArchived && (
                    <span className="badge badge-ignored">Ignored</span>
                  )}
                  {!isArchived && !isIgnored && (
                    <span className="badge badge-active">Active</span>
                  )}
                </td>
                <td>
                  <a
                    href={job.url}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="job-url"
                  >
                    {job.title}
                  </a>
                </td>
                <td>{job.company}</td>
                <td>{job.location || '-'}</td>
                <td>
                  {job.employmentType || '-'}
                  {job.workplaceType && ` (${job.workplaceType})`}
                </td>
                <td>{job.source}</td>
                <td>
                  {formatDistanceToNow(new Date(job.lastScrapedAt), {
                    addSuffix: true,
                  })}
                </td>
                <td>
                  <div className="job-actions">
                    {!isArchived && (
                      <button
                        className={`btn-small ${
                          isIgnored ? 'btn-success' : 'btn-secondary'
                        }`}
                        onClick={() => handleToggleIgnore(job.id, isIgnored)}
                      >
                        {isIgnored ? 'Unignore' : 'Ignore'}
                      </button>
                    )}
                  </div>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
