import { gql } from '@apollo/client';

export const GET_JOBS = gql`
  query GetJobs($includeArchived: Boolean, $includeIgnored: Boolean, $company: String) {
    jobs(includeArchived: $includeArchived, includeIgnored: $includeIgnored, company: $company) {
      id
      title
      company
      location
      employmentType
      workplaceType
      department
      url
      source
      firstScrapedAt
      lastScrapedAt
      archivedAt
      ignored
    }
  }
`;

export const GET_JOB_COUNTS = gql`
  query GetJobCounts {
    activeJobsCount
    archivedJobsCount
    ignoredJobsCount
  }
`;

export const RUN_SCRAPER = gql`
  mutation RunScraper {
    runScraper {
      success
      message
      totalCompanies
      successfulScrapes
      currentJobsCount
      archivedJobsCount
      newJobsCount
    }
  }
`;

export const TOGGLE_IGNORE_JOB = gql`
  mutation ToggleIgnoreJob($jobId: ID!, $ignore: Boolean!) {
    toggleIgnoreJob(jobId: $jobId, ignore: $ignore) {
      id
      ignored
    }
  }
`;

export const EXPORT_JOBS_CSV = gql`
  mutation ExportActiveJobsCSV {
    exportActiveJobsCSV
  }
`;

export const GET_COMPANIES = gql`
  query GetCompanies {
    companies {
      id
      companyName
      companySlug
      domain
      careerPageVendor
    }
  }
`;
