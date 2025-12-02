export interface Job {
  id: string;
  title: string;
  company: string;
  location?: string;
  employmentType?: string;
  workplaceType?: string;
  department?: string;
  url: string;
  description?: string;
  requirements?: string;
  source: string;
  firstScrapedAt: string;
  lastScrapedAt: string;
  archivedAt?: string;
  ignored?: string;
}

export interface Company {
  id: string;
  companyName: string;
  companySlug: string;
  domain: string;
  careerPageVendor: string;
}

export interface ScraperResult {
  success: boolean;
  message: string;
  totalCompanies: number;
  successfulScrapes: number;
  currentJobsCount: number;
  archivedJobsCount: number;
  newJobsCount: number;
}
