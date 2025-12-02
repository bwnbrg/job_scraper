import { exec } from 'child_process';
import { promisify } from 'util';
import { PrismaClient } from '@prisma/client';
import * as fs from 'fs/promises';
import * as path from 'path';

const execAsync = promisify(exec);

interface ScrapedJob {
  title: string;
  employment_type?: string;
  workplace_type?: string;
  location?: string;
  department?: string;
  url: string;
  description?: string;
  requirements?: string;
  company: string;
  source: string;
  scraped_at: string;
}

export class ScraperService {
  constructor(private prisma: PrismaClient) {}

  async runScraper() {
    const scraperPath = process.env.PYTHON_SCRAPER_PATH || '../job_scraper';
    const outputPath = path.join(scraperPath, 'output', 'all_jobs.jsonl');

    console.log('Starting scraper...');

    try {
      // Execute the Python scraper
      const { stdout, stderr } = await execAsync(
        'poetry run python job_scraper_controller.py',
        {
          cwd: scraperPath,
          maxBuffer: 10 * 1024 * 1024, // 10MB buffer
        }
      );

      console.log('Scraper output:', stdout);
      if (stderr) console.error('Scraper errors:', stderr);

      // Read the scraped jobs from the output file
      const fileContent = await fs.readFile(outputPath, 'utf-8');
      const lines = fileContent.trim().split('\n');
      const scrapedJobs: ScrapedJob[] = lines.map((line) => JSON.parse(line));

      console.log(`Processing ${scrapedJobs.length} scraped jobs...`);

      // Get all existing jobs from database
      const existingJobs = await this.prisma.job.findMany({
        where: {
          archivedAt: null, // Only get active jobs
        },
      });

      const existingJobUrls = new Set(existingJobs.map((job) => job.url));
      const scrapedJobUrls = new Set(scrapedJobs.map((job) => job.url));

      const now = new Date();
      let newJobsCount = 0;
      let updatedJobsCount = 0;
      let archivedJobsCount = 0;

      // Process each scraped job
      for (const job of scrapedJobs) {
        const existingJob = await this.prisma.job.findUnique({
          where: { url: job.url },
        });

        if (existingJob) {
          // Update existing job
          await this.prisma.job.update({
            where: { url: job.url },
            data: {
              title: job.title,
              employmentType: job.employment_type,
              workplaceType: job.workplace_type,
              location: job.location,
              department: job.department,
              description: job.description,
              requirements: job.requirements,
              company: job.company,
              source: job.source,
              lastScrapedAt: now,
              // Unarchive if it was previously archived
              archivedAt: null,
            },
          });
          updatedJobsCount++;
        } else {
          // Create new job
          await this.prisma.job.create({
            data: {
              title: job.title,
              employmentType: job.employment_type,
              workplaceType: job.workplace_type,
              location: job.location,
              department: job.department,
              url: job.url,
              description: job.description,
              requirements: job.requirements,
              company: job.company,
              source: job.source,
              firstScrapedAt: now,
              lastScrapedAt: now,
            },
          });
          newJobsCount++;
        }
      }

      // Archive jobs that were not in the current scrape
      const jobsToArchive = existingJobs.filter(
        (job) => !scrapedJobUrls.has(job.url) && !job.archivedAt
      );

      for (const job of jobsToArchive) {
        await this.prisma.job.update({
          where: { id: job.id },
          data: {
            archivedAt: now,
          },
        });
        archivedJobsCount++;
      }

      console.log(
        `Scraper completed: ${newJobsCount} new, ${updatedJobsCount} updated, ${archivedJobsCount} archived`
      );

      return {
        success: true,
        message: 'Scraper completed successfully',
        totalCompanies: 0, // TODO: get from scraper output
        successfulScrapes: 0, // TODO: get from scraper output
        currentJobsCount: scrapedJobs.length,
        archivedJobsCount,
        newJobsCount,
      };
    } catch (error) {
      console.error('Scraper failed:', error);
      return {
        success: false,
        message: `Scraper failed: ${error instanceof Error ? error.message : 'Unknown error'}`,
        totalCompanies: 0,
        successfulScrapes: 0,
        currentJobsCount: 0,
        archivedJobsCount: 0,
        newJobsCount: 0,
      };
    }
  }

  async exportActiveJobsCSV(): Promise<string> {
    const jobs = await this.prisma.job.findMany({
      where: {
        archivedAt: null,
        ignored: null,
      },
      orderBy: {
        lastScrapedAt: 'desc',
      },
    });

    // Create CSV header
    const headers = [
      'title',
      'company',
      'location',
      'employment_type',
      'workplace_type',
      'department',
      'url',
      'source',
      'first_scraped_at',
      'last_scraped_at',
    ];

    // Create CSV rows
    const rows = jobs.map((job) => [
      this.escapeCsv(job.title),
      this.escapeCsv(job.company),
      this.escapeCsv(job.location || ''),
      this.escapeCsv(job.employmentType || ''),
      this.escapeCsv(job.workplaceType || ''),
      this.escapeCsv(job.department || ''),
      this.escapeCsv(job.url),
      this.escapeCsv(job.source),
      job.firstScrapedAt.toISOString(),
      job.lastScrapedAt.toISOString(),
    ]);

    // Combine header and rows
    const csv = [headers.join(','), ...rows.map((row) => row.join(','))].join(
      '\n'
    );

    return csv;
  }

  private escapeCsv(value: string): string {
    if (value.includes(',') || value.includes('"') || value.includes('\n')) {
      return `"${value.replace(/"/g, '""')}"`;
    }
    return value;
  }
}
