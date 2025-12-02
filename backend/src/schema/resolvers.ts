import { PrismaClient } from '@prisma/client';
import { ScraperService } from '../services/scraperService';

export const createResolvers = (prisma: PrismaClient) => {
  const scraperService = new ScraperService(prisma);

  return {
    Query: {
      jobs: async (
        _: any,
        args: {
          includeArchived?: boolean;
          includeIgnored?: boolean;
          company?: string;
        }
      ) => {
        const where: any = {};

        // Filter archived jobs
        if (!args.includeArchived) {
          where.archivedAt = null;
        }

        // Filter ignored jobs
        if (!args.includeIgnored) {
          where.ignored = null;
        }

        // Filter by company
        if (args.company) {
          where.company = {
            contains: args.company,
            mode: 'insensitive',
          };
        }

        return prisma.job.findMany({
          where,
          orderBy: {
            lastScrapedAt: 'desc',
          },
        });
      },

      job: async (_: any, args: { id: string }) => {
        return prisma.job.findUnique({
          where: { id: args.id },
        });
      },

      companies: async () => {
        return prisma.company.findMany({
          orderBy: {
            companyName: 'asc',
          },
        });
      },

      activeJobsCount: async () => {
        return prisma.job.count({
          where: {
            archivedAt: null,
            ignored: null,
          },
        });
      },

      archivedJobsCount: async () => {
        return prisma.job.count({
          where: {
            archivedAt: { not: null },
          },
        });
      },

      ignoredJobsCount: async () => {
        return prisma.job.count({
          where: {
            ignored: { not: null },
          },
        });
      },
    },

    Mutation: {
      runScraper: async () => {
        return scraperService.runScraper();
      },

      toggleIgnoreJob: async (
        _: any,
        args: { jobId: string; ignore: boolean }
      ) => {
        return prisma.job.update({
          where: { id: args.jobId },
          data: {
            ignored: args.ignore ? 'checked' : null,
          },
        });
      },

      archiveJob: async (_: any, args: { jobId: string }) => {
        return prisma.job.update({
          where: { id: args.jobId },
          data: {
            archivedAt: new Date(),
          },
        });
      },

      addCompany: async (
        _: any,
        args: {
          input: {
            companyName: string;
            companySlug: string;
            domain: string;
            careerPageVendor: string;
          };
        }
      ) => {
        return prisma.company.create({
          data: args.input,
        });
      },

      updateCompany: async (
        _: any,
        args: {
          id: string;
          input: {
            companyName: string;
            companySlug: string;
            domain: string;
            careerPageVendor: string;
          };
        }
      ) => {
        return prisma.company.update({
          where: { id: args.id },
          data: args.input,
        });
      },

      deleteCompany: async (_: any, args: { id: string }) => {
        await prisma.company.delete({
          where: { id: args.id },
        });
        return true;
      },

      exportActiveJobsCSV: async () => {
        return scraperService.exportActiveJobsCSV();
      },
    },
  };
};
