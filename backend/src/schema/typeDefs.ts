export const typeDefs = `#graphql
  type Job {
    id: ID!
    title: String!
    employmentType: String
    workplaceType: String
    location: String
    department: String
    url: String!
    description: String
    requirements: String
    company: String!
    source: String!
    firstScrapedAt: String!
    lastScrapedAt: String!
    archivedAt: String
    ignored: String
    createdAt: String!
    updatedAt: String!
  }

  type Company {
    id: ID!
    companyName: String!
    companySlug: String!
    domain: String!
    careerPageVendor: String!
    createdAt: String!
    updatedAt: String!
  }

  type ScraperResult {
    success: Boolean!
    message: String!
    totalCompanies: Int!
    successfulScrapes: Int!
    currentJobsCount: Int!
    archivedJobsCount: Int!
    newJobsCount: Int!
  }

  input CompanyInput {
    companyName: String!
    companySlug: String!
    domain: String!
    careerPageVendor: String!
  }

  type Query {
    jobs(
      includeArchived: Boolean
      includeIgnored: Boolean
      company: String
    ): [Job!]!

    job(id: ID!): Job

    companies: [Company!]!

    activeJobsCount: Int!
    archivedJobsCount: Int!
    ignoredJobsCount: Int!
  }

  type Mutation {
    runScraper: ScraperResult!

    # Set ignore to true to mark as ignored ('checked'), false to unmark
    toggleIgnoreJob(jobId: ID!, ignore: Boolean!): Job!

    archiveJob(jobId: ID!): Job!

    addCompany(input: CompanyInput!): Company!

    updateCompany(id: ID!, input: CompanyInput!): Company!

    deleteCompany(id: ID!): Boolean!

    exportActiveJobsCSV: String!
  }
`;
