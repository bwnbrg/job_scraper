-- CreateTable
CREATE TABLE "jobs" (
    "id" TEXT NOT NULL,
    "title" TEXT NOT NULL,
    "employment_type" TEXT,
    "workplace_type" TEXT,
    "location" TEXT,
    "department" TEXT,
    "url" TEXT NOT NULL,
    "description" TEXT,
    "requirements" TEXT,
    "company" TEXT NOT NULL,
    "source" TEXT NOT NULL,
    "first_scraped_at" TIMESTAMP(3) NOT NULL,
    "last_scraped_at" TIMESTAMP(3) NOT NULL,
    "archived_at" TIMESTAMP(3),
    "ignored" TEXT,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "jobs_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "companies" (
    "id" TEXT NOT NULL,
    "company_name" TEXT NOT NULL,
    "company_slug" TEXT NOT NULL,
    "domain" TEXT NOT NULL,
    "career_page_vendor" TEXT NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "companies_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "jobs_url_key" ON "jobs"("url");

-- CreateIndex
CREATE INDEX "jobs_url_idx" ON "jobs"("url");

-- CreateIndex
CREATE INDEX "jobs_company_idx" ON "jobs"("company");

-- CreateIndex
CREATE INDEX "jobs_archived_at_idx" ON "jobs"("archived_at");

-- CreateIndex
CREATE INDEX "jobs_ignored_idx" ON "jobs"("ignored");

-- CreateIndex
CREATE UNIQUE INDEX "companies_company_slug_key" ON "companies"("company_slug");
