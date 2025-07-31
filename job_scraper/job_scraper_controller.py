"""
Master controller for running job scrapers across multiple companies
"""

import pandas as pd
import subprocess
import json
import os
from pathlib import Path
import logging
from datetime import datetime
import time

class JobScraperController:
    def __init__(self, companies_file, output_dir="output", ignore_urls_file=None):
        self.companies_file = companies_file
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.ignore_urls_file = ignore_urls_file        
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.output_dir / 'scraper.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Spider mapping
        self.spider_mapping = {
            'lever': 'lever_jobs',
            'greenhouse': 'greenhouse_jobs',
            'getro': 'getro_jobs',
            # Add more as we build them
        }
        
        # Load ignore URLs
        self.ignore_urls = self.load_ignore_urls()        
    
    def load_ignore_urls(self):
        """Load URLs to ignore from CSV file"""
        if not self.ignore_urls_file:
            return set()
        
        ignore_file = Path(self.ignore_urls_file)
        if not ignore_file.exists():
            self.logger.info(f"Ignore URLs file not found: {ignore_file}")
            return set()
        
        try:
            # Support different CSV formats
            ignore_df = pd.read_csv(ignore_file)
            
            # grab url column name
            url_column = 'url'
            
            ignore_urls = set(ignore_df[url_column].dropna().astype(str))
            self.logger.info(f"Loaded {len(ignore_urls)} URLs to ignore from {ignore_file}")
            
            return ignore_urls
            
        except Exception as e:
            self.logger.error(f"Error loading ignore URLs file: {e}")
            return set()
            
    def load_companies(self):
        """Load companies from CSV or JSON file"""
        if self.companies_file.endswith('.csv'):
            return pd.read_csv(self.companies_file)
        elif self.companies_file.endswith('.json'):
            with open(self.companies_file, 'r') as f:
                data = json.load(f)
                return pd.DataFrame(data)
        else:
            raise ValueError("Companies file must be CSV or JSON")
    
    def load_previous_jobs(self):
        """Load jobs from the previous_jobs.jsonl file"""
        previous_file = self.output_dir / 'previous_jobs.jsonl'
        if not previous_file.exists():
            self.logger.info("No previous job file found - first run")
            return pd.DataFrame()
        
        try:
            previous_jobs = pd.read_json(previous_file, lines=True)
            self.logger.info(f"Loaded {len(previous_jobs)} jobs from previous run: {previous_file}")
            return previous_jobs
        except Exception as e:
            self.logger.error(f"Error loading previous jobs: {e}")
            return pd.DataFrame()

    def add_scrape_timestamps(self, current_jobs, previous_jobs):
        """Add first_scraped_at and last_scraped_at timestamps to jobs, removing spider's scraped_at"""
        if current_jobs.empty:
            self.logger.debug("Current_jobs DataFrame is empty, skipping timestamp addition")
            return current_jobs
        
        current_timestamp = datetime.now()
        
        # Remove the spider's scraped_at field since we're replacing it with proper tracking
        if 'scraped_at' in current_jobs.columns:
            current_jobs = current_jobs.drop(columns=['scraped_at'])
            self.logger.debug("Removed spider's scraped_at field - replacing with first/last scraped tracking")
        
        # Initialize the timestamp columns
        current_jobs['first_scraped_at'] = current_timestamp
        current_jobs['last_scraped_at'] = current_timestamp
        
        if not previous_jobs.empty and 'url' in previous_jobs.columns:
            # Create a mapping of URL to first_scraped_at from previous jobs
            previous_first_scraped = {}
            for _, job in previous_jobs.iterrows():
                url = job.get('url')
                first_scraped = job.get('first_scraped_at')
                if url and pd.notna(first_scraped):
                    previous_first_scraped[url] = first_scraped
            
            # Update first_scraped_at for jobs that existed in previous runs
            # (last_scraped_at stays as current timestamp for all jobs)
            for idx, job in current_jobs.iterrows():
                url = job.get('url')
                if url in previous_first_scraped:
                    current_jobs.at[idx, 'first_scraped_at'] = previous_first_scraped[url]
                    self.logger.debug(f"Preserved first_scraped_at for existing job: {url}")
        
        # Log summary of new vs existing jobs
        new_jobs = current_jobs[current_jobs['first_scraped_at'] == current_timestamp]
        existing_jobs = current_jobs[current_jobs['first_scraped_at'] != current_timestamp]
        
        self.logger.info(f"Timestamp summary: {len(new_jobs)} new jobs, {len(existing_jobs)} existing jobs updated")
        
        return current_jobs

    def mark_ignored_jobs(self, jobs_df):
        """Mark jobs as ignored based on ignore URLs list"""
        if jobs_df.empty or not self.ignore_urls:
            return jobs_df
        
        # Add ignored_at column if it doesn't exist
        if 'ignored' not in jobs_df.columns:
            jobs_df['ignored'] = None
        
        # Mark jobs as ignored if their URL is in the ignore list
        url_mask = jobs_df['url'].isin(self.ignore_urls)
        newly_ignored_count = url_mask.sum()
        
        if newly_ignored_count > 0:
            jobs_df.loc[url_mask, 'ignored'] = 'checked'
            
            self.logger.info(f"Marked {newly_ignored_count} jobs as ignored")
            
            # Log details of ignored jobs
            ignored_jobs = jobs_df[url_mask]
            if 'company' in ignored_jobs.columns and 'title' in ignored_jobs.columns:
                for _, job in ignored_jobs.iterrows():
                    self.logger.info(f"  Ignored: {job.get('company', 'Unknown')} - {job.get('title', 'Unknown')}")
        
        return jobs_df

    def identify_archived_jobs(self, previous_jobs, current_jobs):
        """Identify jobs that were in previous run but not in current run"""
        if previous_jobs.empty:
            return pd.DataFrame()
        
        # Filter out jobs that were already archived (have archived_at field)
        if 'archived_at' in previous_jobs.columns:
            active_previous_jobs = previous_jobs[
                pd.isna(previous_jobs['archived_at'])
            ].copy()
        else:
            # If no archived_at column exists, all jobs are considered active
            active_previous_jobs = previous_jobs.copy()
        
        if active_previous_jobs.empty:
            return pd.DataFrame()
        
        # Use URL as the unique identifier
        previous_urls = set(active_previous_jobs['url'].dropna())
        current_urls = set(current_jobs['url'].dropna())
        
        # Find URLs that were in previous but not in current
        archived_urls = previous_urls - current_urls
        
        if not archived_urls:
            return pd.DataFrame()
        
        # Get the jobs that should be archived
        archived_jobs = active_previous_jobs[
            active_previous_jobs['url'].isin(archived_urls)
        ].copy()
        
        if not archived_jobs.empty:
            # Mark as archived with timestamp
            archived_jobs['archived_at'] = datetime.now()
            
            self.logger.info(f"Found {len(archived_jobs)} jobs to archive")
            
            # Log details of archived jobs
            if 'company' in archived_jobs.columns and 'title' in archived_jobs.columns:
                for _, job in archived_jobs.iterrows():
                    self.logger.info(f"  Archived: {job.get('company', 'Unknown')} - {job.get('title', 'Unknown')}")
        
        return archived_jobs
    
    def run_spider(self, spider_name, company_data, main_output_file):
        """Run a specific spider with company parameters"""
        try:
            # Build the scrapy command - append to main output file
            cmd = [
                'scrapy', 'crawl', spider_name,
                '-o', str(main_output_file),
                '-a', f'company={company_data["company_slug"]}',
                '-a', f'domain={company_data["domain"]}',
                # '--logfile', str(self.output_dir / f'{company_data["company_name"]}_spider.log') #uncomment for individual logs
            ]
            
            self.logger.info(f"Running spider for {company_data['company_name']}: {' '.join(cmd)}")
            
            # Run the spider
            result = subprocess.run(cmd, capture_output=True, text=True, cwd='.')
            
            if result.returncode == 0:
                self.logger.info(f"Successfully scraped {company_data['company_name']}")
                return True
            else:
                self.logger.error(f"Failed to scrape {company_data['company_name']}: {result.stderr}")
                return False
                
        except Exception as e:
            self.logger.error(f"Exception running spider for {company_data['company_name']}: {str(e)}")
            return False
    
    def combine_jobs_dataframes(self, current_jobs, archived_jobs):
        """Combine current and archived jobs dataframes, ensuring column alignment"""
        if archived_jobs is None or archived_jobs.empty:
            return current_jobs
        
        if current_jobs.empty:
            return archived_jobs
        
        # Ensure both dataframes have the same columns
        all_columns = set(current_jobs.columns) | set(archived_jobs.columns)
        
        for col in all_columns:
            if col not in current_jobs.columns:
                current_jobs[col] = None
            if col not in archived_jobs.columns:
                archived_jobs[col] = None
        
        # Combine the dataframes
        combined_df = pd.concat([current_jobs, archived_jobs], ignore_index=True)
        self.logger.info(f"Combined {len(current_jobs)} current jobs with {len(archived_jobs)} archived jobs")
        
        return combined_df

    def convert_output_to_csv(self, processed_jobs, jsonl_file, archived_jobs=None):
        """Convert processed jobs dataframe to CSV and merge with archived jobs"""
        
        try:
            # Load the already-processed jobs (timestamps and ignore flags already added)            
            current_jobs = processed_jobs.copy()

            self.logger.info(f"Converting {len(current_jobs)} processed jobs to CSV")

            # Combine current jobs with archived jobs
            final_df = self.combine_jobs_dataframes(current_jobs, archived_jobs)
            
            if final_df.empty:
                self.logger.warning("No jobs to save")
                return None
            
            csv_file = jsonl_file.with_suffix('.csv')
            final_df.to_csv(csv_file, index=False, encoding='utf-8')
            
            self.logger.info(f"Successfully saved {len(final_df)} total jobs to CSV: {csv_file}")
            self.logger.info(f"CSV columns: {list(final_df.columns)}")
            
            # Log summary statistics
            if 'company' in final_df.columns:
                company_counts = final_df['company'].value_counts()
                self.logger.info("Jobs per company:")
                for company, count in company_counts.items():
                    self.logger.info(f"  {company}: {count} jobs")
            
            # Log job status breakdown
            active_count = len(final_df)
            archived_count = 0
            ignored_count = 0
            
            if 'archived_at' in final_df.columns:
                archived_count = final_df['archived_at'].notna().sum()
                active_count -= archived_count
            
            if 'ignored' in final_df.columns:
                ignored_count = final_df['ignored'].notna().sum()
                # Ignored jobs might also be archived, so adjust counts
                ignored_and_archived = final_df[
                    final_df['ignored'].notna() & final_df['archived_at'].notna()
                ].shape[0] if 'archived_at' in final_df.columns else 0
                
                # Active jobs should exclude ignored jobs
                active_ignored = final_df[
                    final_df['ignored'].notna() & final_df['archived_at'].isna()
                ].shape[0] if 'archived_at' in final_df.columns else final_df['ignored'].notna().sum()
                
                active_count -= active_ignored
            
            self.logger.info(f"Job status: {active_count} active, {archived_count} archived, {ignored_count} ignored")
            
            # Log timestamp statistics

            if 'first_scraped_at' in final_df.columns:

                current_date = datetime.now().date()

                new_today = (final_df['first_scraped_at'].dt.date == current_date).sum()

                self.logger.info(f"Jobs first discovered today: {new_today}")

            return csv_file
            
        except Exception as e:
            self.logger.error(f"Error converting jobs to CSV: {e}")
            return None
    
    def save_current_jobs_as_previous(self, processed_jobs, archived_jobs=None):
        """Save current run as previous_jobs.jsonl for next run comparison"""
        try:
            # Load the already processed current jobs (timestamps and ignore flags already added)
            current_jobs = processed_jobs.copy()
            
            # Combine with archived jobs for complete dataset
            combined_jobs = self.combine_jobs_dataframes(current_jobs, archived_jobs)
            
            # Save as previous_jobs.jsonl
            previous_file = self.output_dir / 'previous_jobs.jsonl'
            combined_jobs.to_json(previous_file, orient='records', lines=True)
            
            self.logger.info(f"Saved {len(combined_jobs)} jobs to previous_jobs.jsonl for next run")
            
        except Exception as e:
            self.logger.error(f"Error saving previous jobs: {e}")
    
    def run_all_scrapers(self, delay_between_companies=1):
        """Main method to run all scrapers"""
        self.logger.info("Starting job scraping automation")
        
        # Load previous jobs for archiving logic and timestamp tracking
        previous_jobs = self.load_previous_jobs()
        
        # Load companies
        companies_df = self.load_companies()
        self.logger.info(f"Loaded {len(companies_df)} companies")
        
        # Create single output file with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        main_output_file = self.output_dir / f'all_jobs.jsonl' #use f'all_jobs_{timestamp}.jsonl' if you want each run to have its own file
        
        # Ensure the output file doesn't exist (delete if it does)
        if main_output_file.exists():
            main_output_file.unlink()
            self.logger.info(f"Removed existing output file: {main_output_file}")

        successful_scrapes = 0
        
        for idx, company in companies_df.iterrows():
            vendor = company['career_page_vendor'].lower()
            
            if vendor not in self.spider_mapping:
                self.logger.warning(f"No spider available for vendor: {vendor} (company: {company['company_name']})")
                continue
            
            spider_name = self.spider_mapping[vendor]
            
            # Run the spider - all output goes to the same file
            success = self.run_spider(spider_name, company, main_output_file)
            
            if success:
                successful_scrapes += 1
            
            # Add delay between companies to be respectful
            if idx < len(companies_df) - 1:  # Don't delay after the last company
                self.logger.info(f"Waiting {delay_between_companies} seconds before next company...")
                time.sleep(delay_between_companies)
        
        self.logger.info(f"Completed scraping. Successful: {successful_scrapes}/{len(companies_df)}")
        
        # Load current jobs from the JSONL file and process them
        current_jobs = pd.DataFrame()
        if main_output_file.exists():
            try:
                current_jobs = pd.read_json(main_output_file, lines=True)
                
                # Add timestamps to current jobs
                current_jobs = self.add_scrape_timestamps(current_jobs, previous_jobs)
                
                self.logger.info(f"Loaded {len(current_jobs)} jobs from JSONL")
                # Debug: log columns to see what we actually have
                if not current_jobs.empty:
                    self.logger.info(f"JSONL columns: {list(current_jobs.columns)}")
                else:
                    self.logger.warning("JSONL file exists but contains no data")
                
                # Mark ignored jobs
                current_jobs = self.mark_ignored_jobs(current_jobs)

                self.logger.info(f"Processed {len(current_jobs)} jobs with timestamps and ignore flags")
            except Exception as e:
                self.logger.error(f"Error loading and processing current jobs: {e}")
        
        # Identify archived jobs
        archived_jobs = self.identify_archived_jobs(previous_jobs, current_jobs)
        
        # Convert processed Dataframe to CSV and include archived jobs
        csv_output = self.convert_output_to_csv(current_jobs, main_output_file, archived_jobs)
        
        # Save current run as previous_jobs.jsonl for next comparison
        self.save_current_jobs_as_previous(current_jobs, archived_jobs)
        
        # Return summary of the run
        return {
            'total_companies': len(companies_df),
            'successful_scrapes': successful_scrapes,
            'json_output': main_output_file,
            'csv_output': csv_output,
            'current_jobs_count': len(current_jobs) if not current_jobs.empty else 0,
            'archived_jobs_count': len(archived_jobs) if not archived_jobs.empty else 0,
            'ignored_urls_count': len(self.ignore_urls)            
        }

# Example usage
if __name__ == "__main__":
    # Example companies file format (CSV):
    # company_name,company_slug,domain,career_page_vendor
    # Immuta,immuta,immuta.com,lever
    # Example Company,example,example.com,greenhouse
    
    controller = JobScraperController(
        companies_file='companies.csv',
        ignore_urls_file='ignore_urls.csv'  # Optional parameter
    )

    results = controller.run_all_scrapers(delay_between_companies=1)
    
    print(f"Scraping completed!")
    print(f"Results: {results}")

# To run: 
# 1. Update "ignore_urls.csv"  with most up-to-date URLs to ignore.
# 2. Update "companies.csv" with the companies you want to scrape.
# 3. run command in terminal: poetry run python job_scraper_controller.py