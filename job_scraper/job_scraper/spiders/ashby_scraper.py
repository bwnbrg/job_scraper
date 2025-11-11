import scrapy
from scrapy_playwright.page import PageMethod
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class AshbyJobsSpider(scrapy.Spider):
    name = "ashby_jobs"
    start_urls = ["https://jobs.ashbyhq.com/posh"]

    def __init__(self, company=None, domain=None, *args, **kwargs):
        super(AshbyJobsSpider, self).__init__(*args, **kwargs)

        # Set default values if not provided
        self.company = company or "peppr"
        self.domain = domain or "peppr.com"
        
        # Set dynamic domains and URLs
        self.allowed_domains = [

            "jobs.ashbyhq.com",

        ]
        
        self.start_urls = [f"https://jobs.ashbyhq.com/{self.company}"]
        
        self.logger.info(f"Spider initialized for company: {self.company}, domain: {self.domain}")

        

    def parse(self, response):
        options = webdriver.ChromeOptions()
        options.add_argument('--headless') # Run in headless mode
        driver = webdriver.Chrome(options=options)

        # Navigate to the React website


        # Wait for specific elements to be loaded (e.g., by class name or ID)
        try:
            driver.get(self.start_urls[0])


            # Wait for the job list container to appear
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "ashby-job-posting-brief-list"))
            )

            # Find all <a> tags inside the job list container
            job_list = driver.find_element(By.CLASS_NAME, "ashby-job-posting-brief-list")
            job_links = job_list.find_elements(By.TAG_NAME, "a")

            for link in job_links:
                href = link.get_attribute("href")
                full_url = href if href.startswith("http") else f"https://jobs.ashbyhq.com{href}"
                self.logger.info(f"Found job: {full_url}")
                
                yield scrapy.Request(
                    url=full_url,
                    callback=self.parse_job_details
                )

        finally:
            driver.quit()

    def parse_job_details(self, response):
        self.logger.debug(f"Parsing job details from: {response.url}")

        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        driver = webdriver.Chrome(options=options)

        try:
            driver.get(response.url)

            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "ashby-job-posting-heading"))
            )

            title = driver.find_element(By.CLASS_NAME, "ashby-job-posting-heading").text

            def get_section_text(label):
                try:
                    # Try direct <p> sibling
                    return driver.find_element(By.XPATH, f"//h2[text()='{label}']/following-sibling::p").text
                except:
                    try:
                        # Try nested <p> inside <div>
                        return driver.find_element(By.XPATH, f"//h2[text()='{label}']/following-sibling::div//p").text
                    except:
                        return None

            location = get_section_text("Location")
            employment_type = get_section_text("Employment Type")
            location_type = get_section_text("Location Type")
            department = get_section_text("Department")
            compensation = get_section_text("Compensation")

            # Full job description
            try:
                description_container = driver.find_element(By.CLASS_NAME, "_descriptionText_oj0x8_198")
                description = description_container.text
            except:
                description = None

            yield {
                "title": title,
                "location": location,
                "employment_type": employment_type,
                "location_type": location_type,
                "department": department,
                "compensation": compensation,
                "description": description,
                "url": response.url
            }

        except Exception as e:
            self.logger.error(f"Failed to parse job details: {e}")

        finally:
            driver.quit()

#poetry run scrapy crawl ashby_jobs -o jobs.json --logfile debug.log