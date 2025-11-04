## Prerequisites:
This project uses poetry and pyenv.  You can install them with homebrew using:

```brew install pyenv poetry```

## Quickstart
```poetry install```
```cd job_scraper```
```eval $(poetry env activate)```

## Running an individual scraper
```scrapy crawl lever_jobs -o jobs.json -L DEBUG --logfile ./debug.log```

## Running the scraper controller
```poetry run python job_scraper_controller.py```
