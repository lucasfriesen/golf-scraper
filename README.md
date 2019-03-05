# golf-scraper
## Web Scraper for Professional Golf Tournament Results

### Install the golf-scraper script, and the master function as follows:

------------------------------------------------------------

import golf-scraper as gs

df = gs.results_collect(YEARS, PATH, TOURS)

YEARS -> List of years as integers

PATH -> The folder to save all files

TOURS -> List of tours to collect for ['PGA TOUR','Web.com Tour','LPGA Tour','European Tour']

#### EXAMPLE 1:

Collect data for the PGA TOUR for the 2018 and 2019 seasons, saving to Desktop.

df = gs.results_collect([2018, 2019], 'C:/Users/###/Desktop/', ['PGA TOUR'])

#### EXAMPLE 2:

Collect all years for all tours and save in Documents.

df = gs.results_collect(list(range(2003,2020)), 'C:/Users/###/Documents/', ['PGA TOUR', 'Web.com Tour', 'LPGA Tour', 'European Tour'])
