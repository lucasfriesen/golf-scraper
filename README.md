# golf-scraper
## Web Scraper for Professional Golf Tournament Results

Install the golfchannel_scraper functions, and call the command like follows:

------------------------------------------------------------

import 

#DEFINE PATH FOR SAVING -THIS WILL BE USED IN ALL OF THE FUNCTIONS - THIS IS THE BASE DIRECTORY FOR ALL FILES TO BE SAVED
path = 'C:/Users/lucas/Documents/Golf/'
    
####################### RESULT COLLECTION ##############################
dm = rf.results_collect([2019], path, ['PGA TOUR','Web.com Tour','LPGA Tour','European Tour'])
