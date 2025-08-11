update.disease.notification <- function(fonte = "dengue", ano = "recente", sivep_links) {

if(ano == "recente") {
ano_atual <- year(Sys.Date())
anos_seq = (ano_atual-1):ano_atual
} else if (ano == "todos") {
  ano_atual <- year(Sys.Date())
  
  primeiro_ano = case_when(fonte == "dengue" ~ 2010,
            fonte == "chikungunya" ~ 2015,
            fonte == "srag" ~ 2009)
  
  anos_seq = primeiro_ano:ano_atual
} else {
  anos_seq = as.numeric(ano)
}

if(fonte == "dengue") {
for(year in anos_seq) {
  
  # Print the current year to track progress
  print(year)
  
  # Fetch data from SINAN-DENGUE system for the specified year
  sinan_data <- fetch_datasus(year_start = year, 
                              year_end = year, 
                              information_system = "SINAN-DENGUE", 
                              timeout = 2000)
  
  # Generate the filename for saving the data
  filename <- paste0("data/SINAN/dengue_",year,".csv.gz")
  
  # Print a message indicating the file being written
  print(paste0("Writing: ", filename))
  
  # Save the data as a compressed CSV file
  fwrite(sinan_data, file = filename, compress="gzip")
  
  # Print a confirmation message once the file is successfully written
  print(paste("Writing: OK"))
}
}

if(fonte == "chikungunya") {
  
  # Loop through the years 2015 to 2025 to download and save data for Chikungunya
  for(year in anos_seq) {
    
    # Print the current year to track progress
    print(year)
    
    # Fetch data from SINAN-CHIKUNGUNYA-FINAL system for the specified year
    sinan_data <- fetch_datasus(year_start = year, 
                                year_end = year, 
                                information_system = "SINAN-CHIKUNGUNYA", 
                                timeout = 2000)
    
    # Generate the filename for saving the data
    filename <- paste0("data/SINAN/chikungunya_",year,".csv.gz")
    
    # Print a message indicating the file being written
    print(paste0("Writing: ", filename))
    
    # Save the data as a compressed CSV file
    fwrite(sinan_data, file = filename, compress="gzip")
    
    # Print a confirmation message once the file is successfully written
    print(paste("Writing: OK"))
    
  }
}
  if(fonte == "srag") { 
  
  download_sivep_data(sivep_links, years_to_download = anos_seq)
  
  }
}



extract.gt <- function(your_api_key, latest_month = "") {
  
  # Load the list of Brazilian federative units (states)
  FUs <- read.csv("data/br_federative_units.csv")
  
  # Create a vector of geographic codes used in gtrends, including all states and "BR" for the whole country
  federative_units <- c(paste0("BR-", sort(FUs$ABBREV)), "BR")
  
  ########################
  # Create directory to store output files for the current date
  # Use this to update GoogleTrends extraction folder for most recent date
  mainDir <- "./data/GoogleTrends/"
  day_date <- format(Sys.time(), format = "%Y_%m_%d")  # Get current date in YYYY_MM_DD format
  dir_path = paste0(mainDir, day_date)  # Combine base path with date
  dir.create(file.path(dir_path), showWarnings = FALSE)  # Create directory if it doesn't exist
  
  # Load and prepare search term queries
  
  # Search terms for diseases
  disease_query_tbl <- data.frame(
    group = c("Dengue", "Chikungunya", "Influenza", "COVID-19"),
    query = c("/m/09wsg", "/m/01__7l", "/m/0cycc", 
              'COVID-19%20%2B%20covid-19%20%2B%20covid%20%2B%20sarscov2%20%2B%20"covid%2019"%20%2B%20covid19')
  )
  
  # Load popular symptom-related search terms
  popular_terms <- read.csv("data/popular_terms.csv")
  
  # Prepare query strings for coded topics (i.e., specific Google Trends topic codes using Freebase ID)
  query_table_with_code <- popular_terms %>%
    filter(is_code) %>%
    group_by(group) %>%
    mutate(terms_code = paste0(terms, collapse = "%20%2B%20")) %>%  # Join all topic codes with "+"
    ungroup() %>%
    select(group, terms_code)
  
  # Prepare query strings for non-coded (free text) search terms
  query_table <- popular_terms %>% 
    mutate(terms = stringi::stri_trans_general(terms, "Latin-ASCII")) |>
    filter(!is_code) %>%
    group_by(group) %>% 
    summarise(query = paste(trimws(str_to_lower(terms)), collapse = '\"%20%2B%20\"')) %>%  # Concatenate terms
    ungroup() %>%
    mutate(query = paste0('\"', query, '\"'),   # Wrap query in quotes
           query = gsub(" ", "%20", query)) %>%  # URL-encode spaces
    full_join(query_table_with_code, by = "group") %>%  # Merge with coded queries
    mutate(query = case_when(
      !is.na(terms_code) & is.na(query) ~ terms_code,
      !is.na(terms_code) & !is.na(query) ~ paste0(terms_code, "%20%2B%20", query),
      TRUE ~ query
    )) %>%
    bind_rows(disease_query_tbl) %>%  # Add disease queries to the full query list
    distinct()  # Remove duplicates
  
  # Define time window for Google Trends queries
  
  # Use current system date to define the most recent month and a 5-year window
  if(latest_month == "") {
    most_recent_year_month <- format(Sys.time(), format = "%Y-%m")
  } else {
    most_recent_year_month <- latest_month
  }
  
  five_years <- format(as.Date(Sys.time()) - 365*5 + 31, format = "%Y-%m")
  
  # Remove certain overlapping or redundant symptom groups from the query table
  query_table <- query_table %>%
    filter(!group %in% c(
      "Perda de olfato", "Perda do paladar", "Alteração do paladar",
      "Alteração de olfato", "Dor ocular", "Dor atrás dos olhos"
    ))
  
  ########################
  # Extract Google Trends data
  
  ini = Sys.time()  # Record the start time of the extraction
  gt_results <- data.frame()  # Initialize an empty dataframe to store successful results
  error_df <- data.frame()    # Initialize an empty dataframe to store any errors
  
  # Loop over each query (each symptom or disease)
  for(n in 1:nrow(query_table)) {
    
    # Extract the group name (e.g., "Dengue") and its associated query string
    topic = unlist(query_table[n, "group"])
    topic_query = unlist(query_table[n, "query"])
    
    # Loop over each federative unit (e.g., BR-SP, BR-RJ, ..., BR)
    for (i in 1:28) {
      
      # Print status message with current topic, location, and time window
      print(paste0("Topic: ", topic,
                   "     Query for locations: ", federative_units[i],
                   "     Time-frame: ", five_years, " to ", most_recent_year_month))
      
      sys_time = substr(Sys.time(), 1, 16)  # Save the system time for recordkeeping
      error_message <- NA  # Reset error message variable before each API call
      
      # Try to retrieve Google Trends data via custom API wrapper
      gt_temp <- try_gtrends_api(
        topic_keyword = topic_query, 
        geo_location = federative_units[i], 
        start_date = five_years, 
        end_date = most_recent_year_month,
        api_key = your_api_key
      )
      
      # Stop the script if API rate limit (429) is reached
      if(error_message == "Status code was not 200. Returned status code:429" & !is.na(error_message)) {
        stop()
      }
      
      # If the result is NA (i.e., failed retrieval), store the error for later review
      if((length(gt_temp) == 1 & all(is.na(gt_temp)))) {
        
        error_df_temp <- data.frame(
          keyword = topic,
          geo = federative_units[i],
          time = paste0(five_years, " to ", most_recent_year_month),
          error = error_message
        )
        
        error_df <- rbind(error_df, error_df_temp)
        
      } else {
        # If data is successfully retrieved, clean and append to the results table
        gt_results <- bind_rows(gt_results,
                                gt_temp %>%
                                  mutate(keyword2 = keyword) %>%
                                  select(value, date, geo, time, keyword2) %>%
                                  mutate(
                                    keyword = topic,               # Add original group name as keyword
                                    value = ifelse(value == "<1", 0.1, value),  # Convert "<1" to 0.1
                                    value = as.numeric(value),     # Ensure numeric type
                                    sys_time = sys_time            # Add system time for timestamping
                                  )
        )
      }
    }
  }
  
  fin = Sys.time()  # Record end time
  print(fin - ini)  # Print total processing time
  
  # Clean and prepare GoogleTrends data
  gt_ts_final <- gt_results %>%
    select(-all_of(c("sys_time", "time"))) %>%
    mutate(
      geo = substr(geo, 4, 5),
      geo = as.character(geo),
      geo = ifelse(geo == "", "BR", geo),
      location = factor(geo),
      date = as.Date(date),
      topic = factor(keyword)
    ) %>%
    select(date, location, topic, value) %>%
    complete(date, location, topic, fill = list(value = 0))
  
  # Save successful results to CSV file
  filename <- paste0(dir_path, "/GoogleTrends_search.csv")
  print(paste0("Writing: ", filename))
  fwrite(gt_ts_final, filename)
  
  # If any errors occurred, save the error log as well
  if(nrow(error_df) > 0) {
    filename_error <- paste0(dir_path, "/query_error.csv")  
    print(paste0("Writing: ", filename_error))
    fwrite(error_df, filename_error)
  }
  
}
### SRAG DATA EXTRACTION AND SAVING

#' Get all SIVEP-SRAG CSV download links from OpenDataSUS
#'
#' This function scrapes the OpenDataSUS SRAG dataset page and recursively extracts
#' all available links to `.csv` files related to SIVEP-SRAG data (files with "INFLUD" in the name).
#'
#' It first finds all datasets tagged with "SRAG", then visits each dataset page,
#' identifies the associated resource pages, and from each resource page, extracts the
#' direct download link for the corresponding CSV file.
#'
#' @return A character vector containing unique URLs of CSV files with SIVEP-SRAG data.
#' 
get_all_srag_links <- function() {
  library(rvest)
  library(stringr)
  library(dplyr)
  library(purrr)
  
  # 1. Main OpenDataSUS page with SRAG tag
  main_page <- "https://opendatasus.saude.gov.br/dataset?tags=SRAG"
  
  # 2. Read the main page HTML
  html_page <- read_html(main_page)
  
  # 3. Extract all dataset links from the main page
  links_dataset <- html_page %>%
    html_elements("a") %>%
    html_attr("href") %>%
    unique() %>%
    str_subset("^/dataset/") %>%
    paste0("https://opendatasus.saude.gov.br", .)
  
  # 4. For each dataset, find resource pages and extract INFLUD*.csv links
  get_sari_links_csv <- function(dataset_url) {
    html_dataset <- tryCatch(read_html(dataset_url), error = function(e) return(NULL))
    if (is.null(html_dataset)) return(NULL)
    
    links_resource <- html_dataset %>%
      html_elements("a") %>%
      html_attr("href") %>%
      unique() %>%
      str_subset("^/dataset/.*/resource/") %>%
      paste0("https://opendatasus.saude.gov.br", .)
    
    links_csv <- map_chr(links_resource, function(resource_url) {
      resource_html <- tryCatch(read_html(resource_url), error = function(e) return(NA))
      if (is.na(resource_html)) return(NA)
      
      all_links <- resource_html %>% html_elements("a") %>% html_attr("href")
      link_influd <- all_links[grepl("^https.*INFLUD.*\\.csv$", all_links)]
      
      if (length(link_influd) == 0) return(NA)
      return(link_influd[1])
    })
    
    links_csv[!is.na(links_csv)]
  }
  
  # 5. Apply to all datasets and return a unique list of CSV links
  all_links <- links_dataset %>%
    map(get_sari_links_csv) %>%
    unlist() %>%
    unique()
  
  return(all_links)
}

#' Download and save SIVEP-SRAG data from OpenDataSUS CSV links
#'
#' This function downloads one or more SIVEP-SRAG `.csv` files from OpenDataSUS, 
#' using curl via `fread()`, with retry logic and customizable timeout.
#'
#' @param all_sari_links_csv Character vector with URLs to the INFLUD CSV files.
#' @param years_to_download Either "all" (default) or a numeric vector of years to download, e.g., c(2023, 2024).
#' @param dest_dir Destination directory where the files will be saved. Defaults to "data/SIVEP".
#' @param max_tries Maximum number of download attempts per file. Default is 3.
#' @param timeout_sec Timeout (in seconds) for each download attempt using curl. Default is 300.
#'
#' @return Downloads the files and saves them as .csv.gz in the specified folder. 
#'         Prints a list of failed downloads, if any.

download_sivep_data <- function(all_sari_links_csv_total, 
                                years_to_download = "all", 
                                dest_dir = "data/SIVEP", 
                                max_tries = 5, 
                                timeout_sec = 10000) {
  # Load necessary packages
  library(stringr)     # for string manipulation
  library(data.table)  # for fread/fwrite
  library(fs)          # for file system handling (e.g., creating folders)
  
  # Ensure destination folder exists
  dir_create(dest_dir)
  
  # Extract the last two digits of the year from the link using regex
  years_last_digits <- str_extract(all_sari_links_csv_total, "INFLUD\\d{2}") %>%
    str_extract("\\d{2}")
  
  
  all_sari_links_csv_tbl <- data.frame(url = all_sari_links_csv_total) |>
    mutate(years_last_digits = str_extract(url, "INFLUD\\d{2}"),
           years_last_digits = str_extract(years_last_digits, "\\d{2}"),
           years_all =  paste0("20", years_last_digits),
           nc = nchar(url),
           date_string = paste0(substr(url, nc-7, nc-4),"-",substr(url, nc-10, nc-9),"-",substr(url, nc-13, nc-12)),
           date = as.Date(date_string, format = "%Y-%m-%d")
    ) |>
    select(url, years_all, date) |>
    arrange(years_all, desc(date)) |>
    group_by(years_all) |>
    mutate(order = 1:n()) |>
    ungroup() |>
    filter(order == 1)
  
  # Build the full year (e.g., "24" -> "2024")
  years_all <- paste0("20", years_last_digits)
  
  # If the user requested specific years, filter only those links
  if (!identical(years_to_download, "all")) {
    # selected <- years_all %in% as.character(years_to_download)
    # all_sari_links_csv <- all_sari_links_csv_total[selected]
    # years <- years_all[selected]
    selected_tbl <- all_sari_links_csv_tbl |> filter(years_all %in% years_to_download)
  } else {
    selected_tbl <- all_sari_links_csv_tbl
  }
  
  # Initialize a vector to store failed downloads
  failed_downloads <- character(0)
  
  # Mark the start time to measure total duration
  ini <- Sys.time()
  
  # Iterate over each CSV URL
  for (i in 1:nrow(selected_tbl)) {
    sivep_url <- selected_tbl$url[i]
    year <- selected_tbl$years_all[i]
    filename <- file.path(dest_dir, paste0("INFLUD", year, ".csv.gz"))
    
    message("Reading: ", sivep_url)
    
    success <- FALSE
    attempt <- 1
    
    # Retry loop: attempt to download and read the file
    while (!success && attempt <= max_tries) {
      try({
        # Use curl via fread with timeout and redirect following
        curl_cmd <- paste("curl --max-time", timeout_sec, "-L", shQuote(sivep_url))
        sivep_data <- fread(cmd = curl_cmd)
        success <- TRUE
      }, silent = TRUE)
      
      # If attempt failed, wait a few seconds before retrying
      if (!success) {
        message("Attempt ", attempt, " failed. Retrying in ", 2^attempt, " seconds...")
        Sys.sleep(2 ^ attempt)  # Exponential backoff: 2, 4, 8...
        attempt <- attempt + 1
      }
    }
    
    # If successful, write the data to a compressed CSV file
    if (success) {
      message("Writing: ", filename)
      fwrite(sivep_data, file = filename, compress = "gzip")
    } else {
      # If all attempts failed, record the failure
      message("Failed after ", max_tries, " attempts: ", sivep_url)
      failed_downloads <- c(failed_downloads, paste0("Year ", year, ": ", sivep_url))
    }
  }
  
  # Calculate and print the total execution time
  fim <- Sys.time()
  message("Total time: ", round(difftime(fim, ini, units = "mins"), 2), " minutes")
  
  # Report any failed downloads
  if (length(failed_downloads) > 0) {
    message("\nThe following files failed to download:")
    for (fail in failed_downloads) {
      message("- ", fail)
    }
  } else {
    message("All files downloaded successfully.")
  }
}

####


extract.gt.related <- function(type = c("query", "topic"), metric = "top", time_seq, your_api_key) {
  
  if(!metric %in% c("top", "rising")) stop("Metric must be either 'top' or 'rising' ")
  
  # Load state codes (federative units) in Brazil
  FUs <- read.csv("data/br_federative_units.csv")
  # Create a vector of Google Trends codes for each state, plus the whole country ("BR")
  FU_code <- c(paste0("BR-",sort(FUs$ABBREVIATION)), "BR")
  
  ########################
  # Create directory to store output files
  dir_path <- "./data/GoogleTrends/related"
  dir.create(file.path(dir_path), showWarnings = FALSE)
  
  # Define search terms and prepare queries
  
  # Table of disease-related search terms and corresponding topic codes or queries
  disease_query_tbl <- data.frame(
    group = c("Dengue", "Chikungunya", "Influenza/gripe", "COVID-19"),
    query = c("/m/09wsg", "/m/01__7l", "/m/0cycc",
              'COVID-19%20%2B%20covid-19%20%2B%20covid%20%2B%20sarscov2%20%2B%20"covid%2019"%20%2B%20covid19')
  )
  
  ########################
  # Extract data from Google Trends
  
  # Track processing time
  ini = Sys.time()
  
  # Loop through each month in time range
  for(t in time_seq) {
    
    # Initialize result and error data.frames
    gt_results_topic <- data.frame()
    error_df_topic <- data.frame()
    gt_results_query <- data.frame()
    error_df_query <- data.frame()
    
    # Loop through diseases (4 rows in the query table)
    for(n in 1:4) {
      
      topic = unlist(disease_query_tbl[n, "group"])
      topic_query = unlist(disease_query_tbl[n, "query"])
      
      # Loop through 27 Brazilian states + BR (total 28 geolocations)
      for (i in 1:28) {
        
        month_date <- substr(as.character(t), 1, 7)  # Get year-month format
        
        print(paste0("Topic: ", topic,
                     "     Query for locations: ", FU_code[i],
                     "     Time-frame: ", month_date))
        
        sys_time = substr(Sys.time(), 1, 16)  # Record system time for tracking
        
        if(metric == "top") {
          
        if("topic" %in% type) {
        ## -------- Get TOPICS -------- ##
        error_message <- NA  # Reset error message
        
        # Try to get Google Trends data for related topics
        gt_temp_topic <- try_gtrends_api(
          topic_keyword = topic_query,
          geo_location = FU_code[i],
          start_date = month_date,
          end_date = month_date,
          fun = "topics",
          api_key = your_api_key
        )
        
        # Stop execution if API rate limit is exceeded
        if(error_message == "Status code was not 200. Returned status code:429" &
           !is.na(error_message)) {stop()}
        
        # Save error if no data was returned
        if((length(gt_temp_topic) == 1 & all(is.na(gt_temp_topic)))) {
          error_df_temp_topic <- data.frame(
            keyword = topic,
            geo = FU_code[i],
            time = month_date,
            error = error_message
          )
          error_df_topic <- rbind(error_df_topic, error_df_temp_topic)
          
        } else {
          # Append valid results to main data frame
          gt_results_topic <- bind_rows(gt_results_topic,
                                        gt_temp_topic %>%
                                          select(topicTitle, topicId, value, geo) %>%
                                          mutate(
                                            keyword = topic,
                                            time = month_date,
                                            value = ifelse(value == "<1", 0.1, value),  # Replace "<1" with 0.1
                                            value = as.numeric(value),
                                            sys_time = sys_time
                                          )
          )
        }
        }
        
        if("query" %in% type) {
          
        ## -------- Get QUERIES -------- ##
        error_message <- NA  # Reset error message again
        
        # Try to get Google Trends data for top related search queries
        gt_temp_query <- try_gtrends_api(
          topic_keyword = topic_query,
          geo_location = FU_code[i],
          start_date = month_date,
          end_date = month_date,
          fun = "queries",
          api_key = your_api_key
        )
        
        # Save error if no data returned
        if("error" %in% colnames(gt_temp_query)) {
        # if((length(gt_temp_query) == 1 & all(is.na(gt_temp_query)))) {
        #   error_df_temp_query <- data.frame(
        #     keyword = topic,
        #     geo = FU_code[i],
        #     time = month_date,
        #     error = error_message
        #   )
        #   error_df_query <- rbind(error_df_query, error_df_temp_query)
          error_df_query <- rbind(error_df_query, gt_temp_query)
        } else {
          # Append valid query results to main data frame
          gt_results_query <- bind_rows(gt_results_query,
                                        gt_temp_query %>%
                                          select(topSearches, value, geo) %>%
                                          mutate(
                                            keyword = topic,
                                            time = month_date,
                                            value = ifelse(value == "<1", 0.1, value),
                                            value = as.numeric(value),
                                            sys_time = sys_time
                                          )
          )
        }
        } # if query
        } # if metric top
        
        if(metric == "rising") {
          
          if("topic" %in% type) {
            ## -------- Get TOPICS -------- ##
            error_message <- NA  # Reset error message
            
            # Try to get Google Trends data for related topics
            gt_temp_topic <- try_gtrends_api(
              topic_keyword = topic_query,
              geo_location = FU_code[i],
              start_date = month_date,
              end_date = month_date,
              fun = "rtopics",
              api_key = your_api_key
            )
            
            # Stop execution if API rate limit is exceeded
            if(error_message == "Status code was not 200. Returned status code:429" &
               !is.na(error_message)) {stop()}
            
            # Save error if no data was returned
            if("error" %in% colnames(gt_temp_topic)) {
              #if((length(gt_temp_topic) == 1 & all(is.na(gt_temp_topic)))) {
              # error_df_temp_topic <- data.frame(
              #   keyword = topic,
              #   geo = FU_code[i],
              #   time = month_date,
              #   error = error_message
              # )
              # error_df_topic <- rbind(error_df_topic, error_df_temp_topic)
              error_df_query <- rbind(error_df_query, gt_temp_topic)
              
            } else {
              
              if(any(is.na(colnames(gt_temp_topic)))) {
                colnames(gt_temp_topic)[which(is.na(colnames(gt_temp_topic)))[1]] = 'unknown'
              } else {
                gt_temp_topic$unknown <- NA
              }
              
              # Append valid results to main data frame
              gt_results_topic <- bind_rows(gt_results_topic,
                                            gt_temp_topic %>%
                                              select(risingSearches, keyword, value, unknown, geo) %>%
                                              mutate(
                                                keyword = topic,
                                                time = month_date,
                                                value = ifelse(value == "<1", 0.1, value),  # Replace "<1" with 0.1
                                                value = as.numeric(value),
                                                sys_time = sys_time
                                              )
              )
            }
            
            
          } # topic type rising
          
          if("query" %in% type) {
            
            ## -------- Get QUERIES -------- ##
            error_message <- NA  # Reset error message again
            
            # Try to get Google Trends data for top related search queries
            gt_temp_query <- try_gtrends_api(
              topic_keyword = topic_query,
              geo_location = FU_code[i],
              start_date = month_date,
              end_date = month_date,
              fun = "rqueries",
              api_key = your_api_key
            )
            
            # Save error if no data returned
            if("error" %in% colnames(gt_temp_query)) {
              #if((length(gt_temp_query) == 1 & all(is.na(gt_temp_query)))) {
              # error_df_temp_query <- data.frame(
              #   keyword = topic,
              #   geo = FU_code[i],
              #   time = month_date,
              #   error = error_message
              # )
              # error_df_query <- rbind(error_df_query, error_df_temp_query)
              
              error_df_query <- rbind(error_df_query, gt_temp_query)
              
            } else {
              
              if(any(is.na(colnames(gt_temp_query)))) {
                colnames(gt_temp_query)[which(is.na(colnames(gt_temp_query)))[1]] = 'unknown'
              } else {
                gt_temp_query$unknown <- NA
              }
              
              # Append valid query results to main data frame
              gt_results_query <- bind_rows(gt_results_query,
                                            gt_temp_query %>%
                                              select(risingSearches, keyword, value, unknown, geo) %>%
                                              mutate(
                                                time = month_date,
                                                value = ifelse(value == "<1", 0.1, value),
                                                value = as.numeric(value),
                                                sys_time = sys_time
                                              )
              )
            }
            
          } # # query type rising
          
        }
        
      } # i
    } # n
    
    if("topic" %in% type) {
    file_name <- paste0(dir_path, "/", metric, "_topic_", substr(t,1,4),"_",substr(t,6,7),".csv")
    print(paste0("Saving: ", file_name))
    fwrite(gt_results_topic, file_name)
    
    
    # Save topic errors (if any)
    if(nrow(error_df_topic) > 0) {
      file_name_error <- paste0(dir_path, "/", metric, "_topic_error_", substr(t,1,4),"_",substr(t,6,7),".csv")
      print(paste0("Saving: ", file_name_error))
      fwrite(error_df_topic, file_name_error)
    }
    }
    
    if("query" %in% type) {
    file_name <- paste0(dir_path, "/", metric, "_query_", substr(t,1,4),"_",substr(t,6,7),".csv")
    print(paste0("Saving: ", file_name))
    fwrite(gt_results_query, file_name)
    
    # Save topic errors (if any)
    if(nrow(error_df_query) > 0) {
      file_name_error <- paste0(dir_path, "/", metric, "_query_error_", substr(t,1,4),"_",substr(t,6,7),".csv")
      print(paste0("Saving: ", file_name_error))
      fwrite(error_df_query, file_name_error)
    }
    }
  } # t
  
  # Print total time spent
  fin = Sys.time()
  print(fin - ini)
}



