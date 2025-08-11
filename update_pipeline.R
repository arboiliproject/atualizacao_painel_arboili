if (!requireNamespace("read.dbc", quietly = TRUE)) devtools::install_github("danicat/read.dbc")
if (!requireNamespace("microdatasus", quietly = TRUE)) remotes::install_github("rfsaldanha/microdatasus", force = TRUE)
# Install 'gtrendsAPI' from GitHub if not already installed
if (!requireNamespace("gtrendsAPI", quietly = TRUE)) 
  devtools::install_github("racorreia/gtrendsAPI", build_vignettes = TRUE)

library(microdatasus)
library(tidyverse)
library(data.table)
library(lubridate)
library(rvest)
library(stringr)
library(purrr)
library(gtrendsAPI)

# Load source custom functions from an external R script
source("functions/pipeline_functions.R")
source("functions/fun.R")
source("functions/getGraph2.R")
source("functions/getTopTopics2.R")
source("functions/getTopQueries2.R")
source("functions/getRisingTopics2.R")
source("functions/getRisingQueries2.R")

# Extrair e atualizar base de dados
update.disease.notification(fonte = "dengue", ano = "recente")
update.disease.notification(fonte = "chikungunya", ano = "recente")

all_sari_links_csv_total <- get_all_srag_links()
update.disease.notification(fonte = "srag", ano = "recente", sivep_links = all_sari_links_csv_total)

# Transformar os dados
source("2_1_transform_sivep_data.R")
source("2_transform_sinan_data.R")
source("4_prepare_final_disease_table.R")

# Extrair os dados do GoogleTrends

your_api_key = "..." # Fornecer chave de API para acesso ao GoogleTrends

extract.gt(your_api_key = your_api_key)

# Define time range (monthly) from Jan 2020 to 30 days before current date
current_month = floor_date(Sys.Date(), unit = "month")
time_seq <- format(c(current_month-1, current_month), format = "%Y-%m")

# Extrair top topics
extract.gt.related(type = "topic", metric = "top", time_seq, your_api_key = your_api_key)

# Extrair top queries
extract.gt.related(type = "query", metric = "top", time_seq, your_api_key = your_api_key)

# Extrair rising queries
extract.gt.related(type = "topic", metric = "rising", time_seq, your_api_key = your_api_key)

# Extrair rising queries
extract.gt.related(type = "query", metric = "rising", time_seq, your_api_key = your_api_key)
