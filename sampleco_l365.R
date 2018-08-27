### Install necessary packages 
### This section only needs to be run once per machine
### After it has been run, comment it out by highlighting all rows and typing Ctrl-Alt-C
# install.packages('tidyverse')
# install.packages('lubridate')
# install.packages('zoo')
# install.packages('ggthemes')
# install.packages('scales')
# install.packages('RColorBrewer')


### Load necessary libraries (must be run with every new instance of the R kernel)
library(dplyr)
library(tidyr)
library(lubridate)
library(zoo)
library(ggplot2)
library(ggthemes)
library(scales)
library(RColorBrewer)


### The company's name is "SampleCo"
company_name <- 'SampleCo'

### All files in this code will be read from and written to the "wd" working directory
### In this sample, the working directory is C:\Users\dksmi\Dropbox (TheVentureCity)\David\SampleCo
### Please change this to refer to the folder that you would like to use
### Be sure to use double back-slashes to designate your folder ("\\")
wd <- paste0('C:\\Users\\dksmi\\Dropbox (TheVentureCity)\\David\\', company_name)
setwd(wd)

### Before running this step, make sure the sample_transactions.csv file is in the 
### folder designated in the previous step
### This step reads the contents of the csv file into a data.frame called "c"
filename <- 'sample_transactions.csv'
t <- read.csv(filename)

### Observe the variable types and top 6 rows in the data
str(t)
head(t)

### Convert the dt field to a Date type so we can perform date math later
t$dt <- as.Date(as.character(t$dt), "%Y-%m-%d")

### Create a new column called "Month_Year" that stores the month/year of the transaction date
t$Month_Year <- as.yearmon(t$dt)


### DAU Growth Accounting
### Grouping by user and date (and Month_Year), calculate the sum of the income
dau <- t %>%
  group_by(user_id, dt, Month_Year) %>%
  summarize(inc_amt = sum(inc_amt)) %>%
  filter(inc_amt > 0) %>%
  data.frame()
head(dau, 20)

dau %>%
  summarize(dt2 = min(dt + 365))

date.l365 <- dau %>% filter(dt > min(dt + 365)) %>% data.frame()
min(dau$dt)
max(dau$dt)
head(date.l365)
Xd365s <- data.frame()
for(max_dt in unique(date.l365$dt)) {
  min_dt <- max_dt - 365
  l365.tmp <- dau[dau$dt > min_dt & t$dt <= max_dt, ] %>%
    group_by(user_id) %>%
    summarize(days_active = n(),
              inc_amt = sum(inc_amt)) %>% 
    ungroup() %>%
    mutate(t.2d365s = (days_active >= 2),
           t.4d365s = (days_active >= 4),
           t.2d365s_inc_amt = t.2d365s * inc_amt,
           t.4d365s_inc_amt = t.4d365s * inc_amt) %>%
    summarize(t.1d365s = n_distinct(user_id),
              t.2d365s = sum(t.2d365s),
              t.4d365s = sum(t.4d365s),
              t.1d365s_inc_amt = sum(inc_amt),
              t.2d365s_per_yau = t.2d365s / t.1d365s,
              t.4d365s_per_yau = t.4d365s / t.1d365s,
              dau_yau_ratio = (sum(days_active)/365) / n_distinct(user_id),
              t.1d365s_inc_per_cust = t.1d365s_inc_amt / n_distinct(user_id),
              t.2d365s_inc_per_cust = sum(t.2d365s_inc_amt) / t.2d365s,
              t.4d365s_inc_per_cust = sum(t.4d365s_inc_amt) / t.4d365s) %>%
    mutate(annual.freq = dau_yau_ratio * 365,
           end_dt = as.Date(max_dt)) %>%
    data.frame()
  Xd365s <- rbind(Xd365s, l365.tmp) %>% data.frame()
}
head(Xd365s)

ggplot(data = Xd365s, aes(x = end_dt, y = annual.freq)) +
  geom_line(size = 1.2) +
  scale_y_continuous(limits = c(0, NA)) +
  NULL

