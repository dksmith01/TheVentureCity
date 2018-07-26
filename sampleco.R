### Install necessary packages 
### This section only needs to be run once per machine
### After it has been run, comment it out by highlighting all rows and typing Ctrl-Alt-C
install.packages('tidyverse')
install.packages('lubridate')
install.packages('zoo')
install.packages('ggthemes')
install.packages('scales')
install.packages('RColorBrewer')


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
c <- read.csv(filename)

### Observe the variable types and top 6 rows in the data
str(c)
head(c)

### Convert the dt field to a Date type so we can perform date math later
c$dt <- as.Date(as.character(c$dt), "%Y-%m-%d")

### Create a new column called "Month_Year" that stores the month/year of the transaction date
c$Month_Year <- as.yearmon(c$dt)


### DAU Growth Accounting
### Grouping by user and date (and Month_Year), calculate the sum of the income
dau <- c %>%
  group_by(user_id, dt, Month_Year) %>%
  summarize(inc_amt = sum(inc_amt)) %>%
  filter(inc_amt > 0)
head(dau, 20)


### MAU Growth Accounting
### Grouping by user and month, calculate the sum of the income
mau <- dau %>%
  group_by(user_id, Month_Year) %>%
  summarize(inc_amt = sum(inc_amt, na.rm = T))
head(mau, 20)


### Find the first date of activity and the month-year of that date
first_dt <- dau %>%
  group_by(user_id) %>%
  summarize(first_dt = min(dt),
            first_month = as.yearmon(first_dt))
head(first_dt, 20)


### Merge the first dates with the MAU data via a left join
### and create a new column called Next_Month_Year, which is one 
### month from the transaction's month-year
mau_decorated <- mau %>%
  left_join(first_dt, by = 'user_id') %>%
  filter(inc_amt > 0) 
mau_decorated$Next_Month_Year = as.yearmon(mau_decorated$Month_Year + 1/12)
head(mau_decorated, 20)


### Calculate all Growth Accounting metrics in one dataframe
growth_accounting <- full_join(mau_decorated, mau_decorated, suffix = c('.tm', '.lm'),
                               by = c("user_id" = "user_id", 
                                      "Month_Year" = "Next_Month_Year")) %>%
  group_by(Month_Year) %>%
  summarize(mau = n_distinct(ifelse(!is.na(inc_amt.tm), user_id, NA), na.rm = T),
            rev = sum(inc_amt.tm, na.rm = T),
            retained_users = n_distinct(ifelse(!is.na(inc_amt.tm) & !is.na(inc_amt.lm), user_id, NA), na.rm = T),
            retained_rev = sum(ifelse(!is.na(inc_amt.tm) & !is.na(inc_amt.lm), 
                                      ifelse(inc_amt.tm >= inc_amt.lm, inc_amt.lm, inc_amt.tm), NA), na.rm = T),
            new_users = n_distinct(ifelse(first_month.tm == Month_Year, user_id, NA), na.rm = T),
            new_rev = sum(ifelse(first_month.tm == Month_Year, inc_amt.tm, 0), na.rm = T),
            resurrected_users = n_distinct(ifelse(!is.na(first_month.tm) & first_month.tm != Month_Year & is.na(inc_amt.lm), user_id, NA), na.rm = T),
            resurrected_rev = sum(ifelse(!is.na(first_month.tm) & first_month.tm != Month_Year & is.na(inc_amt.lm), inc_amt.tm, NA), na.rm = T),
            churned_users = -1 * n_distinct(ifelse(is.na(inc_amt.tm), user_id, NA), na.rm = T),
            churned_rev = -1 * sum(ifelse(inc_amt.lm > 0 & (is.na(inc_amt.tm) | inc_amt.tm == 0),
                                          inc_amt.lm, 0), na.rm = T),
            expansion_rev = sum(ifelse(first_month.tm != Month_Year & !is.na(inc_amt.tm) & !is.na(inc_amt.tm) &
                                         inc_amt.tm > inc_amt.lm & inc_amt.lm > 0, 
                                       inc_amt.tm - inc_amt.lm, 0), na.rm = T),
            contraction_rev = -1 * sum(ifelse(Month_Year != first_month.tm & !is.na(inc_amt.tm) & !is.na(inc_amt.lm) &
                                                inc_amt.tm < inc_amt.lm & inc_amt.tm > 0,
                                              inc_amt.lm - inc_amt.tm, 0), na.rm = T)
  ) %>%
  mutate(mau_quick_ratio = -(new_users + resurrected_users) / churned_users,
         mau_bom = lag(mau),
         mau_retention_rate = retained_users / mau_bom,
         mrr_quick_ratio = -(new_rev + resurrected_rev + expansion_rev) / (churned_rev + contraction_rev),
         rev_lm = lag(rev),
         mrr_retention_rate = retained_rev / rev_lm
  ) %>%
  filter(mau > 0) 


### Create dataset for MAU Growth Accounting
mau_growth_accounting <- growth_accounting %>%
  select(Month_Year, mau, retained_users, new_users, resurrected_users, churned_users, mau_quick_ratio,
         mau_quick_ratio, mau_retention_rate)
write.csv(mau_growth_accounting, sprintf('%s MAU Growth Accounting.csv', company_name), row.names = F)
tail(mau_growth_accounting, 20)


### Create dataset for MRR Growth accounting
mrr_growth_accounting <- growth_accounting %>%
  select(Month_Year, rev, retained_rev, new_rev, resurrected_rev, 
         expansion_rev, contraction_rev, churned_rev, mrr_quick_ratio, mrr_retention_rate)
write.csv(mrr_growth_accounting, sprintf('%s MRR Growth Accounting.csv', company_name), row.names = F)
tail(mrr_growth_accounting, 20)

###
### STOP HERE IF YOU WISH TO DO YOUR ANALYSIS AND CHARTS IN EXCEL ###
###


### USER GROWTH ACCOUNTING

### Plot Monthly Users
ggplot(data = mau_growth_accounting,
       aes(x = as.factor(Month_Year), y = mau)) +
  geom_bar(stat = 'identity') +
  theme_bw() +
  scale_y_continuous(labels = comma) +
  geom_text(aes(label = comma(mau)), vjust = -1) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  labs(title = sprintf('%s MAUs by Month', company_name),
       x = 'Month',
       y = 'MAU'
  ) + 
  NULL



### Prep data for plotting
ga.tidy <- growth_accounting %>% gather(key = 'metric', value = 'value', -Month_Year)
tail(ga.tidy)
growth_accounting.plot <- growth_accounting
tail(growth_accounting)

### Define Color Palette
new_color <- 'grey64'
resurrected_color <- 'seagreen1'
churned_color <- 'tomato1'
expansion_color <- 'cyan1'
contraction_color <- 'tan1'

### Prepare variables to control MAU plot's appearance
head(growth_accounting.plot)
main_top_limit <- max(growth_accounting.plot$new_users + growth_accounting.plot$resurrected_users)
main_bottom_limit <- -min(growth_accounting.plot$churned_users)
main_y_limit = max(main_top_limit, main_bottom_limit) * 1.1
main_y_limits <- c(-main_y_limit, main_y_limit)

qr_top_limit <- max(growth_accounting.plot$mau_quick_ratio[which(growth_accounting.plot$mau_quick_ratio < Inf)], na.rm = T) 
sec_axis_multiplier <- main_y_limit / qr_top_limit

mau_cols <- c('resurrected_users', 'new_users', 'churned_users')
mau_col_values <- c('resurrected_users' = resurrected_color, 'new_users' = new_color, 'churned_users' = churned_color)


### Create the MAU monthly bar chart with Quick Ratio and Retention Rate
ggplot() +
  geom_bar(data = ga.tidy[ga.tidy$metric %in% mau_cols, ],
           stat= 'identity', aes(x = as.factor(Month_Year), y = value, 
                                 fill = factor(metric, levels = mau_cols))) +
  geom_hline(yintercept = sec_axis_multiplier, linetype = 'dashed') + 
  geom_line(data = ga.tidy[ga.tidy$metric %in% c('mau_retention_rate'), ],
            stat= 'summary', fun.y = sum, color = 'yellow', size = 1.5,
            aes(x = as.factor(Month_Year), y = value*sec_axis_multiplier, group = 1)) +
  geom_label(data = ga.tidy[ga.tidy$metric %in% c('mau_retention_rate') & !is.na(ga.tidy$value), ], 
             fill = 'yellow',
             aes(x = as.factor(Month_Year), y = value*sec_axis_multiplier, label = sprintf('%.0f%%', value*100))) +
  geom_line(data = ga.tidy[ga.tidy$metric %in% c('mau_quick_ratio'), ],
            stat= 'summary', fun.y = sum, color = 'blue', size = 1.5,
            aes(x = as.factor(Month_Year), y = value*sec_axis_multiplier, group = 1)) +
  geom_label(data = ga.tidy[ga.tidy$metric %in% c('mau_quick_ratio') & ga.tidy$value < Inf, ], 
             color = 'white', fill = 'blue',
             aes(x = as.factor(Month_Year), y = value*sec_axis_multiplier, label = sprintf('%.1f', value))) +
  scale_y_continuous(labels = comma, limits = main_y_limits, 
                     sec.axis = sec_axis(~.*1/sec_axis_multiplier, name = 'Quick Ratio & Retention Rate')) +
  scale_fill_manual('User Types', values = mau_col_values) +
  theme_bw() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1),
        panel.grid.major = element_blank(), 
        panel.grid.minor = element_blank()) +
  labs(title = sprintf('%s MAU Growth Accounting', company_name),
       x = 'Month',
       y = 'MAU'
  ) + 
  NULL


### REVENUE GROWTH ACCOUNTING

### Plot Monthly Revenue
ggplot(data = mrr_growth_accounting,
       aes(x = as.factor(Month_Year), y = rev)) +
  geom_bar(stat = 'identity') +
  geom_text(aes(label = dollar(rev)), vjust = -1) +
  theme_bw() +
  scale_y_continuous(labels = dollar) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  labs(title = sprintf('%s Revenue by Month', company_name),
       x = 'Month',
       y = 'Revenue'
  ) + 
  NULL


### ### Prepare variables to control MRR plot's appearance
head(growth_accounting.plot)
main_top_limit <- max(growth_accounting.plot$new_rev + growth_accounting.plot$resurrected_rev + growth_accounting.plot$expansion_rev)
main_bottom_limit <- -min(growth_accounting.plot$churned_rev + growth_accounting.plot$contraction_rev)
main_y_limit = max(main_top_limit, main_bottom_limit) * 1.1
main_y_limits <- c(-main_y_limit, main_y_limit)

qr_top_limit <- max(growth_accounting.plot$mrr_quick_ratio[which(growth_accounting.plot$mrr_quick_ratio < Inf)], na.rm = T) 
sec_axis_multiplier <- main_y_limit / qr_top_limit

mrr_cols <- c('expansion_rev', 'resurrected_rev', 'new_rev', 'contraction_rev', 'churned_rev')
mrr_col_values <- c('expansion_rev' = expansion_color, 'resurrected_rev' = resurrected_color, 'new_rev' = new_color, 
                    'contraction_rev' = contraction_color, 'churned_rev' = churned_color)


### Create the MRR monthly bar chart with Quick Ratio and Retention Rate
ggplot() +
  geom_bar(data = ga.tidy[ga.tidy$metric %in% mrr_cols, ],
           stat= 'identity', aes(x = as.factor(Month_Year), y = value, 
                                 fill = factor(metric, levels = mrr_cols))) +
  geom_hline(yintercept = sec_axis_multiplier, linetype = 'dashed') + 
  geom_line(data = ga.tidy[ga.tidy$metric %in% c('mrr_retention_rate'), ],
            stat= 'summary', fun.y = sum, color = 'yellow', size = 1.5,
            aes(x = as.factor(Month_Year), y = value*sec_axis_multiplier, group = 1)) +
  geom_label(data = ga.tidy[ga.tidy$metric %in% c('mrr_retention_rate') & !is.na(ga.tidy$value), ], 
             fill = 'yellow',
             aes(x = as.factor(Month_Year), y = value*sec_axis_multiplier, label = sprintf('%.0f%%', value*100))) +
  geom_line(data = ga.tidy[ga.tidy$metric %in% c('mrr_quick_ratio'), ],
            stat= 'summary', fun.y = sum, color = 'blue', size = 1.5,
            aes(x = as.factor(Month_Year), y = value*sec_axis_multiplier, group = 1)) +
  geom_label(data = ga.tidy[ga.tidy$metric %in% c('mrr_quick_ratio') & ga.tidy$value < Inf, ], 
             color = 'white', fill = 'blue',
             aes(x = as.factor(Month_Year), y = value*sec_axis_multiplier, label = sprintf('%.1f', value))) +
  scale_y_continuous(labels = dollar, limits = main_y_limits, 
                     sec.axis = sec_axis(~.*1/sec_axis_multiplier, name = 'Quick Ratio')) +
  scale_fill_manual('Revenue Types', values = mrr_col_values) +
  theme_bw() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1),
        panel.grid.major = element_blank(), 
        panel.grid.minor = element_blank()) +
  # guides(fill = guide_legend(title="Metrics")) +
  labs(title = sprintf('%s MRR Growth Accounting', company_name),
       x = 'Month',
       y = 'Revenue'
  ) + 
  NULL


