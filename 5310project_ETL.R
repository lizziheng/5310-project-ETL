#dataset inspection

##basic information dataset
df_category_table <- read.csv('Downloads/5310_project_data/basic_info_tables/category_table.csv')
df_company_table <- read.csv('Downloads/5310_project_data/basic_info_tables/company_table.csv')
df_keyword_table <- read.csv('Downloads/5310_project_data/basic_info_tables/keyword_table.csv')
df_platform_table <- read.csv('Downloads/5310_project_data/basic_info_tables/platform_table.csv')
df_product_table <- read.csv('Downloads/5310_project_data/basic_info_tables/product_table.csv')
df_review_table <- read.csv('Downloads/5310_project_data/basic_info_tables/review_table.csv')

##bridge dataset
df_company_product_table <- read.csv('Downloads/5310_project_data/bridge_tables/company_product_table.csv')
df_product_category_table <- read.csv('Downloads/5310_project_data/bridge_tables/product_category_table.csv')

##contract dataset
df_category_contract_table <- read.csv('Downloads/5310_project_data/contract_tables/category_contract_table.csv')
df_company_contract_table <- read.csv('Downloads/5310_project_data/contract_tables/company_contract_table.csv')
df_keyword_contract_table <- read.csv('Downloads/5310_project_data/contract_tables/keyword_contract_table.csv')
df_price_contract_table <- read.csv('Downloads/5310_project_data/contract_tables/price_contract_table.csv')

##monitor dataset
df_keyword_monitor_table <- read.csv('Downloads/5310_project_data/monitor_tables/keyword_monitor_table.csv')
df_price_monitor_table <- read.csv('Downloads/5310_project_data/monitor_tables/price_monitor_table.csv')
df_sentiment_monitor_table <- read.csv('Downloads/5310_project_data/monitor_tables/sentiment_monitor_table.csv')

##summary dataset
df_category_keyword_summary_table <- read.csv('Downloads/5310_project_data/summary_tables/category_keyword_summary_table.csv')
df_category_sentiment_summary_table <- read.csv('Downloads/5310_project_data/summary_tables/category_sentiment_summary_table.csv')
df_product_keyword_summary_table <- read.csv('Downloads/5310_project_data/summary_tables/product_keyword_summary_table.csv')
df_product_sentiment_summary_table <- read.csv('Downloads/5310_project_data/summary_tables/product_sentiment_summary_table.csv')


#Import packages
library('RPostgreSQL')
library("DBI")
library("remotes")


# connect to database
con <- DBI::dbConnect(
  RPostgres::Postgres(),
  dbname = 'project', 
  host = 'localhost', 
  port = 5432,
  user = 'postgres', 
  password = '123'
)


#create tables

##platform_table
stmt_platform_table <- '
CREATE TABLE platform_table
(
 platform_id   varchar(20),
 platform_name varchar(50) NOT NULL,
 PRIMARY KEY ( platform_id )
);'

dbGetQuery(con, stmt_platform_table)

##keyword_table
stmt_keyword_table <- '
CREATE TABLE keyword_table
(
 keyword_id varchar(20),
 word       varchar(50) NOT NULL,
 PRIMARY KEY ( keyword_id )
);'

dbGetQuery(con, stmt_keyword_table)  

##company_table
stmt_company_table <- '
CREATE TABLE company_table
(
 company_id   varchar(20),
 company_name varchar(50) NOT NULL,
 PRIMARY KEY ( company_id )
);'

dbGetQuery(con, stmt_company_table)  

##company_contract_table
stmt_company_contract_table <- '
CREATE TABLE company_contract_table
(
 company_contract_id varchar(50),
 monitor_company_id  varchar(50) NOT NULL,
 client_company_id   varchar(50) NOT NULL,
 monitor_start_date  date NOT NULL,
 monitor_end_date    date NOT NULL,
 PRIMARY KEY ( company_contract_id )
);'

dbGetQuery(con, stmt_company_contract_table)

##category_table
stmt_category_table <- '
CREATE TABLE category_table
(
 category_id   varchar(20),
 category_name varchar(50) NOT NULL,
 PRIMARY KEY ( category_id )
);'

dbGetQuery(con, stmt_category_table)

##product_table
stmt_product_table <- '
CREATE TABLE product_table
(
 product_id   varchar(20),
 product_name varchar(255) NOT NULL,
 PRIMARY KEY ( product_id )
);'

dbGetQuery(con, stmt_product_table)

##product_keyword_summary_table
stmt_product_keyword_summary_table <- '
CREATE TABLE product_keyword_summary_table
(
 product_id varchar(20),
 keyword_id varchar(20) NOT NULL,
 frequency  integer NOT NULL,
 PRIMARY KEY ( product_id, keyword_id ),
 FOREIGN KEY ( product_id ) REFERENCES product_table ( product_id ),
 FOREIGN KEY ( keyword_id ) REFERENCES keyword_table ( keyword_id )
);'

dbGetQuery(con, stmt_product_keyword_summary_table)

##keyword_monitor_table
stmt_keyword_monitor_table <- '
CREATE TABLE keyword_monitor_table
(
 keyword_monitor_id varchar(50),
 product_id         varchar(20) NOT NULL,
 keyword_id         varchar(20) NOT NULL,
 frequency         integer NOT NULL,
 keyword_monitor_date date NOT NULL,
 PRIMARY KEY ( keyword_monitor_id ),
 FOREIGN KEY ( keyword_id ) REFERENCES keyword_table ( keyword_id ),
 FOREIGN KEY ( product_id ) REFERENCES product_table ( product_id )
);'

dbGetQuery(con, stmt_keyword_monitor_table)

##category_sentiment_summary_table
stmt_category_sentiment_summary_table <- '
CREATE TABLE category_sentiment_summary_table
(
 category_summary_id varchar(50),
 anger               integer NULL,
 category_id         varchar(20) NOT NULL,
 anticipation        integer NULL,
 disgust             integer NULL,
 fear                integer NULL,
 joy                 integer NULL,
 sadness             integer NULL,
 surprise            integer NULL,
 trust               integer NULL,
 negative            integer NULL,
 positive            integer NULL,
 PRIMARY KEY ( category_summary_id ),
 FOREIGN KEY ( category_id ) REFERENCES category_table ( category_id )
);'

dbGetQuery(con, stmt_category_sentiment_summary_table)
  
##category_keyword_summary_table
stmt_category_keyword_summary_table <- '
CREATE TABLE category_keyword_summary_table
(
 category_id varchar(50),
 keyword_id  varchar(20) NOT NULL,
 frequency   integer NOT NULL,
 PRIMARY KEY ( category_id, keyword_id ),
 FOREIGN KEY ( category_id ) REFERENCES category_table ( category_id ),
 FOREIGN KEY ( keyword_id ) REFERENCES keyword_table ( keyword_id )
);'
  
dbGetQuery(con, stmt_category_keyword_summary_table)
  
##category_contract_table
stmt_category_contract_table <- '
CREATE TABLE category_contract_table
(
 category_contract_id varchar(50),
 category_id          varchar(20) NOT NULL,
 company_id           varchar(20) NOT NULL,
 monitor_start_date   date NOT NULL,
 monitor_end_date     date NOT NULL,
 PRIMARY KEY ( category_contract_id ),
 FOREIGN KEY ( category_id ) REFERENCES category_table ( category_id ),
 FOREIGN KEY ( company_id ) REFERENCES company_table ( company_id )
);'

dbGetQuery(con, stmt_category_contract_table)

##sentiment_monitor_table
stmt_sentiment_monitor_table <- '
CREATE TABLE sentiment_monitor_table
(
 sentiment_monitor_id   varchar(50),
 "date"                 date NOT NULL,
 anger          integer NULL,
 anticipation   integer NULL,
 disgust        integer NULL,
 fear           integer NULL,
 joy            integer NULL,
 sadness        integer NULL,
 surprise       integer NULL,
 trust          integer NULL,
 negative       integer NULL,
 positive       integer NULL,
 product_id           varchar(20) NOT NULL,
 PRIMARY KEY ( sentiment_monitor_id ),
 FOREIGN KEY ( product_id ) REFERENCES product_table ( product_id )
);'

dbGetQuery(con, stmt_sentiment_monitor_table)

##review_table
stmt_review_table <- '
CREATE TABLE review_table
(
 review_id      varchar(50),
 product_id     varchar(20) NOT NULL,
 platform_id    varchar(20),
 company_id     varchar(20) NOT NULL,
 reviewer       varchar(50),
 review_content text NOT NULL,
 review_date    date NOT NULL,
 anger          integer NULL,
 anticipation   integer NULL,
 disgust        integer NULL,
 fear           integer NULL,
 joy            integer NULL,
 sadness        integer NULL,
 surprise       integer NULL,
 trust          integer NULL,
 negative       integer NULL,
 positive       integer NULL,
 rating         numeric,
 PRIMARY KEY ( review_id ),
 FOREIGN KEY ( product_id ) REFERENCES product_table ( product_id ),
 FOREIGN KEY ( company_id ) REFERENCES company_table ( company_id ),
 FOREIGN KEY ( platform_id ) REFERENCES platform_table ( platform_id )
);
'
dbGetQuery(con, stmt_review_table)

##product_sentiment_summary_table
stmt_product_sentiment_summary_table <- '
CREATE TABLE product_sentiment_summary_table
(
 product_summary_id varchar(50),
 product_id         varchar(20) NOT NULL,
 anger              integer NULL,
 anticipation       integer NULL,
 disgust            integer NULL,
 fear               integer NULL,
 joy                integer NULL,
 sadness            integer NULL,
 surprise           integer NULL,
 trust              integer NULL,
 negative           integer NULL,
 positive           integer NULL,
 PRIMARY KEY ( product_summary_id ),
 FOREIGN KEY ( product_id ) REFERENCES product_table ( product_id )
);'

dbGetQuery(con, stmt_product_sentiment_summary_table)

##price_monitor_table
stmt_price_monitor_table <-'
CREATE TABLE price_monitor_table
(
 product_price_id varchar(50),
 price            numeric(8,2) NOT NULL,
 product_id       varchar(20) NOT NULL,
 "date"             date NOT NULL,
 PRIMARY KEY ( product_price_id ),
 FOREIGN KEY ( product_id ) REFERENCES product_table ( product_id )
);'

dbGetQuery(con, stmt_price_monitor_table)

##price_contract_table
stmt_price_contract_table <- '
CREATE TABLE price_contract_table
(
 price_contract_id  varchar(50),
 company_id         varchar(20) NOT NULL,
 product_id         varchar(20) NOT NULL,
 monitor_start_date date NOT NULL,
 monitor_end_date   date NOT NULL,
 PRIMARY KEY ( price_contract_id ),
 FOREIGN KEY ( product_id ) REFERENCES product_table ( product_id ),
 FOREIGN KEY ( company_id ) REFERENCES company_table ( company_id )
);'

dbGetQuery(con, stmt_price_contract_table)

##keyword_contract_table
stmt_keyword_contract_table <- '
CREATE TABLE keyword_contract_table
(
 keyword_contract_id varchar(20),
 keyword_id          varchar(20) NOT NULL,
 company_id          varchar(20) NOT NULL,
 product_id          varchar(20) NOT NULL,
 monitor_start_date  date NOT NULL,
 monitor_end_date    date NOT NULL,
 PRIMARY KEY ( keyword_contract_id ),
 FOREIGN KEY ( keyword_id ) REFERENCES keyword_table ( keyword_id ),
 FOREIGN KEY ( product_id ) REFERENCES product_table ( product_id ),
 FOREIGN KEY ( company_id ) REFERENCES company_table ( company_id )
);'

dbGetQuery(con, stmt_keyword_contract_table)

##company_product_table
stmt_company_product_table <- '
CREATE TABLE company_product_table
(
 company_id varchar(20),
 product_id varchar(20) NOT NULL,
 FOREIGN KEY ( company_id ) REFERENCES company_table ( company_id ),
 FOREIGN KEY ( product_id ) REFERENCES product_table ( product_id )
);
'
dbGetQuery(con, stmt_company_product_table)

##product_category_table
stmt_product_category_table <- '
CREATE TABLE product_category_table
(
 product_id varchar(20) NOT NULL,
 category_id varchar(20) NOT NULL,
 FOREIGN KEY ( product_id ) REFERENCES product_table ( product_id ),
 FOREIGN KEY ( category_id ) REFERENCES category_table ( category_id )
);'

dbGetQuery(con, stmt_product_category_table)


#ETL

##platform_table ETL
head(df_platform_table)
##check whether PK has duplicated values
which(duplicated(df_platform_table$platform_id))
##No duplicated value! It's 3NF already so do ETL
dbWriteTable(con, name = 'platform_table', value = df_platform_table, row.names = FALSE, append = TRUE)

##keyword_table ETL
head(df_keyword_table)
which(duplicated(df_keyword_table$keyword_id))
dbWriteTable(con, name = 'keyword_table', value = df_keyword_table, row.names = FALSE, append = TRUE)

##company_table ETL
head(df_company_table)
which(duplicated(df_company_table$company_id))
dbWriteTable(con, name = 'company_table', value = df_company_table, row.names = FALSE, append = TRUE)

##company_contract_table ETL
head(df_company_contract_table)
which(duplicated(df_company_contract_table$company_contract_id))
dbWriteTable(con, name = 'company_contract_table', value = df_company_contract_table, row.names = FALSE, append = TRUE)

##category_table ETL
head(df_category_table)
which(duplicated(df_category_table$category_id))
dbWriteTable(con, name = 'category_table', value = df_category_table, row.names = FALSE, append = TRUE)

##product_table ETL
head(df_product_table)
which(duplicated(df_product_table$product_id))
dbWriteTable(con, name = 'product_table', value = df_product_table, row.names = FALSE, append = TRUE)

##keyword_monitor_table ETL
head(df_keyword_monitor_table)
which(duplicated(df_keyword_monitor_table$keyword_monitor_id))
dbWriteTable(con, name = 'keyword_monitor_table', value = df_keyword_monitor_table, row.names = FALSE, append = TRUE)


##category_contract_table ETL
head(df_category_contract_table)
which(duplicated(df_category_contract_table$category_contract_id))
dbWriteTable(con, name = 'category_contract_table', value = df_category_contract_table, row.names = FALSE, append = TRUE)

##sentiment_monitor_table ETL
head(df_sentiment_monitor_table)
which(duplicated(df_sentiment_monitor_table$sentiment_monitor_id))
dbWriteTable(con, name = 'sentiment_monitor_table', value = df_sentiment_monitor_table, row.names = FALSE, append = TRUE)

##review_table ETL
head(df_review_table)
which(duplicated(df_review_table$review_id))
dbWriteTable(con, name = 'review_table', value = df_review_table, row.names = FALSE, append = TRUE)

##price_monitor_table ETL
head(df_price_monitor_table)
which(duplicated(df_price_monitor_table$product_price_id))
dbWriteTable(con, name = 'price_monitor_table', value = df_price_monitor_table, row.names = FALSE, append = TRUE)

##price_contract_table ETL
head(df_price_contract_table)
###the price_contract_id has dpulicate values, which means it can't be Primary Key, so we should create a new price_contract_id
df_price_contract_table$price_contract_id <- 1:nrow(df_price_contract_table)
head(df_price_contract_table)
###it looks good now so load it to database
dbWriteTable(con, name = 'price_contract_table', value = df_price_contract_table, row.names = FALSE, append = TRUE)

##keyword_contract_table ETL
head(df_keyword_contract_table)
which(duplicated(df_keyword_contract_table$keyword_contract_id))
dbWriteTable(con, name = 'keyword_contract_table', value = df_keyword_contract_table, row.names = FALSE, append = TRUE)

##company_product_table ETL
head(df_company_product_table)
###the column name doesn't match, so rename them.
colnames(df_company_product_table) <- c('product_id', 'company_id')
head(df_company_product_table)
dbWriteTable(con, name = 'company_product_table', value = df_company_product_table, row.names = FALSE, append = TRUE)

##product_category_table ETL
head(df_product_category_table)
###rename columns
colnames(df_product_category_table) <- c('product_id', 'category_id')
head(df_product_category_table)
dbWriteTable(con, name = 'product_category_table', value = df_product_category_table, row.names = FALSE, append = TRUE)

##product_keyword_summary_table ETL
head(df_product_keyword_summary_table)
###There is only 1 row in this dataset and I'm not sure whether this row of data is correct, so we should delete it and extract frequency from the main dataset(keyword_monitor_table)
library(dplyr)
df_product_keyword_summary_table <- df_product_keyword_summary_table %>%  filter(!row_number() %in% c(1))
head(df_product_keyword_summary_table)
summary(df_keyword_monitor_table)
df_keyword_monitor_table[df_keyword_monitor_table$product_id %in% c('1440'),]
###Count the total frequency of each group by product_id and keyword_id
temp_df_keyword_monitor_table <- df_keyword_monitor_table[c('product_id', 'keyword_id', 'frequency')]
temp_df_keyword_monitor_table[temp_df_keyword_monitor_table$product_id %in% c('1440'),]
temp_df_product_keyword_summary_table <- temp_df_keyword_monitor_table %>% group_by(product_id, keyword_id) %>% mutate(frequency_by_group = sum(frequency))
temp_df_product_keyword_summary_table[temp_df_product_keyword_summary_table$product_id %in% c('1440'),]
###It looks good now, delete the previous frequency column, remove duplicated rows and load it to database
temp_df_product_keyword_summary_table = subset(temp_df_product_keyword_summary_table, select = -c(frequency))
temp_df_product_keyword_summary_table <- temp_df_product_keyword_summary_table[!duplicated(temp_df_product_keyword_summary_table[c('product_id','keyword_id', 'frequency_by_group')]),]
names(temp_df_product_keyword_summary_table)[3] <- 'frequency'
temp_df_product_keyword_summary_table[temp_df_product_keyword_summary_table$product_id %in% c('1440'),]

dbWriteTable(con, name = 'product_keyword_summary_table', value = temp_df_product_keyword_summary_table, row.names = FALSE, append = TRUE)


##category_keyword_summary_table ETL
head(df_category_keyword_summary_table)
df_category_keyword_summary_table <- df_category_keyword_summary_table %>%  filter(!row_number() %in% c(1))
###a single category can have different products, since I've already had the product_keyword_summary_table, I should use left join function by dplyr
df_category_keyword_summary_table <- left_join(temp_df_product_keyword_summary_table, df_product_category_table)
df_category_keyword_summary_table
###it looks good, so now delete product_id column, calculate the sum and remove duplicated rows
df_category_keyword_summary_table <- subset(df_category_keyword_summary_table, select = -c(product_id))
df_category_keyword_summary_table <- df_category_keyword_summary_table %>% group_by(category_id, keyword_id) %>% mutate(frequency_by_group = sum(frequency))
df_category_keyword_summary_table <- subset(df_category_keyword_summary_table, select = -c(frequency))
df_category_keyword_summary_table <- df_category_keyword_summary_table[!duplicated(df_category_keyword_summary_table[c('category_id','keyword_id', 'frequency_by_group')]),]
names(df_category_keyword_summary_table)[3] <- 'frequency'
###final check
df_category_keyword_summary_table[df_category_keyword_summary_table$category_id %in% c('1'),]

dbWriteTable(con, name = 'category_keyword_summary_table', value = df_category_keyword_summary_table, row.names = FALSE, append = TRUE)


##product_sentiment_summary_table ETL
head(df_product_sentiment_summary_table)
df_product_sentiment_summary_table <- df_product_sentiment_summary_table %>%  filter(!row_number() %in% c(1))
head(df_sentiment_monitor_table)
temp_df_product_sentiment_summary_table <- df_sentiment_monitor_table[c('product_id', 'anger' , 'anticipation' , 'disgust' , 'fear' , 'joy' ,  'sadness' , 'surprise' , 'trust' , 'negative' , 'positive')]
head(temp_df_product_sentiment_summary_table)
###As we can see, there is many null values in those sentiment columns, so we should drop null values then find median value.
temp_df_product_sentiment_summary_table <- temp_df_product_sentiment_summary_table %>% 
  group_by(product_id) %>% 
  mutate(anger_by_group = median(anger, na.rm = TRUE),
         anticipation_by_group = median(anticipation, na.rm = TRUE),
         disgust_by_group = median(disgust, na.rm = TRUE),
         fear_by_group = median(fear, na.rm = TRUE),
         joy_by_group = median(joy, na.rm = TRUE),
         sadness_by_group = median(sadness, na.rm = TRUE),
         surprise_by_group = median(surprise, na.rm = TRUE),
         trust_by_group = median(trust, na.rm = TRUE),
         negative_by_group = median(negative, na.rm = TRUE),
         positive_by_group = median(positive, na.rm = TRUE))
temp_df_product_sentiment_summary_table <- subset(temp_df_product_sentiment_summary_table, select = -c(anger, anticipation, disgust, fear, joy, sadness, surprise, trust, negative, positive))
temp_df_product_sentiment_summary_table <- temp_df_product_sentiment_summary_table[!duplicated(temp_df_product_sentiment_summary_table[c('product_id')]),]
summary(temp_df_product_sentiment_summary_table)
###Looks good now, so add product_summary_id, change the columns' names and load it to database.
colnames(temp_df_product_sentiment_summary_table) <- c('product_id', 'anger' , 'anticipation' , 'disgust' , 'fear' , 'joy' ,  'sadness' , 'surprise' , 'trust' , 'negative' , 'positive')
temp_df_product_sentiment_summary_table$product_summary_id <- 1:nrow(temp_df_product_sentiment_summary_table)

dbWriteTable(con, name = 'product_sentiment_summary_table', value = temp_df_product_sentiment_summary_table, row.names = FALSE, append = TRUE)


##category_sentiment_summary_table ETL
head(df_category_sentiment_summary_table)
df_category_sentiment_summary_table <- df_category_sentiment_summary_table %>%  filter(!row_number() %in% c(1))
###similar procedures as category_keyword_summary table ETL
temp_df_category_sentiment_summary_table <- left_join(temp_df_product_sentiment_summary_table, df_product_category_table)
temp_df_category_sentiment_summary_table
temp_df_category_sentiment_summary_table <- subset(temp_df_category_sentiment_summary_table, select = -c(product_id, product_summary_id))
temp_df_category_sentiment_summary_table <- temp_df_category_sentiment_summary_table %>% 
  group_by(category_id) %>% 
  mutate(anger_by_group = median(anger, na.rm = TRUE),
         anticipation_by_group = median(anticipation, na.rm = TRUE),
         disgust_by_group = median(disgust, na.rm = TRUE),
         fear_by_group = median(fear, na.rm = TRUE),
         joy_by_group = median(joy, na.rm = TRUE),
         sadness_by_group = median(sadness, na.rm = TRUE),
         surprise_by_group = median(surprise, na.rm = TRUE),
         trust_by_group = median(trust, na.rm = TRUE),
         negative_by_group = median(negative, na.rm = TRUE),
         positive_by_group = median(positive, na.rm = TRUE))
temp_df_category_sentiment_summary_table <- subset(temp_df_category_sentiment_summary_table, select = -c(anger, anticipation, disgust, fear, joy, sadness, surprise, trust, negative, positive))
temp_df_category_sentiment_summary_table <- temp_df_category_sentiment_summary_table[!duplicated(temp_df_category_sentiment_summary_table[c('category_id')]),]
temp_df_category_sentiment_summary_table$category_summary_id <- 1:nrow(temp_df_category_sentiment_summary_table)
colnames(temp_df_category_sentiment_summary_table) <- c('category_id', 'anger' , 'anticipation' , 'disgust' , 'fear' , 'joy' ,  'sadness' , 'surprise' , 'trust' , 'negative' , 'positive', 'category_summary_id')

dbWriteTable(con, name = 'category_sentiment_summary_table', value = temp_df_category_sentiment_summary_table, row.names = FALSE, append = TRUE)


##Spot Checks & Validation¶
###Except 4 summary data, the rest 15 dataset are loaded in the original form because I've already check the PK's duplicate in the ETL progress, so we only need to check the 4 summary datasets

###product_keyword_summary_table check
head(df_keyword_monitor_table)
summary(df_keyword_monitor_table)
####Let's check the total frequency of product_id=1440 and keyword_id=1
check_df_keyword_monitor_table <- df_keyword_monitor_table[c('product_id', 'keyword_id', "frequency")]
check_df_keyword_monitor_table %>%
  filter(product_id == 1440, keyword_id == 1) %>%
  group_by(product_id, keyword_id) %>%
  mutate(group_frequency = sum(frequency))


stmt_check_product_keyword_summary_table <- "
select * from product_keyword_summary_table 
where product_id = '1440' and keyword_id = '1';
"

dbGetQuery(con, stmt_check_product_keyword_summary_table)
####the frequency are both 92, result match!


###category_keyword_summary_table check
head(df_category_keyword_summary_table)
####Let's check the frequency of category_id=1 and keyword_id=1
stmt_check_category_keyword_summary_table <- "
select frequency from category_keyword_summary_table
where category_id = '1' and keyword_id='1';
"
dbGetQuery(con, stmt_check_category_keyword_summary_table)
####the frequency are both 2114, result match!



###product_sentiment_summary_table check
head(temp_df_product_sentiment_summary_table)
####Let's check the anger sentiment when product_id=1
stmt_check_product_sentiment_summary_table <- "
select anger from product_sentiment_summary_table
where product_id = '1';
"
dbGetQuery(con, stmt_check_product_sentiment_summary_table)
####result match


###category_sentiment_summary_table check
head(temp_df_category_sentiment_summary_table)
####check anger sentiment when category_id=1
stmt_check_category_sentiment_summary_table <- "
select anger from category_sentiment_summary_table
where category_id = '1';
"
dbGetQuery(con, stmt_check_category_sentiment_summary_table)
####result match
#END