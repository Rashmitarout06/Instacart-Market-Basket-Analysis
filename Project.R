library(data.table)
library(dplyr)
library(tidyr)

##Importing the data
path <- "C:/Users/Rashmita Rout/Desktop/Data Mining/Project/DataSet"

aisles <- fread(file.path(path, "aisles.csv"))
departments <- fread(file.path(path, "departments.csv"))
order_prior <- fread(file.path(path, "order_products__prior.csv"))
order_train <- fread(file.path(path, "order_products__train.csv"))
orders <- fread(file.path(path, "orders.csv"))
products <- fread(file.path(path, "products.csv"))

#***_______________________________DATA PREPARATION________________________________***#

##viewing and reshaping the data
#aisles data
head(aisles)
str(aisles)
aisles$aisle <- as.factor(aisles$aisle)
#departments data
head(departments)
str(departments)
departments$department <- as.factor(departments$department)
#Prior data
head(order_prior)
str(order_prior)
#train data
head(order_train)
str(order_train)
#orders data
head(orders)
str(orders)
orders$eval_set <- as.factor(orders$eval_set)
#products data
head(products)
str(products)
products$product_name <- as.factor(products$product_name)

#joining the product , aisles and department tables and forming one product table
#with product ID , product Name and the aisle and department the products belong.
products <- products %>% 
  inner_join(aisles) %>% inner_join(departments) %>% 
  select(-aisle_id, -department_id)

#removing aisles and department data from the environment
rm(aisles, departments)

#finding user_id from Orders data for the orders which also exist in order_trian data
order_train$user_id <- orders$user_id[match(order_train$order_id, orders$order_id)]

#joining the order_prior data with the orders data by the order_id 
orders_products <- orders %>% inner_join(order_prior, by = "order_id")
head(orders_products)

#now we can remove the order_prior data from the environment
rm(order_prior)

# Cleaning the products data
#In the orders_products table ,
#first we will order by the columns user_id, order_number, product_id using arrange()
#then we are doing group by on columns user_id, product_id
#next we create a new column "product_time" using the mutate() where product_time= row_number
#Now that we have the new column , we ungroup the data
#Next we again group the table by product_id
#Now we use the summarise() to ceate 4 new variables in the following ways
#prod_orders = the number of rows,  prod_reorders = sum(reordered),
#prod_first_orders = sum of the rows where product_time == 1, 
#prod_second_orders = sum of the rows where product_time == 2
prod <- orders_products %>%
  arrange(user_id, order_number, product_id) %>%
  group_by(user_id, product_id) %>%
  mutate(product_time = row_number()) %>%
  ungroup() %>%
  group_by(product_id) %>%
  summarise(
    prod_orders = n(),
    prod_reorders = sum(reordered),
    prod_first_orders = sum(product_time == 1),
    prod_second_orders = sum(product_time == 2)
  )
head(prod)

#calculating product reorder probability
prod$prod_reorder_probability <- prod$prod_second_orders / prod$prod_first_orders
#calculating the number of times a product was reordered
prod$prod_reorder_times <- 1 + prod$prod_reorders / prod$prod_first_orders
#calculating the product reorder ratio
prod$prod_reorder_ratio <- prod$prod_reorders / prod$prod_orders
#removing the insignificant columns
prod <- prod %>% select(-prod_reorders, -prod_first_orders, -prod_second_orders)
head(prod)

#now removing the products data from the environment
rm(products)

#creating a dataset with customer details
#From the orders table we are only filtering out the rows that have eval_set == "prior"  
#and then we are grouping them by "user_id" and then we are creating three new columns
#which are named cust_orders, cust_period,cust_mean_days_since_prior
customers <- orders %>%
  filter(eval_set == "prior") %>%
  group_by(user_id) %>%
  summarise(
    cust_orders = max(order_number),
    cust_period = sum(days_since_prior_order, na.rm = T),
    cust_mean_days_since_prior = mean(days_since_prior_order, na.rm = T)
  )
head(customers)

#Now we are creating another dataset cust_products from the orders_product dataset
#From the orders_products table we are grouping them by "user_id" and then we are 
#creating three new columns which are named cust_total_products, cust_reorder_ratio,cust_distinct_products
cust_products <- orders_products %>%
  group_by(user_id) %>%
  summarise(
    cust_total_products = n(),
    cust_reorder_ratio = sum(reordered == 1) / sum(order_number > 1),
    cust_distinct_products = n_distinct(product_id)
  )
head(cust_products)

#Now we do inner join on customers and cust_products
customers <- customers %>% inner_join(cust_products)
#create a new column cust_average_basket which has the number of products per order
customers$cust_average_basket <- customers$cust_total_products / customers$cust_orders

#Now fron the orders data we filter the rows where "eval_set" is not equal to prior
#and we select the columns user_id, order_id, eval_set,
#and time_since_last_order = days_since_prior_order and create it into cust_products
cust_products <- orders %>%
  filter(eval_set != "prior") %>%
  select(user_id, order_id, eval_set,
         time_since_last_order = days_since_prior_order)
#inner join the customers and cust_products data
customers <- customers %>% inner_join(cust_products)
head(customers)
#now we can remove the cust_products data
rm(cust_products)

#creating a dataset from orders_product
#In the orders product table we group by user id and product id
#then form new columns using summarise()
data <- orders_products %>%
  group_by(user_id, product_id) %>% 
  summarise(
    up_orders = n(),
    up_first_order = min(order_number),
    up_last_order = max(order_number),
    up_average_cart_position = mean(add_to_cart_order))

#we can now remove orders_products and orders
rm(orders_products, orders)

#combining all the datasets and creating one dataset.
#we now do inner joins on the data, prod and customers data
data <- data %>% 
  inner_join(prod, by = "product_id") %>%
  inner_join(customers, by = "user_id")
#
data$up_order_rate <- data$up_orders / data$cust_orders

data$up_orders_since_last_order <- data$cust_orders - data$up_last_order

data$up_order_rate_since_first_order <- data$up_orders / (data$cust_orders - data$up_first_order + 1)

#we do left join on data and order_train where we select
#user_id, product_id, reordered grouped by user_id and product_id
data <- data %>% 
  left_join(order_train %>% select(user_id, product_id, reordered), 
            by = c("user_id", "product_id"))

#now that we have combined and created one dataset we can remove order_train, prod, customers
rm(order_train, prod, customers)
#view the dataset created
head(data)

#creating train and test datasets
train <- as.data.frame(data[data$eval_set == "train",])
train$eval_set <- NULL
train$user_id <- NULL
train$product_id <- NULL
train$order_id <- NULL
train$reordered[is.na(train$reordered)] <- 0
head(train)

test <- as.data.frame(data[data$eval_set == "test",])
test$eval_set <- NULL
test$user_id <- NULL
test$reordered <- NULL
head(test)

rm(data)

#------------------------Training the XGBOOST Model---------------------------------------#

library(xgboost)

params <- list(
  "objective"           = "reg:logistic",
  "eval_metric"         = "logloss",
  "eta"                 = 0.1,
  "max_depth"           = 6,
  "min_child_weight"    = 10,
  "gamma"               = 0.70,
  "subsample"           = 0.76,
  "colsample_bytree"    = 0.95,
  "alpha"               = 2e-05,
  "lambda"              = 10
)
#As we have a huge dataset , we take 40% of our train data into subtrain
subtrain <- train %>% sample_frac(0.4)
X <- xgb.DMatrix(as.matrix(subtrain %>% select(-reordered)), label = subtrain$reordered)
model <- xgboost(data = X, params = params, nrounds = 80)

importance <- xgb.importance(colnames(X), model = model)
xgb.plot.importance(importance)

#---------------------Validating on the test data------------------------------#

X <- xgb.DMatrix(as.matrix(test %>% select(-order_id, -product_id)))
test$reordered <- predict(model, X)

test$reordered <- (test$reordered > 0.21) * 1

submission <- test %>%
  filter(reordered == 1) %>%
  group_by(order_id) %>%
  summarise(
    products = paste(product_id, collapse = " ")
  )
#Labeling the blanks in product column as "None"
missing <- data.frame(
  order_id = unique(test$order_id[!test$order_id %in% submission$order_id]),
  products = "None"
)

#creating the submission file
submission <- submission %>% bind_rows(missing) %>% arrange(order_id)
write.csv(submission, file = "C:/Users/Rashmita Rout/Desktop/Data Mining/Project/submit_1.csv", row.names = F)


