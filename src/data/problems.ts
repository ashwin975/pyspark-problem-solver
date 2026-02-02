export type Difficulty = "Easy" | "Medium" | "Hard";
export type Category = 
  | "Filtering" 
  | "Transformations" 
  | "Aggregations" 
  | "Joins" 
  | "Window Functions" 
  | "String Functions"
  | "Date Functions"
  | "Null Handling"
  | "Regex"
  | "Pivoting"
  | "UDFs"
  | "Data Cleaning";

export const categories: Category[] = [
  "Filtering",
  "Transformations", 
  "Aggregations",
  "Joins",
  "Window Functions",
  "String Functions",
  "Date Functions",
  "Null Handling",
  "Regex",
  "Pivoting",
  "UDFs",
  "Data Cleaning"
];

export interface TestCase {
  input: Record<string, unknown[]>;
  expected_output: unknown[];
}

export interface Problem {
  id: string;
  title: string;
  difficulty: Difficulty;
  category: Category;
  description: string;
  starterCode: string;
  solution: string;
  explanation: string;
  hints: string[];
  testCases?: TestCase[];
  completed?: boolean;
}

const PYSPARK_IMPORTS = `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

`;

// Problems from ZillaCode - PySpark learning platform (52 problems)
export const problems: Problem[] = [
  // 1. Streaming Platform - Filter videos
  {
    id: "1",
    title: "Streaming Platform Filter",
    difficulty: "Easy",
    category: "Filtering",
    description: `**Streaming Platform**

You work for a video streaming platform and are given a DataFrame containing information about videos. The DataFrame named **input_df** has the following schema:

| Column Name  | Data Type |
|--------------|-----------|
| video_id     | Integer   |
| title        | String    |
| genre        | String    |
| release_year | Integer   |
| duration     | Integer   |
| view_count   | Integer   |

Your task is to write a function that takes in the input DataFrame and returns a DataFrame containing only the videos with more than 1,000,000 views and released in the last 5 years (assume current year is 2024).

**Example**

| video_id | title                | genre  | release_year | duration | view_count |
|----------|----------------------|--------|--------------|----------|------------|
| 1        | Amazing Adventure    | Action | 2020         | 120      | 2500000    |
| 2        | Sci-fi World         | Sci-fi | 2018         | 140      | 800000     |
| 3        | Mysterious Island    | Drama  | 2022         | 115      | 1500000    |

**Output** - Only videos with view_count > 1,000,000 AND release_year >= 2019`,
    starterCode: PYSPARK_IMPORTS + `def etl(input_df):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(input_df):
    current_year = 2024
    filtered_df = input_df.where(
        (F.col("view_count") > 1000000)
        & (F.col("release_year") >= current_year - 5)
    )
    return filtered_df`,
    explanation: `Filter videos using multiple conditions with the where() method and logical operators.`,
    hints: [
      "Use F.col() to reference columns",
      "Combine conditions with & (and) operator",
      "Use where() or filter() methods"
    ]
  },

  // 2. Geology Samples - Extract age
  {
    id: "2",
    title: "Geology Samples - Extract Age",
    difficulty: "Medium",
    category: "Regex",
    description: `**Geology Samples**

A geologist is working with a dataset containing rock samples. The DataFrame has the following schema:

| Column Name | Data Type |
|-------------|-----------|
| sample_id   | string    |
| description | string    |

The description field contains a mixture of letters and numbers. Extract the numeric parts from the description column and create a new column called \`age\`.

**Example**

| sample_id | description     |
|-----------|-----------------|
| S1        | Basalt_450Ma    |
| S2        | Sandstone_300Ma |
| S3        | Limestone       |

**Output**

| sample_id | description     | age  |
|-----------|-----------------|------|
| S1        | Basalt_450Ma    | 450  |
| S2        | Sandstone_300Ma | 300  |
| S3        | Limestone       |      |`,
    starterCode: PYSPARK_IMPORTS + `def etl(input_df):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(input_df):
    output_df = input_df.withColumn(
        "age",
        F.regexp_extract(F.col("description"), r"(\\d+)", 0)
    )
    return output_df`,
    explanation: `Use regexp_extract to extract numeric patterns from strings.`,
    hints: [
      "Use F.regexp_extract() for pattern extraction",
      "The regex pattern \\d+ matches one or more digits",
      "Empty string is returned when no match found"
    ]
  },

  // 3. Previous Product (Window Function - LAG)
  {
    id: "3",
    title: "Transaction Previous Product",
    difficulty: "Hard",
    category: "Window Functions",
    description: `**Customer Transactions**

You have a DataFrame of customer transactions with schema:

| Column Name | Data Type |
|-------------|-----------|
| customer_id | string    |
| product_id  | string    |
| date        | string    |
| quantity    | integer   |

Add a column \`previous_product\` that contains the product_id from the customer's previous transaction (ordered by date). Also add a \`date_and_product\` column concatenating date and previous_product.

**Example**

| customer_id | product_id | date       | quantity |
|-------------|------------|------------|----------|
| CUST1       | PROD1      | 2023-01-01 | 2        |
| CUST1       | PROD2      | 2023-02-10 | 3        |

**Output** includes previous_product and date_and_product columns`,
    starterCode: PYSPARK_IMPORTS + `def etl(transactions):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(transactions):
    windowSpec = W.partitionBy("customer_id").orderBy("date")
    
    transactions = transactions.withColumn(
        "previous_product",
        F.lag(transactions["product_id"]).over(windowSpec)
    )
    
    transactions = transactions.withColumn(
        "previous_product",
        F.when(F.col("previous_product").isNull(), "None")
        .otherwise(F.col("previous_product"))
    )
    
    transactions = transactions.withColumn(
        "date_and_product",
        F.concat_ws(" ", transactions["date"], transactions["previous_product"])
    )
    
    return transactions`,
    explanation: `Use Window functions with LAG to access previous row values.`,
    hints: [
      "Define a Window with partitionBy and orderBy",
      "Use F.lag() to get previous row value",
      "Handle null values with F.when() and F.otherwise()"
    ]
  },

  // 4. Mining Extraction (Join + Aggregation)
  {
    id: "4",
    title: "Mining Extraction Summary",
    difficulty: "Medium",
    category: "Aggregations",
    description: `**Mining Data**

You have two DataFrames: \`mines\` and \`extraction\`.

**mines Schema:**
| Column   | Type    |
|----------|---------|
| id       | integer |
| location | string  |
| type     | string  |

**extraction Schema:**
| Column   | Type    |
|----------|---------|
| mine_id  | integer |
| date     | string  |
| mineral  | string  |
| quantity | integer |

Calculate the total quantity of each mineral extracted per location.

**Output Schema:**
| Column         | Type    |
|----------------|---------|
| location       | string  |
| mineral        | string  |
| total_quantity | integer |`,
    starterCode: PYSPARK_IMPORTS + `def etl(mines, extraction):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(mines, extraction):
    joined_df = mines.join(
        extraction, mines.id == extraction.mine_id
    )
    
    result_df = joined_df.groupby(
        ["location", "mineral"]
    ).agg(
        F.sum("quantity").alias("total_quantity")
    )
    
    result_df = result_df.orderBy("location", "mineral")
    
    return result_df`,
    explanation: `Join DataFrames and aggregate with groupBy and sum.`,
    hints: [
      "Join mines and extraction on id == mine_id",
      "Group by location and mineral",
      "Use F.sum() to calculate totals"
    ]
  },

  // 5. Financial Transactions - Data Cleaning
  {
    id: "5",
    title: "Clean Financial Transactions",
    difficulty: "Hard",
    category: "Data Cleaning",
    description: `**Financial Data Cleaning**

You have two DataFrames: \`df_transactions\` and \`df_clients\`.

**df_transactions Schema:**
| Column        | Type    |
|---------------|---------|
| TransactionID | integer |
| ClientID      | integer |
| Date          | string  |
| Amount        | float   |

**df_clients Schema:**
| Column     | Type    |
|------------|---------|
| ClientID   | integer |
| ClientName | string  |
| Industry   | string  |

Clean the data by:
1. Removing duplicate TransactionIDs (keep first)
2. Removing duplicate ClientIDs (keep first)
3. Filtering out invalid IDs (must be > 0)
4. Filtering out invalid date formats
5. Join the cleaned DataFrames on ClientID`,
    starterCode: PYSPARK_IMPORTS + `def etl(df_transactions, df_clients):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(df_transactions, df_clients):
    window_trans = W.partitionBy("TransactionID").orderBy("TransactionID")
    window_clients = W.partitionBy("ClientID").orderBy("ClientID")
    
    df_transactions = (
        df_transactions.withColumn("rn", F.row_number().over(window_trans))
        .where(F.col("rn") == 1)
        .drop("rn")
    )
    df_clients = (
        df_clients.withColumn("rn", F.row_number().over(window_clients))
        .where(F.col("rn") == 1)
        .drop("rn")
    )
    
    df_transactions = df_transactions.filter(F.col("TransactionID") > 0)
    df_transactions = df_transactions.filter(
        F.col("Date").rlike("^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
    )
    df_clients = df_clients.filter(F.col("ClientID") > 0)
    
    df_cleaned = df_transactions.join(df_clients, on="ClientID", how="inner")
    
    return df_cleaned`,
    explanation: `Combine deduplication, filtering, and joining for data cleaning.`,
    hints: [
      "Use row_number() for deduplication",
      "Use rlike() for date format validation",
      "Filter invalid IDs before joining"
    ]
  },

  // 6. User Page Interactions (Union)
  {
    id: "6",
    title: "Combine User Interactions",
    difficulty: "Medium",
    category: "Transformations",
    description: `**User Interactions**

You have three DataFrames representing different user interactions:

**page_visits:** user_id, page_id, visit_time
**page_likes:** user_id, page_id, like_time  
**page_comments:** user_id, page_id, comment_time

Combine all interactions into a single DataFrame with columns:
- user_id
- page_id
- interaction_time (renamed from original timestamp)
- interaction_type ("visit", "like", or "comment")

Sort by interaction_time.`,
    starterCode: PYSPARK_IMPORTS + `def etl(page_visits, page_likes, page_comments):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(page_visits, page_likes, page_comments):
    visits = page_visits.withColumn("interaction_type", F.lit("visit"))
    likes = page_likes.withColumn("interaction_type", F.lit("like"))
    comments = page_comments.withColumn("interaction_type", F.lit("comment"))
    
    visits = visits.withColumnRenamed("visit_time", "interaction_time")
    likes = likes.withColumnRenamed("like_time", "interaction_time")
    comments = comments.withColumnRenamed("comment_time", "interaction_time")
    
    user_interactions = visits.select("user_id", "page_id", "interaction_time", "interaction_type") \
        .union(likes.select("user_id", "page_id", "interaction_time", "interaction_type")) \
        .union(comments.select("user_id", "page_id", "interaction_time", "interaction_type"))
    
    return user_interactions.orderBy("interaction_time")`,
    explanation: `Add literal columns and union multiple DataFrames.`,
    hints: [
      "Use F.lit() to add constant columns",
      "Use withColumnRenamed() to standardize column names",
      "Union DataFrames with matching schemas"
    ]
  },

  // 7. Self Interactions
  {
    id: "7",
    title: "Count Self Interactions",
    difficulty: "Easy",
    category: "Filtering",
    description: `**Social Network Interactions**

You have a DataFrame of user interactions:

| Column         | Type    |
|----------------|---------|
| interaction_id | integer |
| user1_id       | integer |
| user2_id       | integer |
| interaction_type | string |

Find users who have interacted with themselves (user1_id == user2_id) and count how many self-interactions each user has.

**Output Schema:**
| Column                | Type    |
|-----------------------|---------|
| user1_id              | integer |
| self_interaction_count| integer |`,
    starterCode: PYSPARK_IMPORTS + `def etl(input_df):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(input_df):
    self_interactions_df = input_df.filter(
        F.col("user1_id") == F.col("user2_id")
    )
    
    self_interactions_count_df = (
        self_interactions_df.groupBy("user1_id")
        .agg(F.count("interaction_id").alias("self_interaction_count"))
    )
    
    return self_interactions_count_df`,
    explanation: `Filter for self-interactions and count by user.`,
    hints: [
      "Filter where user1_id equals user2_id",
      "Group by user1_id and count interactions"
    ]
  },

  // 8. Call Center Aggregation
  {
    id: "8",
    title: "Call Center Aggregation",
    difficulty: "Medium",
    category: "Aggregations",
    description: `**Call Center**

You have two DataFrames: \`calls_df\` and \`customers_df\`.

**calls_df Schema:**
| Column   | Type    |
|----------|---------|
| call_id  | integer |
| cust_id  | integer |
| date     | string  |
| duration | integer |

**customers_df Schema:**
| Column     | Type    |
|------------|---------|
| cust_id    | integer |
| name       | string  |
| state      | string  |
| tenure     | integer |
| occupation | string  |

Return the number of distinct customers and total duration of calls per date.

**Output Schema:**
| Column         | Type    |
|----------------|---------|
| date           | string  |
| num_customers  | integer |
| total_duration | integer |`,
    starterCode: PYSPARK_IMPORTS + `def etl(calls_df, customers_df):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(calls_df, customers_df):
    joined_df = calls_df.join(customers_df, "cust_id")
    
    agg_df = joined_df.groupby("date").agg(
        F.count_distinct("cust_id").alias("num_customers"),
        F.sum("duration").alias("total_duration")
    )
    
    return agg_df`,
    explanation: `Join and aggregate with count_distinct and sum.`,
    hints: [
      "Join on cust_id first",
      "Use F.count_distinct() for unique customer count",
      "Use F.sum() for total duration"
    ]
  },

  // 9. PE Firms Portfolio Value
  {
    id: "9",
    title: "PE Portfolio Value",
    difficulty: "Medium",
    category: "Aggregations",
    description: `**Private Equity Portfolio**

You have two DataFrames: \`portfolio\` and \`prices\`.

**portfolio Schema:**
| Column  | Type    |
|---------|---------|
| PE_firm | string  |
| company | string  |
| shares  | integer |

**prices Schema:**
| Column        | Type   |
|---------------|--------|
| date          | string |
| company       | string |
| closing_price | float  |

Calculate the daily portfolio value for each PE firm (shares * closing_price, summed by firm and date).

**Output Schema:**
| Column          | Type    |
|-----------------|---------|
| PE_firm         | string  |
| date            | string  |
| portfolio_value | integer |`,
    starterCode: PYSPARK_IMPORTS + `def etl(portfolio, prices):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(portfolio, prices):
    merged_df = portfolio.join(prices, on="company", how="inner")
    
    portfolio_value = merged_df.withColumn(
        "value",
        F.col("shares") * F.col("closing_price")
    )
    
    portfolio_value = portfolio_value.groupby(
        ["PE_firm", "date"]
    ).agg(F.sum("value").alias("portfolio_value"))
    
    return portfolio_value`,
    explanation: `Join, compute values, and aggregate by groups.`,
    hints: [
      "Join portfolio and prices on company",
      "Calculate value as shares * closing_price",
      "Group by PE_firm and date"
    ]
  },

  // 10. PE Firms Full Join
  {
    id: "10",
    title: "PE Firms Full Join",
    difficulty: "Medium",
    category: "Joins",
    description: `**Private Equity Data**

You have three DataFrames: \`pe_firms\`, \`pe_funds\`, and \`pe_investments\`.

**pe_firms:** firm_id, firm_name, founded_year, location
**pe_funds:** fund_id, firm_id, fund_name, fund_size, fund_start_year, fund_end_year
**pe_investments:** investment_id, fund_id, company_name, investment_amount, investment_date

Perform a full outer join on all three DataFrames to combine all records. Filter out rows where all columns are null.`,
    starterCode: PYSPARK_IMPORTS + `def etl(pe_firms, pe_funds, pe_investments):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(pe_firms, pe_funds, pe_investments):
    joined_df = pe_firms.join(
        pe_funds, "firm_id", "outer"
    ).join(pe_investments, "fund_id", "outer")
    
    joined_df = joined_df.dropna(how="all")
    
    return joined_df`,
    explanation: `Chain full outer joins and remove completely null rows.`,
    hints: [
      "Use 'outer' for full outer join",
      "Chain multiple join operations",
      "Use dropna(how='all') to remove all-null rows"
    ]
  },

  // 11. Filter Null Values (Movies)
  {
    id: "11",
    title: "Filter Null Values",
    difficulty: "Easy",
    category: "Null Handling",
    description: `**Movies**

You have a DataFrame \`movies_df\` with schema:

| Column Name           | Data Type |
|-----------------------|-----------|
| movie_id              | integer   |
| movie_title           | string    |
| director_name         | string    |
| release_date          | date      |
| box_office_collection | float     |
| genre                 | string    |

Filter to retain only rows where \`box_office_collection\` is null.`,
    starterCode: PYSPARK_IMPORTS + `def etl(movies_df):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(movies_df):
    null_box_office_df = movies_df.filter(
        F.isnull("box_office_collection")
    )
    return null_box_office_df`,
    explanation: `Use F.isnull() to filter for null values.`,
    hints: [
      "Use F.isnull('column_name') to check for nulls",
      "Alternative: F.col('column').isNull()"
    ]
  },

  // 12. Social Media Regex Replace
  {
    id: "12",
    title: "Correct Social Media Posts",
    difficulty: "Easy",
    category: "Regex",
    description: `**Social Media Posts**

You have a DataFrame \`social_media\` with:

| Column   | Type    |
|----------|---------|
| id       | integer |
| text     | string  |
| date     | string  |
| likes    | integer |
| comments | integer |
| shares   | integer |
| platform | string  |

Replace all occurrences of "Python" in the text column with "PySpark".`,
    starterCode: PYSPARK_IMPORTS + `def etl(social_media):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(social_media):
    new_social_media = social_media.withColumn(
        "text",
        F.regexp_replace("text", "Python", "PySpark")
    )
    return new_social_media`,
    explanation: `Use regexp_replace to substitute text patterns.`,
    hints: [
      "Use F.regexp_replace(column, pattern, replacement)",
      "This replaces all occurrences"
    ]
  },

  // 13. Products Sales Inventory
  {
    id: "13",
    title: "Products Sales Summary",
    difficulty: "Hard",
    category: "Joins",
    description: `**Retail Data**

You have three DataFrames: \`products\`, \`sales\`, and \`inventory\`.

**products:** product_id, product_name, category
**sales:** product_id, quantity, revenue
**inventory:** product_id, stock

Join all three and calculate totals for each product:
- total_quantity (from sales)
- total_revenue (from sales)  
- total_stock (from inventory)

Use 0 for products with no sales or inventory.`,
    starterCode: PYSPARK_IMPORTS + `def etl(products, sales, inventory):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(products, sales, inventory):
    sales_agg = sales.groupby("product_id").agg(
        F.sum("quantity").alias("total_quantity"),
        F.sum("revenue").alias("total_revenue")
    )
    
    stock_agg = inventory.groupby("product_id").agg(
        F.sum("stock").alias("total_stock")
    )
    
    result = products.join(sales_agg, "product_id", "left") \
        .join(stock_agg, "product_id", "left")
    
    result = result.select(
        "*",
        F.coalesce(F.col("total_quantity"), F.lit(0)).alias("total_quantity"),
        F.coalesce(F.col("total_revenue"), F.lit(0)).alias("total_revenue"),
        F.coalesce(F.col("total_stock"), F.lit(0)).alias("total_stock")
    )
    
    return result`,
    explanation: `Aggregate separately, left join, and use coalesce for nulls.`,
    hints: [
      "Aggregate sales and inventory separately first",
      "Use left joins to keep all products",
      "Use F.coalesce() to replace nulls with 0"
    ]
  },

  // 14. Flooring Company (Split + Join)
  {
    id: "14",
    title: "Flooring Company ETL",
    difficulty: "Medium",
    category: "String Functions",
    description: `**Flooring Company**

You have three DataFrames: \`customers\`, \`orders\`, and \`products\`.

**customers:** customer_id, full_name, location
**orders:** order_id, customer_id, product_id, quantity
**products:** product_id, product_info

Split \`full_name\` into \`first_name\` and \`last_name\` columns.
Split \`product_info\` (format: "type,color") into \`product_type\` and \`product_color\`.
Join all three DataFrames.`,
    starterCode: PYSPARK_IMPORTS + `def etl(customers, orders, products):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(customers, orders, products):
    customers = customers.withColumn(
        "split_name", F.split(customers["full_name"], " ")
    )
    customers = customers.withColumn("first_name", customers["split_name"].getItem(0))
    customers = customers.withColumn("last_name", customers["split_name"].getItem(1))
    customers = customers.drop("split_name", "full_name")
    
    products = products.withColumn(
        "split_info", F.split(products["product_info"], ",")
    )
    products = products.withColumn("product_type", products["split_info"].getItem(0))
    products = products.withColumn("product_color", products["split_info"].getItem(1))
    products = products.drop("split_info", "product_info")
    
    df = orders.join(customers, "customer_id").join(products, "product_id")
    
    return df`,
    explanation: `Split strings into arrays and extract elements.`,
    hints: [
      "Use F.split() to split strings into arrays",
      "Use getItem(index) to access array elements",
      "Drop intermediate columns after extraction"
    ]
  },

  // 15. Research Papers Row Number
  {
    id: "15",
    title: "Author Row Numbers",
    difficulty: "Medium",
    category: "Window Functions",
    description: `**Research Papers**

You have two DataFrames: \`research_papers\` and \`authors\`.

**research_papers:** paper_id, title, year
**authors:** paper_id, author_id, name

Add a \`row_number\` column to authors that assigns a unique row number to each author within their paper, ordered by author_id.

**Example Output:**
| paper_id | author_id | name  | row_number |
|----------|-----------|-------|------------|
| P1       | A1        | Alice | 1          |
| P1       | A2        | Bob   | 2          |
| P2       | A3        | Carol | 1          |`,
    starterCode: PYSPARK_IMPORTS + `def etl(research_papers, authors):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(research_papers, authors):
    author_window = W.partitionBy("paper_id").orderBy("author_id")
    result = authors.withColumn(
        "row_number",
        F.row_number().over(author_window)
    )
    return result`,
    explanation: `Use Window row_number for sequential numbering within groups.`,
    hints: [
      "Create Window with partitionBy and orderBy",
      "Use F.row_number().over(window)",
      "Row numbers restart at 1 for each partition"
    ]
  },

  // 16. Mountain Climbing (Last Climber)
  {
    id: "16",
    title: "Mountain Last Climber",
    difficulty: "Hard",
    category: "Window Functions",
    description: `**Mountain Climbing**

You have: \`mountain_info\` (name, height, country, range) and \`mountain_climbers\` (climber_name, mountain_name, climb_date, climb_time).

Find the last climber for each mountain (most recent climb_date). Return only mountains that have been climbed.

**Output:** mountain_name, last_climber_name, last_climb_date, last_climb_time`,
    starterCode: PYSPARK_IMPORTS + `def etl(mountain_info, mountain_climbers):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(mountain_info, mountain_climbers):
    window = W.partitionBy("mountain_name").orderBy(
        F.desc("climb_date"), F.desc("climb_time")
    )
    
    latest_climb = (
        mountain_climbers.select(
            "*", F.rank().over(window).alias("rank")
        )
        .filter("rank == 1")
        .drop("rank")
    )
    
    output = latest_climb.select(
        "mountain_name",
        F.col("climber_name").alias("last_climber_name"),
        F.col("climb_date").alias("last_climb_date"),
        F.col("climb_time").alias("last_climb_time")
    )
    
    return output`,
    explanation: `Use Window ranking to find the most recent record per group.`,
    hints: [
      "Order by date descending to get latest first",
      "Use rank() or row_number() to identify first record",
      "Filter where rank == 1"
    ]
  },

  // 17. Rental Income (Pivot)
  {
    id: "17",
    title: "Total Rental Income",
    difficulty: "Hard",
    category: "Pivoting",
    description: `**Rental Properties**

You have: \`properties_df\` (property_id, landlord_id, property_type, rent, square_feet, city) and \`landlords_df\` (landlord_id, first_name, last_name, email, phone).

Calculate total rental income per landlord. Return: landlord_id, landlord_name (full name), total_rental_income.`,
    starterCode: PYSPARK_IMPORTS + `def etl(properties_df, landlords_df):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(properties_df, landlords_df):
    rental_agg = properties_df.groupBy("landlord_id").agg(
        F.sum("rent").alias("total_rental_income")
    )
    
    result = rental_agg.join(landlords_df, "landlord_id")
    
    result = result.select(
        "landlord_id",
        F.concat(F.col("first_name"), F.lit(" "), F.col("last_name")).alias("landlord_name"),
        F.col("total_rental_income").cast("float")
    )
    
    return result.orderBy("landlord_id")`,
    explanation: `Aggregate rent by landlord and join with landlord info.`,
    hints: [
      "Sum rent grouped by landlord_id",
      "Join with landlords for name info",
      "Use F.concat() for full name"
    ]
  },

  // 18-52: Additional problems (condensed for brevity, following same pattern)
  {
    id: "18",
    title: "Date Formatting",
    difficulty: "Easy",
    category: "Date Functions",
    description: `Convert date strings from 'yyyy-MM-dd' format to 'MM/dd/yyyy' format.`,
    starterCode: PYSPARK_IMPORTS + `def etl(input_df):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(input_df):
    return input_df.withColumn(
        "formatted_date",
        F.date_format(F.to_date("date_col", "yyyy-MM-dd"), "MM/dd/yyyy")
    )`,
    explanation: `Use to_date and date_format for date conversions.`,
    hints: ["Use F.to_date() to parse", "Use F.date_format() to format"]
  },

  {
    id: "19",
    title: "Cumulative Sum",
    difficulty: "Medium",
    category: "Window Functions",
    description: `Calculate cumulative sum of sales per product over time.`,
    starterCode: PYSPARK_IMPORTS + `def etl(sales_df):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(sales_df):
    window = W.partitionBy("product_id").orderBy("date").rowsBetween(W.unboundedPreceding, W.currentRow)
    return sales_df.withColumn("cumulative_sales", F.sum("amount").over(window))`,
    explanation: `Use Window with rowsBetween for cumulative calculations.`,
    hints: ["Define window with unboundedPreceding to currentRow", "Sum over the window"]
  },

  {
    id: "20",
    title: "Rank Products",
    difficulty: "Medium",
    category: "Window Functions",
    description: `Rank products by sales within each category.`,
    starterCode: PYSPARK_IMPORTS + `def etl(products_df):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(products_df):
    window = W.partitionBy("category").orderBy(F.desc("sales"))
    return products_df.withColumn("rank", F.rank().over(window))`,
    explanation: `Use rank() over a partitioned window.`,
    hints: ["Partition by category", "Order by sales descending", "Apply F.rank()"]
  },

  {
    id: "21",
    title: "Filter by Date Range",
    difficulty: "Easy",
    category: "Date Functions",
    description: `Filter records between two dates (inclusive).`,
    starterCode: PYSPARK_IMPORTS + `def etl(input_df, start_date, end_date):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(input_df, start_date, end_date):
    return input_df.filter(
        (F.col("date") >= start_date) & (F.col("date") <= end_date)
    )`,
    explanation: `Use comparison operators with dates.`,
    hints: ["Use >= and <= for range", "Combine with & operator"]
  },

  {
    id: "22",
    title: "Calculate Age",
    difficulty: "Easy",
    category: "Date Functions",
    description: `Calculate age in years from birth_date column.`,
    starterCode: PYSPARK_IMPORTS + `def etl(users_df):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(users_df):
    return users_df.withColumn(
        "age",
        F.floor(F.datediff(F.current_date(), F.col("birth_date")) / 365)
    )`,
    explanation: `Use datediff to calculate days and convert to years.`,
    hints: ["Use F.datediff(end, start)", "Divide by 365 and floor"]
  },

  {
    id: "23",
    title: "String Case Conversion",
    difficulty: "Easy",
    category: "String Functions",
    description: `Convert name column to uppercase and title case.`,
    starterCode: PYSPARK_IMPORTS + `def etl(input_df):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(input_df):
    return input_df.withColumn("upper_name", F.upper("name")) \
                   .withColumn("title_name", F.initcap("name"))`,
    explanation: `Use upper() and initcap() for case conversion.`,
    hints: ["F.upper() for uppercase", "F.initcap() for title case"]
  },

  {
    id: "24",
    title: "Trim Whitespace",
    difficulty: "Easy",
    category: "String Functions",
    description: `Trim leading and trailing whitespace from string columns.`,
    starterCode: PYSPARK_IMPORTS + `def etl(input_df):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(input_df):
    return input_df.withColumn("cleaned_text", F.trim("text"))`,
    explanation: `Use F.trim() to remove whitespace.`,
    hints: ["F.trim() removes both leading and trailing spaces", "F.ltrim() and F.rtrim() for one-sided"]
  },

  {
    id: "25",
    title: "Fill Null Values",
    difficulty: "Easy",
    category: "Null Handling",
    description: `Fill null values with defaults: 0 for numeric, 'Unknown' for string.`,
    starterCode: PYSPARK_IMPORTS + `def etl(input_df):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(input_df):
    return input_df.fillna({"amount": 0, "name": "Unknown"})`,
    explanation: `Use fillna with dictionary for column-specific defaults.`,
    hints: ["fillna accepts a dictionary", "Keys are column names, values are defaults"]
  },

  {
    id: "26",
    title: "Drop Null Rows",
    difficulty: "Easy",
    category: "Null Handling",
    description: `Drop rows where any specified column is null.`,
    starterCode: PYSPARK_IMPORTS + `def etl(input_df):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(input_df):
    return input_df.dropna(subset=["col1", "col2"])`,
    explanation: `Use dropna with subset parameter.`,
    hints: ["dropna() removes null rows", "subset limits to specific columns"]
  },

  {
    id: "27",
    title: "Coalesce Columns",
    difficulty: "Medium",
    category: "Null Handling",
    description: `Return first non-null value from multiple columns.`,
    starterCode: PYSPARK_IMPORTS + `def etl(input_df):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(input_df):
    return input_df.withColumn(
        "result",
        F.coalesce(F.col("primary"), F.col("secondary"), F.lit("default"))
    )`,
    explanation: `F.coalesce returns first non-null value.`,
    hints: ["Pass multiple columns to coalesce", "Include F.lit() for default value"]
  },

  {
    id: "28",
    title: "Inner Join",
    difficulty: "Easy",
    category: "Joins",
    description: `Join two DataFrames on a common key using inner join.`,
    starterCode: PYSPARK_IMPORTS + `def etl(df1, df2):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(df1, df2):
    return df1.join(df2, "common_key", "inner")`,
    explanation: `Inner join returns only matching rows.`,
    hints: ["Specify column name for join", "'inner' is default join type"]
  },

  {
    id: "29",
    title: "Left Join with Null Handling",
    difficulty: "Medium",
    category: "Joins",
    description: `Left join and replace nulls from right table with defaults.`,
    starterCode: PYSPARK_IMPORTS + `def etl(df1, df2):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(df1, df2):
    result = df1.join(df2, "key", "left")
    return result.fillna({"right_col": "N/A"})`,
    explanation: `Left join keeps all left rows, fill nulls for unmatched.`,
    hints: ["Use 'left' join type", "fillna for null handling"]
  },

  {
    id: "30",
    title: "Anti Join",
    difficulty: "Medium",
    category: "Joins",
    description: `Find records in df1 that don't exist in df2.`,
    starterCode: PYSPARK_IMPORTS + `def etl(df1, df2):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(df1, df2):
    return df1.join(df2, "key", "left_anti")`,
    explanation: `Anti join returns rows from left that have no match in right.`,
    hints: ["Use 'left_anti' or 'anti' join type", "Returns only non-matching rows"]
  },

  {
    id: "31",
    title: "Group By with Multiple Aggregations",
    difficulty: "Medium",
    category: "Aggregations",
    description: `Calculate min, max, avg, and count for grouped data.`,
    starterCode: PYSPARK_IMPORTS + `def etl(input_df):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(input_df):
    return input_df.groupBy("category").agg(
        F.min("value").alias("min_val"),
        F.max("value").alias("max_val"),
        F.avg("value").alias("avg_val"),
        F.count("value").alias("count")
    )`,
    explanation: `Use multiple aggregation functions in single agg() call.`,
    hints: ["Pass multiple aggregations to agg()", "Use alias() for column names"]
  },

  {
    id: "32",
    title: "Pivot Table",
    difficulty: "Hard",
    category: "Pivoting",
    description: `Pivot data to show values as columns.`,
    starterCode: PYSPARK_IMPORTS + `def etl(input_df):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(input_df):
    return input_df.groupBy("row_key").pivot("column_key").agg(F.sum("value"))`,
    explanation: `Pivot transforms row values into columns.`,
    hints: ["GroupBy the row identifier", "pivot() on the column to spread", "Aggregate the values"]
  },

  {
    id: "33",
    title: "Unpivot Data",
    difficulty: "Hard",
    category: "Transformations",
    description: `Convert wide data to long format using stack.`,
    starterCode: PYSPARK_IMPORTS + `def etl(input_df):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(input_df):
    return input_df.select(
        "id",
        F.expr("stack(3, 'col1', col1, 'col2', col2, 'col3', col3) as (column_name, value)")
    )`,
    explanation: `Use stack() expression to unpivot columns.`,
    hints: ["stack(n, key1, val1, key2, val2, ...)", "First arg is number of pairs"]
  },

  {
    id: "34",
    title: "Case When Logic",
    difficulty: "Easy",
    category: "Transformations",
    description: `Create categories based on value ranges.`,
    starterCode: PYSPARK_IMPORTS + `def etl(input_df):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(input_df):
    return input_df.withColumn(
        "category",
        F.when(F.col("value") < 10, "low")
        .when(F.col("value") < 50, "medium")
        .otherwise("high")
    )`,
    explanation: `Chain when/otherwise for conditional logic.`,
    hints: ["Use F.when() for first condition", "Chain .when() for more", "End with .otherwise()"]
  },

  {
    id: "35",
    title: "Add Months to Date",
    difficulty: "Easy",
    category: "Date Functions",
    description: `Add a specified number of months to a date column.`,
    starterCode: PYSPARK_IMPORTS + `def etl(input_df):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(input_df):
    return input_df.withColumn("new_date", F.add_months("date_col", 3))`,
    explanation: `Use add_months for date arithmetic.`,
    hints: ["F.add_months(date_column, num_months)", "Works with negative numbers too"]
  },

  {
    id: "36",
    title: "Extract Date Parts",
    difficulty: "Easy",
    category: "Date Functions",
    description: `Extract year, month, and day from a date column.`,
    starterCode: PYSPARK_IMPORTS + `def etl(input_df):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(input_df):
    return input_df.withColumn("year", F.year("date_col")) \
                   .withColumn("month", F.month("date_col")) \
                   .withColumn("day", F.dayofmonth("date_col"))`,
    explanation: `Use year(), month(), dayofmonth() for extraction.`,
    hints: ["F.year() extracts year", "F.month() for month", "F.dayofmonth() for day"]
  },

  {
    id: "37",
    title: "Substring Extraction",
    difficulty: "Easy",
    category: "String Functions",
    description: `Extract a substring from a string column.`,
    starterCode: PYSPARK_IMPORTS + `def etl(input_df):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(input_df):
    return input_df.withColumn("code", F.substring("product_id", 1, 3))`,
    explanation: `Use substring(column, start_pos, length).`,
    hints: ["Position is 1-indexed", "Specify start position and length"]
  },

  {
    id: "38",
    title: "String Length",
    difficulty: "Easy",
    category: "String Functions",
    description: `Calculate the length of string values.`,
    starterCode: PYSPARK_IMPORTS + `def etl(input_df):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(input_df):
    return input_df.withColumn("name_length", F.length("name"))`,
    explanation: `Use length() to get string length.`,
    hints: ["F.length() returns character count", "Returns null for null inputs"]
  },

  {
    id: "39",
    title: "Array Contains",
    difficulty: "Medium",
    category: "Transformations",
    description: `Check if an array column contains a specific value.`,
    starterCode: PYSPARK_IMPORTS + `def etl(input_df):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(input_df):
    return input_df.withColumn("has_item", F.array_contains("items", "target"))`,
    explanation: `Use array_contains to check array membership.`,
    hints: ["F.array_contains(array_col, value)", "Returns boolean"]
  },

  {
    id: "40",
    title: "Explode Array",
    difficulty: "Medium",
    category: "Transformations",
    description: `Explode array column into multiple rows.`,
    starterCode: PYSPARK_IMPORTS + `def etl(input_df):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(input_df):
    return input_df.select("*", F.explode("array_col").alias("item"))`,
    explanation: `Explode creates a row for each array element.`,
    hints: ["F.explode() creates multiple rows", "Original row is duplicated for each element"]
  },

  {
    id: "41",
    title: "Collect List",
    difficulty: "Medium",
    category: "Aggregations",
    description: `Aggregate values into an array per group.`,
    starterCode: PYSPARK_IMPORTS + `def etl(input_df):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(input_df):
    return input_df.groupBy("group_id").agg(
        F.collect_list("value").alias("values")
    )`,
    explanation: `collect_list aggregates values into an array.`,
    hints: ["F.collect_list() includes duplicates", "F.collect_set() for unique values only"]
  },

  {
    id: "42",
    title: "Dense Rank",
    difficulty: "Medium",
    category: "Window Functions",
    description: `Apply dense ranking (no gaps) to values.`,
    starterCode: PYSPARK_IMPORTS + `def etl(input_df):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(input_df):
    window = W.partitionBy("category").orderBy(F.desc("score"))
    return input_df.withColumn("dense_rank", F.dense_rank().over(window))`,
    explanation: `dense_rank has no gaps between rank values.`,
    hints: ["F.dense_rank() vs F.rank()", "dense_rank has no gaps for ties"]
  },

  {
    id: "43",
    title: "Percent Rank",
    difficulty: "Medium",
    category: "Window Functions",
    description: `Calculate percentile rank within groups.`,
    starterCode: PYSPARK_IMPORTS + `def etl(input_df):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(input_df):
    window = W.partitionBy("group").orderBy("value")
    return input_df.withColumn("percentile", F.percent_rank().over(window))`,
    explanation: `percent_rank gives relative rank as percentage.`,
    hints: ["Returns value between 0 and 1", "0 for first, 1 for last"]
  },

  {
    id: "44",
    title: "Lead Function",
    difficulty: "Medium",
    category: "Window Functions",
    description: `Access the next row's value.`,
    starterCode: PYSPARK_IMPORTS + `def etl(input_df):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(input_df):
    window = W.partitionBy("id").orderBy("date")
    return input_df.withColumn("next_value", F.lead("value", 1).over(window))`,
    explanation: `lead() accesses forward rows.`,
    hints: ["F.lead(column, offset)", "Offset 1 gets next row", "Returns null for last row"]
  },

  {
    id: "45",
    title: "First and Last Values",
    difficulty: "Medium",
    category: "Window Functions",
    description: `Get first and last values within a window.`,
    starterCode: PYSPARK_IMPORTS + `def etl(input_df):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(input_df):
    window = W.partitionBy("group").orderBy("date").rowsBetween(W.unboundedPreceding, W.unboundedFollowing)
    return input_df.withColumn("first_val", F.first("value").over(window)) \
                   .withColumn("last_val", F.last("value").over(window))`,
    explanation: `first() and last() with proper window bounds.`,
    hints: ["Need unbounded window for true first/last", "rowsBetween defines the frame"]
  },

  {
    id: "46",
    title: "Moving Average",
    difficulty: "Hard",
    category: "Window Functions",
    description: `Calculate 7-day moving average.`,
    starterCode: PYSPARK_IMPORTS + `def etl(input_df):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(input_df):
    window = W.partitionBy("product_id").orderBy("date").rowsBetween(-6, 0)
    return input_df.withColumn("moving_avg", F.avg("sales").over(window))`,
    explanation: `Use rowsBetween for fixed-size windows.`,
    hints: ["rowsBetween(-6, 0) covers 7 days", "Current row is 0", "Negative values are preceding rows"]
  },

  {
    id: "47",
    title: "Distinct Count",
    difficulty: "Easy",
    category: "Aggregations",
    description: `Count distinct values per group.`,
    starterCode: PYSPARK_IMPORTS + `def etl(input_df):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(input_df):
    return input_df.groupBy("category").agg(
        F.count_distinct("item").alias("unique_items")
    )`,
    explanation: `Use count_distinct for unique value counts.`,
    hints: ["F.count_distinct() or F.countDistinct()", "Can pass multiple columns"]
  },

  {
    id: "48",
    title: "Conditional Aggregation",
    difficulty: "Medium",
    category: "Aggregations",
    description: `Sum values conditionally using when inside aggregation.`,
    starterCode: PYSPARK_IMPORTS + `def etl(input_df):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(input_df):
    return input_df.groupBy("category").agg(
        F.sum(F.when(F.col("status") == "active", F.col("amount")).otherwise(0)).alias("active_total"),
        F.sum(F.when(F.col("status") == "inactive", F.col("amount")).otherwise(0)).alias("inactive_total")
    )`,
    explanation: `Combine when() with aggregation functions.`,
    hints: ["Put when inside sum", "Use otherwise(0) for non-matching"]
  },

  {
    id: "49",
    title: "Column Renaming",
    difficulty: "Easy",
    category: "Transformations",
    description: `Rename multiple columns at once.`,
    starterCode: PYSPARK_IMPORTS + `def etl(input_df):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(input_df):
    return input_df.withColumnRenamed("old_name1", "new_name1") \
                   .withColumnRenamed("old_name2", "new_name2")`,
    explanation: `Chain withColumnRenamed for multiple columns.`,
    hints: ["withColumnRenamed(old, new)", "Can also use toDF() with all column names"]
  },

  {
    id: "50",
    title: "Drop Columns",
    difficulty: "Easy",
    category: "Transformations",
    description: `Remove specified columns from DataFrame.`,
    starterCode: PYSPARK_IMPORTS + `def etl(input_df):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(input_df):
    return input_df.drop("col1", "col2", "col3")`,
    explanation: `Use drop() to remove columns.`,
    hints: ["Pass multiple column names to drop", "Can also pass Column objects"]
  },

  {
    id: "51",
    title: "Cast Data Types",
    difficulty: "Easy",
    category: "Transformations",
    description: `Convert column data types.`,
    starterCode: PYSPARK_IMPORTS + `def etl(input_df):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(input_df):
    return input_df.withColumn("int_col", F.col("str_col").cast("integer")) \
                   .withColumn("double_col", F.col("str_col").cast("double"))`,
    explanation: `Use cast() to convert types.`,
    hints: ["cast('integer'), cast('double'), cast('string')", "Also cast('date'), cast('timestamp')"]
  },

  {
    id: "52",
    title: "Order By Multiple Columns",
    difficulty: "Easy",
    category: "Transformations",
    description: `Sort DataFrame by multiple columns with mixed order.`,
    starterCode: PYSPARK_IMPORTS + `def etl(input_df):
    # Write code here
    pass`,
    solution: PYSPARK_IMPORTS + `def etl(input_df):
    return input_df.orderBy(
        F.col("category").asc(),
        F.col("date").desc()
    )`,
    explanation: `Use asc() and desc() for sort direction.`,
    hints: ["F.col('name').asc() for ascending", "F.col('name').desc() for descending", "Can mix in same orderBy"]
  }
];

export const getProblemById = (id: string): Problem | undefined => {
  return problems.find((p) => p.id === id);
};
