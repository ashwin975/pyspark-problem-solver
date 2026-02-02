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
  | "UDFs";

// All unique categories from problems
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
  "UDFs"
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

// Problems from ZillaCode (Apache 2.0 License) - PySpark learning platform
export const problems: Problem[] = [
  // Problem 1 - Filter Null Values (ZillaCode ID: 11)
  {
    id: "1",
    title: "Filter Null Values",
    difficulty: "Easy",
    category: "Null Handling",
    description: `**Movies**

You have been given a DataFrame \`movies_df\` containing information about the movies released in the year 2022.

**movies_df Schema**

| Column Name           | Data Type | Description                        |
|-----------------------|-----------|------------------------------------|
| movie_id              | integer   | unique id of the movie             |
| movie_title           | string    | title of the movie                 |
| director_name         | string    | name of the movie director         |
| release_date          | date      | date when the movie was released   |
| box_office_collection | float     | box office collection of the movie |
| genre                 | string    | genre of the movie                 |

Filter \`movies_df\` to retain only the rows where the \`box_office_collection\` column is null.

**Result Schema**

| Column Name           | Data Type |
|-----------------------|-----------|
| movie_id              | integer   |
| movie_title           | string    |
| director_name         | string    |
| release_date          | date      |
| box_office_collection | float     |
| genre                 | string    |

**Example**

| movie_id | movie_title                                  | director_name   | release_date | box_office_collection | genre                              |
|----------|----------------------------------------------|-----------------|--------------|-----------------------|------------------------------------|
| 1        | The Avengers                                 | Joss Whedon     | 2022-05-06   | 1856.45               | Action, Adventure, Sci-Fi          |
| 2        | Black Panther: Wakanda Forever               | Ryan Coogler    | 2022-11-10   | NULL                  | Action, Adventure, Drama           |
| 3        | Jurassic World: Dominion                     | Colin Trevorrow | 2022-06-10   | 1234.56               | Action, Adventure, Sci-Fi          |
| 4        | Fantastic Beasts 3                           | David Yates     | 2022-11-04   | NULL                  | Adventure, Family, Fantasy         |

**Output**

| movie_id | movie_title                    | director_name | box_office_collection | genre                      |
|----------|--------------------------------|---------------|----------------------|----------------------------|
| 2        | Black Panther: Wakanda Forever | Ryan Coogler  | NULL                 | Action, Adventure, Drama   |
| 4        | Fantastic Beasts 3             | David Yates   | NULL                 | Adventure, Family, Fantasy |`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(movies_df):
    # Write code here
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(movies_df):
    null_box_office_df = movies_df.filter(
        F.isnull("box_office_collection")
    )

    return null_box_office_df`,
    explanation: `The PySpark solution uses the \`isnull\` function from the \`pyspark.sql.functions\` module to filter out rows where the \`box_office_collection\` column is null.

**Step-by-step:**
1. The \`isnull\` function is imported from PySpark's \`pyspark.sql.functions\` module.
2. The \`etl\` function takes a DataFrame \`movies_df\` as input.
3. The \`filter\` method is used on the \`movies_df\` DataFrame to filter rows where the \`box_office_collection\` column is null.
4. The resulting DataFrame is returned.

**Complexity:**
- Space: O(n) where n is the number of rows
- Time: O(n) - linear scan of all rows`,
    hints: [
      "Use F.isnull() to check for null values in a column",
      "The filter() method accepts a column expression that returns boolean",
      "Alternative syntax: F.col('column').isNull()"
    ],
    testCases: [
      {
        input: {
          movies_df: [
            { movie_id: 1, movie_title: "The Avengers", director_name: "Joss Whedon", release_date: "2022-05-06", box_office_collection: 1856.45, genre: "Action, Adventure, Sci-Fi" },
            { movie_id: 2, movie_title: "Black Panther: Wakanda Forever", director_name: "Ryan Coogler", release_date: "2022-11-10", box_office_collection: null, genre: "Action, Adventure, Drama" },
            { movie_id: 3, movie_title: "Jurassic World: Dominion", director_name: "Colin Trevorrow", release_date: "2022-06-10", box_office_collection: 1234.56, genre: "Action, Adventure, Sci-Fi" },
            { movie_id: 4, movie_title: "Fantastic Beasts 3", director_name: "David Yates", release_date: "2022-11-04", box_office_collection: null, genre: "Adventure, Family, Fantasy" },
          ]
        },
        expected_output: [
          { box_office_collection: null, director_name: "Ryan Coogler", genre: "Action, Adventure, Drama", movie_id: 2, movie_title: "Black Panther: Wakanda Forever", release_date: "2022-11-10" },
          { box_office_collection: null, director_name: "David Yates", genre: "Adventure, Family, Fantasy", movie_id: 4, movie_title: "Fantastic Beasts 3", release_date: "2022-11-04" },
        ]
      }
    ]
  },

  // Problem 2 - Call Center Aggregation (ZillaCode ID: 8)
  {
    id: "2",
    title: "Call Center Aggregation",
    difficulty: "Medium",
    category: "Aggregations",
    description: `**Call Center**

You are given two DataFrames, \`calls_df\` and \`customers_df\`, which contain information about calls made by customers of a telecommunications company.

**calls_df Schema:**

| Column   | Type    | Description                              |
|----------|---------|------------------------------------------|
| call_id  | integer | unique identifier of each call           |
| cust_id  | integer | unique identifier of the customer        |
| date     | string  | date when the call was made (yyyy-MM-dd) |
| duration | integer | duration of the call in seconds          |

**customers_df Schema:**

| Column     | Type    | Description                              |
|------------|---------|------------------------------------------|
| cust_id    | integer | unique identifier of each customer       |
| name       | string  | name of the customer                     |
| state      | string  | state where the customer lives           |
| tenure     | integer | months the customer has been with company|
| occupation | string  | occupation of the customer               |

Write a function that returns the number of distinct customers who made calls on each date, along with the total duration of calls made on each date.

**Output Schema:**

| Column         | Type    | Description                                  |
|----------------|---------|----------------------------------------------|
| date           | string  | date when the calls were made                |
| num_customers  | integer | number of distinct customers on that date    |
| total_duration | integer | total duration of calls on that date         |

**Example**

**calls_df**
| call_id | cust_id | date       | duration |
|---------|---------|------------|----------|
| 1       | 1       | 2022-01-01 | 100      |
| 2       | 2       | 2022-01-01 | 200      |
| 3       | 1       | 2022-01-02 | 150      |
| 4       | 3       | 2022-01-02 | 300      |
| 5       | 2       | 2022-01-03 | 50       |

**Output**
| date       | num_customers | total_duration |
|------------|---------------|----------------|
| 2022-01-01 | 2             | 300            |
| 2022-01-02 | 2             | 450            |
| 2022-01-03 | 1             | 50             |`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(calls_df, customers_df):
    # Write code here
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(calls_df, customers_df):
    # Join calls_df and customers_df on cust_id column
    joined_df = calls_df.join(
        customers_df, "cust_id"
    )

    # Aggregate the joined DataFrame by date
    agg_df = joined_df.groupby("date").agg(
        F.count_distinct("cust_id").alias(
            "num_customers"
        ),
        F.sum("duration").alias("total_duration"),
    )

    return agg_df`,
    explanation: `The solution joins the calls and customers DataFrames, then aggregates by date.

**Step-by-step:**
1. Join \`calls_df\` with \`customers_df\` on \`cust_id\`
2. Group the result by \`date\`
3. Use \`F.count_distinct("cust_id")\` to count unique customers per date
4. Use \`F.sum("duration")\` to get total call duration per date
5. Return the aggregated DataFrame

**Key Functions:**
- \`join()\`: Combines two DataFrames on a common column
- \`groupby()\`: Groups rows by specified column(s)
- \`F.count_distinct()\`: Counts unique values
- \`F.sum()\`: Calculates sum of values`,
    hints: [
      "Join the DataFrames first on cust_id",
      "Use groupby('date') to group by date",
      "F.count_distinct() counts unique values in a column",
      "Use .alias() to name your aggregated columns"
    ],
    testCases: [
      {
        input: {
          calls_df: [
            { call_id: 1, cust_id: 1, date: "2022-01-01", duration: 100 },
            { call_id: 2, cust_id: 2, date: "2022-01-01", duration: 200 },
            { call_id: 3, cust_id: 1, date: "2022-01-02", duration: 150 },
            { call_id: 4, cust_id: 3, date: "2022-01-02", duration: 300 },
            { call_id: 5, cust_id: 2, date: "2022-01-03", duration: 50 },
          ],
          customers_df: [
            { cust_id: 1, name: "Alice", state: "NY", tenure: 10, occupation: "doctor" },
            { cust_id: 2, name: "Bob", state: "CA", tenure: 12, occupation: "lawyer" },
            { cust_id: 3, name: "Charlie", state: "TX", tenure: 6, occupation: "engineer" },
          ]
        },
        expected_output: [
          { date: "2022-01-01", num_customers: 2, total_duration: 300 },
          { date: "2022-01-02", num_customers: 2, total_duration: 450 },
          { date: "2022-01-03", num_customers: 1, total_duration: 50 },
        ]
      }
    ]
  },

  // Problem 3 - Full Outer Join (ZillaCode PE Firms problem)
  {
    id: "3",
    title: "PE Firms Full Join",
    difficulty: "Medium",
    category: "Joins",
    description: `**Private Equity Data**

You are given three DataFrames containing information about private equity firms, their funds, and investments.

**pe_firms Schema:**

| Column      | Type    | Description                    |
|-------------|---------|--------------------------------|
| firm_id     | integer | unique identifier of firm      |
| firm_name   | string  | name of the firm               |
| founded_year| integer | year the firm was founded      |
| location    | string  | location of the firm           |

**pe_funds Schema:**

| Column          | Type    | Description                |
|-----------------|---------|----------------------------|
| fund_id         | integer | unique identifier of fund  |
| firm_id         | integer | firm that manages the fund |
| fund_name       | string  | name of the fund           |
| fund_size       | integer | size of the fund           |
| fund_start_year | integer | year fund started          |
| fund_end_year   | integer | year fund ended            |

**pe_investments Schema:**

| Column           | Type    | Description               |
|------------------|---------|---------------------------|
| investment_id    | integer | unique investment id      |
| fund_id          | integer | fund making investment    |
| company_name     | string  | company invested in       |
| investment_amount| float   | amount invested           |
| investment_date  | date    | date of investment        |

Write a function to perform a full outer join on all three DataFrames, keeping all records from all tables.`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(pe_firms, pe_funds, pe_investments):
    # Write code here
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(pe_firms, pe_funds, pe_investments):
    # join the three dataframes using full join
    joined_df = pe_firms.join(
        pe_funds, "firm_id", "outer"
    ).join(pe_investments, "fund_id", "outer")

    # filter out any rows where all columns are null
    joined_df = joined_df.dropna(how="all")

    return joined_df`,
    explanation: `The solution performs full outer joins to combine all three DataFrames.

**Step-by-step:**
1. Join \`pe_firms\` with \`pe_funds\` on \`firm_id\` using outer join
2. Join the result with \`pe_investments\` on \`fund_id\` using outer join
3. Drop rows where all columns are null
4. Return the combined DataFrame

**Key Concepts:**
- Full outer join preserves all records from both sides
- Chain joins for multiple DataFrames
- Use dropna(how="all") to remove completely null rows`,
    hints: [
      "Use 'outer' as the join type for full outer join",
      "Chain multiple joins together",
      "The join column should match between DataFrames",
      "dropna(how='all') removes rows with all nulls"
    ]
  },

  // Problem 4 - Window Function Row Number
  {
    id: "4",
    title: "Author Row Numbers",
    difficulty: "Medium",
    category: "Window Functions",
    description: `**Research Papers**

You have two DataFrames: \`research_papers\` and \`authors\`.

**research_papers Schema:**

| Column   | Type    | Description                |
|----------|---------|----------------------------|
| paper_id | string  | unique paper identifier    |
| title    | string  | title of the paper         |
| year     | integer | publication year           |

**authors Schema:**

| Column    | Type   | Description              |
|-----------|--------|--------------------------|
| paper_id  | string | paper the author wrote   |
| author_id | string | unique author identifier |
| name      | string | author's name            |

Add a \`row_number\` column to the authors DataFrame that assigns a unique row number to each author within their respective paper, ordered by author_id.

**Example Output:**

| paper_id | author_id | name          | row_number |
|----------|-----------|---------------|------------|
| P1       | A1        | Alice Smith   | 1          |
| P1       | A2        | Bob Johnson   | 2          |
| P2       | A3        | Carol Williams| 1          |`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(research_papers, authors):
    # Write code here
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(research_papers, authors):
    author_window = W.partitionBy(
        "paper_id"
    ).orderBy("author_id")
    result = authors.withColumn(
        "row_number",
        F.row_number().over(author_window),
    )
    return result`,
    explanation: `The solution uses Window functions to assign row numbers within groups.

**Step-by-step:**
1. Create a Window specification partitioned by \`paper_id\` and ordered by \`author_id\`
2. Use \`F.row_number().over(window)\` to assign sequential numbers
3. Add the row_number column using \`withColumn\`

**Key Concepts:**
- Window.partitionBy() defines groups
- Window.orderBy() defines ordering within groups
- row_number() starts at 1 for each partition`,
    hints: [
      "Create a Window with partitionBy and orderBy",
      "Use F.row_number().over(window)",
      "The Window should partition by paper_id",
      "Order by author_id within each partition"
    ]
  },

  // Problem 5 - Regex Extract
  {
    id: "5",
    title: "Extract Numeric Age",
    difficulty: "Medium",
    category: "Regex",
    description: `**Geology Samples**

A geologist is working with a dataset containing rock samples. The dataset has the following schema:

| Column Name | Data Type |
|-------------|-----------|
| sample_id   | string    |
| description | string    |

The \`description\` field contains a mixture of letters and numbers. Extract the numeric parts from the description column and create a new column called \`age\`.

**Example**

**Input:**
| sample_id | description     |
|-----------|-----------------|
| S1        | Basalt_450Ma    |
| S2        | Sandstone_300Ma |
| S3        | Limestone       |
| S4        | Granite_200Ma   |
| S5        | Marble_1800Ma   |

**Output:**
| sample_id | description     | age  |
|-----------|-----------------|------|
| S1        | Basalt_450Ma    | 450  |
| S2        | Sandstone_300Ma | 300  |
| S3        | Limestone       |      |
| S4        | Granite_200Ma   | 200  |
| S5        | Marble_1800Ma   | 1800 |`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(input_df):
    # Write code here
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(input_df):
    output_df = input_df.withColumn(
        "age",
        F.regexp_extract(
            F.col("description"), r"(\\d+)", 0
        ),
    )
    return output_df`,
    explanation: `The solution uses \`regexp_extract\` to extract numeric parts from strings.

**Step-by-step:**
1. Use \`F.regexp_extract()\` with a regex pattern matching digits
2. The pattern \`(\\d+)\` matches one or more digits
3. The third argument (0) specifies which capture group to extract
4. Empty string is returned when no match is found

**Key Concepts:**
- regexp_extract(column, pattern, group_index)
- \\d+ matches one or more digits
- Group 0 returns the entire match`,
    hints: [
      "Use F.regexp_extract() for regex operations",
      "The pattern \\d+ matches digits",
      "Use withColumn to add the new column",
      "Returns empty string if no match found"
    ]
  },

  // Problem 6 - ML Model Analytics
  {
    id: "6",
    title: "ML Model Analytics",
    difficulty: "Hard",
    category: "Aggregations",
    description: `**Machine Learning Models**

You have two DataFrames tracking ML model performance:

**df_models Schema:**

| Column     | Type   | Description          |
|------------|--------|----------------------|
| Model_ID   | string | unique model ID      |
| Model_Name | string | name of the model    |
| Model_Type | string | type of model        |
| Accuracy   | float  | model accuracy       |

**df_usage Schema:**

| Column   | Type    | Description           |
|----------|---------|-----------------------|
| Model_ID | string  | model identifier      |
| Date     | string  | date of usage         |
| Uses     | integer | number of uses        |

Calculate:
1. Total uses per model (sum of Uses)
2. Average accuracy per model type

Join and return a DataFrame with Model_ID, Model_Name, Model_Type, Accuracy, Total_Uses, and Average_Accuracy.`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(df_models, df_usage):
    # Write code here
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(df_models, df_usage):
    df_usage_agg = df_usage.groupBy(
        "Model_ID"
    ).agg(F.sum("Uses").alias("Total_Uses"))

    df_model_avg_accuracy = df_models.groupBy(
        "Model_Type"
    ).agg(
        F.avg("Accuracy").alias(
            "Average_Accuracy"
        )
    )

    df_models_joined = df_models.join(
        df_usage_agg, on="Model_ID", how="inner"
    ).join(
        df_model_avg_accuracy,
        on="Model_Type",
        how="inner",
    )

    return df_models_joined`,
    explanation: `The solution combines aggregations with joins.

**Step-by-step:**
1. Aggregate usage data by Model_ID to get Total_Uses
2. Aggregate models by Model_Type to get Average_Accuracy
3. Join models with usage aggregation on Model_ID
4. Join with accuracy aggregation on Model_Type

**Key Concepts:**
- Multiple aggregations on different groupings
- Chain joins to combine results
- Use appropriate join keys for each join`,
    hints: [
      "Aggregate usage separately by Model_ID",
      "Aggregate accuracy by Model_Type",
      "Chain joins to combine all data",
      "Inner join ensures matching records only"
    ]
  },

  // Problem 7 - Row Number by Date
  {
    id: "7",
    title: "Product Manufacturing Order",
    difficulty: "Medium",
    category: "Window Functions",
    description: `**Manufacturing Data**

You have two DataFrames with product and manufacturing information:

**df1 Schema:**

| Column             | Type   | Description            |
|--------------------|--------|------------------------|
| product_id         | string | unique product ID      |
| manufacturing_date | date   | manufacturing date     |
| manufacturing_loc  | string | manufacturing location |

**df2 Schema:**

| Column       | Type   | Description        |
|--------------|--------|--------------------|
| product_id   | string | product identifier |
| product_name | string | name of product    |
| product_type | string | type of product    |

Join the DataFrames and add a row_number column based on manufacturing_date order.`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(df1, df2):
    # Write code here
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(df1, df2):
    joined_df = df1.join(
        df2, on="product_id", how="inner"
    )

    window = W.orderBy(
        joined_df.manufacturing_date
    )

    result_df = joined_df.withColumn(
        "row_number", F.row_number().over(window)
    )

    return result_df`,
    explanation: `The solution joins DataFrames and adds sequential row numbers.

**Step-by-step:**
1. Join df1 and df2 on product_id
2. Create a Window ordered by manufacturing_date
3. Apply row_number() over the window
4. Return the result with the new column

**Key Concepts:**
- Window without partitionBy applies to entire dataset
- orderBy determines row number sequence
- Inner join only keeps matching records`,
    hints: [
      "Join on product_id first",
      "Window.orderBy without partitionBy numbers all rows",
      "row_number() assigns sequential numbers",
      "Use withColumn to add the new column"
    ]
  },

  // Problem 8 - Union User Interactions
  {
    id: "8",
    title: "User Interactions Union",
    difficulty: "Medium",
    category: "Transformations",
    description: `**Social Media Interactions**

You have three separate DataFrames representing user interactions on a website:

**page_visits Schema:**

| Column     | Type      | Description            |
|------------|-----------|------------------------|
| user_id    | string    | user identifier        |
| page_id    | string    | page visited           |
| visit_time | timestamp | time of visit          |

**page_likes Schema:**

| Column    | Type      | Description            |
|-----------|-----------|------------------------|
| user_id   | string    | user identifier        |
| page_id   | string    | page liked             |
| like_time | timestamp | time of like           |

**page_comments Schema:**

| Column       | Type      | Description            |
|--------------|-----------|------------------------|
| user_id      | string    | user identifier        |
| page_id      | string    | page commented on      |
| comment_time | timestamp | time of comment        |

Combine all interactions into a single DataFrame with columns: user_id, page_id, interaction_time, interaction_type (visit/like/comment).`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(page_visits, page_likes, page_comments):
    # Write code here
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(page_visits, page_likes, page_comments):
    # Add interaction type to each DataFrame
    visits = page_visits.withColumn(
        "interaction_type", F.lit("visit")
    ).withColumnRenamed("visit_time", "interaction_time")
    
    likes = page_likes.withColumn(
        "interaction_type", F.lit("like")
    ).withColumnRenamed("like_time", "interaction_time")
    
    comments = page_comments.withColumn(
        "interaction_type", F.lit("comment")
    ).withColumnRenamed("comment_time", "interaction_time")

    # Union all three DataFrames
    user_interactions = visits.select(
        "user_id", "page_id", "interaction_time", "interaction_type"
    ).union(
        likes.select("user_id", "page_id", "interaction_time", "interaction_type")
    ).union(
        comments.select("user_id", "page_id", "interaction_time", "interaction_type")
    )

    # Sort by interaction_time
    result = user_interactions.orderBy("interaction_time")

    return result`,
    explanation: `The solution combines multiple DataFrames with union.

**Step-by-step:**
1. Add interaction_type column to each DataFrame using F.lit()
2. Rename timestamp columns to a common name
3. Select columns in the same order for union
4. Chain union() calls to combine all DataFrames
5. Sort by interaction_time

**Key Concepts:**
- F.lit() creates a literal column value
- withColumnRenamed() for consistent column names
- union() requires same column order
- select() ensures consistent column ordering`,
    hints: [
      "Add a literal column for interaction_type using F.lit()",
      "Rename timestamp columns to match",
      "Use select to ensure same column order before union",
      "Chain union() calls for multiple DataFrames"
    ]
  },

  // Problem 9 - Background Check UDF
  {
    id: "9",
    title: "Background Check Processing",
    difficulty: "Hard",
    category: "UDFs",
    description: `**Background Checks**

You have a DataFrame with background check data where some columns contain comma-separated values:

**background_checks Schema:**

| Column             | Type   | Description                        |
|--------------------|--------|------------------------------------|
| check_id           | int    | unique check identifier            |
| full_name          | string | person's full name                 |
| dob                | date   | date of birth                      |
| criminal_record    | string | comma-separated criminal records   |
| employment_history | string | comma-separated jobs               |
| education_history  | string | comma-separated degrees            |
| address            | string | comma-separated addresses          |

Create new columns that count the items in each comma-separated field:
- crime_count
- jobs_count
- degrees_count
- places_lived_count`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(background_checks):
    # Write code here
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(background_checks):
    # creating UDF for counting items in comma separated string
    count_items = F.udf(
        lambda s: len(s.split(",")) if s else 0,
        pyspark.sql.types.IntegerType(),
    )

    # processing each column
    result_df = (
        background_checks.withColumn(
            "crime_count",
            count_items(F.col("criminal_record")),
        )
        .withColumn(
            "jobs_count",
            count_items(F.col("employment_history")),
        )
        .withColumn(
            "degrees_count",
            count_items(F.col("education_history")),
        )
        .withColumn(
            "places_lived_count",
            count_items(F.col("address")),
        )
    )

    # selecting only the required columns
    result_df = result_df.select(
        "check_id",
        "full_name",
        "dob",
        "crime_count",
        "jobs_count",
        "degrees_count",
        "places_lived_count",
    )
    return result_df`,
    explanation: `The solution uses a User Defined Function (UDF) to count items in comma-separated strings.

**Step-by-step:**
1. Define a UDF that splits a string by comma and returns the count
2. Register the UDF with F.udf() specifying the return type
3. Apply the UDF to each column using withColumn
4. Select only the required output columns

**Key Concepts:**
- F.udf() creates a User Defined Function
- UDFs can contain arbitrary Python logic
- Specify return type for proper DataFrame schema
- Chain withColumn calls for multiple new columns`,
    hints: [
      "Create a UDF that splits and counts",
      "Use F.udf() with a return type",
      "Apply UDF with F.col() to reference columns",
      "Chain withColumn for multiple new columns"
    ]
  },

  // Problem 10 - Full Outer Join Materials
  {
    id: "10",
    title: "Materials Full Outer Join",
    difficulty: "Easy",
    category: "Joins",
    description: `**Materials Science**

You have two DataFrames about experiments and materials:

**df_experiments Schema:**

| Column            | Type    | Description              |
|-------------------|---------|--------------------------|
| experiment_id     | integer | unique experiment ID     |
| material_id       | integer | material used            |
| experiment_date   | date    | date of experiment       |
| experiment_results| float   | results of experiment    |

**df_materials Schema:**

| Column        | Type    | Description             |
|---------------|---------|-------------------------|
| material_id   | integer | unique material ID      |
| material_name | string  | name of the material    |
| material_type | string  | type of material        |

Perform a full outer join to combine experiments with materials, keeping all records from both tables.`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(df_experiments, df_materials):
    # Write code here
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(df_experiments, df_materials):
    return df_experiments.join(
        df_materials, on="material_id", how="full"
    )`,
    explanation: `The solution performs a simple full outer join between two DataFrames.

**Key Concepts:**
- Full outer join keeps all records from both sides
- Use "full" or "outer" as the join type
- Null values appear where there's no match
- Join column should exist in both DataFrames`,
    hints: [
      "Use how='full' or how='outer' for full outer join",
      "The on parameter specifies the join column",
      "Both DataFrames must have the join column",
      "Nulls appear for non-matching rows"
    ]
  },

  // Problem 11 - Flight Data String Length
  {
    id: "11",
    title: "Flight Data String Lengths",
    difficulty: "Hard",
    category: "String Functions",
    description: `**Flight Information**

You have three DataFrames with flight data:

**flights Schema:**

| Column              | Type    | Description            |
|---------------------|---------|------------------------|
| flight_id           | integer | unique flight ID       |
| origin_airport      | string  | origin airport ID      |
| destination_airport | string  | destination airport ID |

**airports Schema:**

| Column       | Type   | Description           |
|--------------|--------|-----------------------|
| airport_id   | string | unique airport ID     |
| airport_name | string | name of the airport   |

**planes Schema:**

| Column      | Type    | Description         |
|-------------|---------|---------------------|
| plane_id    | integer | unique plane ID     |
| plane_model | string  | model of the plane  |

Join all three DataFrames and calculate the length of airport names and plane model names for each flight:
- origin_airport_name_length
- destination_airport_name_length
- plane_model_length`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(flights, airports, planes):
    # Write code here
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(flights, airports, planes):
    # Join flights with airports for origin
    flights_with_airports = flights.join(
        airports.withColumnRenamed(
            "airport_id", "origin_airport"
        ).withColumnRenamed(
            "airport_name", "origin_airport_name"
        ),
        on="origin_airport",
        how="left",
    ).join(
        airports.withColumnRenamed(
            "airport_id", "destination_airport"
        ).withColumnRenamed(
            "airport_name", "destination_airport_name",
        ),
        on="destination_airport",
        how="left",
    )

    # Join with planes
    flights_with_all = flights_with_airports.join(
        planes.withColumnRenamed(
            "plane_id", "flight_id"
        ).withColumnRenamed(
            "plane_model", "plane_model_name"
        ),
        on="flight_id",
        how="left",
    )

    # Calculate string lengths
    result = flights_with_all.select(
        F.col("flight_id"),
        F.length(F.col("origin_airport_name")).alias(
            "origin_airport_name_length"
        ),
        F.length(F.col("destination_airport_name")).alias(
            "destination_airport_name_length"
        ),
        F.length(F.col("plane_model_name")).alias(
            "plane_model_length"
        ),
    )

    return result`,
    explanation: `The solution performs multiple joins with column renaming and string operations.

**Step-by-step:**
1. Join flights with airports twice (for origin and destination)
2. Rename columns to avoid conflicts
3. Join with planes DataFrame
4. Use F.length() to calculate string lengths
5. Select only required columns

**Key Concepts:**
- Self-join patterns with column renaming
- F.length() calculates string length
- withColumnRenamed() for unique column names
- Chain joins for multiple DataFrames`,
    hints: [
      "Join airports twice with different column names",
      "Use withColumnRenamed to avoid column conflicts",
      "F.length() calculates string length",
      "Left join preserves all flights"
    ]
  },

  // Problem 12 - Filter with Column Expression
  {
    id: "12",
    title: "Filter Employees by Department",
    difficulty: "Easy",
    category: "Filtering",
    description: `**Employee Data**

Given an \`employees_df\` DataFrame, filter to return only employees from the Engineering department who have a salary greater than 75000.

**employees_df Schema:**

| Column Name | Data Type |
|-------------|-----------|
| emp_id      | integer   |
| name        | string    |
| department  | string    |
| salary      | float     |
| hire_date   | date      |

**Example Input:**
| emp_id | name    | department  | salary  |
|--------|---------|-------------|---------|
| 1      | Alice   | Engineering | 80000   |
| 2      | Bob     | Marketing   | 60000   |
| 3      | Charlie | Engineering | 70000   |
| 4      | Diana   | Engineering | 95000   |

**Expected Output:**
| emp_id | name  | department  | salary |
|--------|-------|-------------|--------|
| 1      | Alice | Engineering | 80000  |
| 4      | Diana | Engineering | 95000  |`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(employees_df):
    # Write code here
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(employees_df):
    filtered_df = employees_df.filter(
        (F.col("department") == "Engineering") & 
        (F.col("salary") > 75000)
    )
    return filtered_df`,
    explanation: `Use the & operator to combine multiple filter conditions.

**Key Concepts:**
- F.col() references a column by name
- Use & for AND conditions
- Each condition must be wrapped in parentheses
- == for equality, > < >= <= for comparisons`,
    hints: [
      "Use F.col() to reference columns",
      "Combine conditions with & (AND)",
      "Wrap each condition in parentheses",
      "Use == for string equality"
    ]
  },

  // Problem 13 - Add Calculated Column
  {
    id: "13",
    title: "Calculate Total Revenue",
    difficulty: "Easy",
    category: "Transformations",
    description: `**Sales Data**

Given a \`sales_df\` DataFrame, add a new column \`total_revenue\` that is the product of \`quantity\` and \`unit_price\`.

**sales_df Schema:**

| Column Name | Data Type |
|-------------|-----------|
| sale_id     | integer   |
| product     | string    |
| quantity    | integer   |
| unit_price  | float     |

**Example Input:**
| sale_id | product | quantity | unit_price |
|---------|---------|----------|------------|
| 1       | Widget  | 10       | 25.00      |
| 2       | Gadget  | 5        | 50.00      |

**Expected Output:**
| sale_id | product | quantity | unit_price | total_revenue |
|---------|---------|----------|------------|---------------|
| 1       | Widget  | 10       | 25.00      | 250.00        |
| 2       | Gadget  | 5        | 50.00      | 250.00        |`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(sales_df):
    # Write code here
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(sales_df):
    result_df = sales_df.withColumn(
        "total_revenue",
        F.col("quantity") * F.col("unit_price")
    )
    return result_df`,
    explanation: `Use withColumn() to add a new calculated column.

**Key Concepts:**
- withColumn(name, expression) adds or replaces a column
- Arithmetic operators work on column expressions
- F.col() references existing columns
- The new column is appended to the DataFrame`,
    hints: [
      "Use withColumn() to add new columns",
      "F.col() references existing columns",
      "Use * for multiplication",
      "Column expressions support arithmetic"
    ]
  },

  // Problem 14 - Group and Aggregate
  {
    id: "14",
    title: "Department Statistics",
    difficulty: "Easy",
    category: "Aggregations",
    description: `**Employee Statistics**

Given an \`employees_df\` DataFrame, calculate the average salary and employee count for each department.

**employees_df Schema:**

| Column Name | Data Type |
|-------------|-----------|
| emp_id      | integer   |
| name        | string    |
| department  | string    |
| salary      | float     |

**Expected Output Schema:**

| Column Name   | Data Type |
|---------------|-----------|
| department    | string    |
| avg_salary    | float     |
| employee_count| integer   |`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(employees_df):
    # Write code here
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(employees_df):
    result_df = employees_df.groupBy("department").agg(
        F.avg("salary").alias("avg_salary"),
        F.count("emp_id").alias("employee_count")
    )
    return result_df`,
    explanation: `Use groupBy() with multiple aggregations.

**Key Concepts:**
- groupBy() groups rows by column values
- agg() allows multiple aggregation functions
- F.avg() calculates average
- F.count() counts rows
- .alias() renames the output column`,
    hints: [
      "Use groupBy().agg() for multiple aggregations",
      "F.avg() calculates the average",
      "F.count() counts rows",
      "Use .alias() to name result columns"
    ]
  },

  // Problem 15 - Date Formatting
  {
    id: "15",
    title: "Format Date Column",
    difficulty: "Easy",
    category: "Date Functions",
    description: `**Order Data**

Given an \`orders_df\` DataFrame with an \`order_date\` column, add a new column \`order_month\` that extracts the month and year in "YYYY-MM" format.

**orders_df Schema:**

| Column Name | Data Type |
|-------------|-----------|
| order_id    | integer   |
| customer_id | integer   |
| order_date  | date      |
| amount      | float     |

**Example:**
order_date: 2023-03-15 → order_month: "2023-03"`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(orders_df):
    # Write code here
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(orders_df):
    result_df = orders_df.withColumn(
        "order_month",
        F.date_format(F.col("order_date"), "yyyy-MM")
    )
    return result_df`,
    explanation: `Use date_format() to format dates as strings.

**Key Concepts:**
- F.date_format(column, pattern) formats dates
- Pattern "yyyy-MM" gives year and month
- Common patterns: yyyy (year), MM (month), dd (day)
- Returns a string column`,
    hints: [
      "Use F.date_format() for date formatting",
      "Pattern 'yyyy-MM' gives year-month format",
      "The result is a string column",
      "Reference column with F.col()"
    ]
  }
];

// Helper function to get problem by ID
export const getProblemById = (id: string): Problem | undefined => {
  return problems.find(problem => problem.id === id);
};

// Helper function to get problems by category
export const getProblemsByCategory = (category: Category): Problem[] => {
  return problems.filter(problem => problem.category === category);
};

// Helper function to get problems by difficulty
export const getProblemsByDifficulty = (difficulty: Difficulty): Problem[] => {
  return problems.filter(problem => problem.difficulty === difficulty);
};
