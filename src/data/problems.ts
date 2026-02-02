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
  | "Pivoting"
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
  "Pivoting",
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

// Problems from ZillaCode (Apache 2.0 License) - adapted for PySpark learning
export const problems: Problem[] = [
  // Problem 1 (ZillaCode ID: 11) - Filter Null Values
  {
    id: "1",
    title: "Filter Null Values",
    difficulty: "Easy",
    category: "Null Handling",
    description: `**Movies**

You have been given a DataFrame \`movies_df\` containing information about the movies released in the year 2022. The schema of \`movies_df\` is as follows:

**movies_df**

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

**movies_df**
| movie_id | movie_title                                  | director_name   | release_date | box_office_collection | genre                              |
|----------|----------------------------------------------|-----------------|--------------|-----------------------|------------------------------------|
| 1        | The Avengers                                 | Joss Whedon     | 2022-05-06   | 1856.45               | Action, Adventure, Sci-Fi          |
| 2        | Black Panther: Wakanda Forever               | Ryan Coogler    | 2022-11-10   | NULL                  | Action, Adventure, Drama           |
| 3        | Jurassic World: Dominion                     | Colin Trevorrow | 2022-06-10   | 1234.56               | Action, Adventure, Sci-Fi          |
| 4        | Fantastic Beasts and Where to Find Them 3    | David Yates     | 2022-11-04   | NULL                  | Adventure, Family, Fantasy         |
| 5        | Sonic the Hedgehog 2                         | Jeff Fowler     | 2022-04-08   | 789.12                | Action, Adventure, Comedy          |
| 6        | The Batman                                   | Matt Reeves     | 2022-03-04   | 2345.67               | Action, Adventure, Crime           |
| 7        | Avatar 2                                     | James Cameron   | 2022-12-16   | 5678.90               | Action, Adventure, Fantasy, Sci-Fi |
| 8        | Doctor Strange in the Multiverse of Madness  | Sam Raimi       | 2022-03-25   | 4567.89               | Action, Adventure, Fantasy         |

**Output**
| box_office_collection | director_name | genre                      | movie_id | movie_title                               | release_date |
|-----------------------|---------------|----------------------------|----------|-------------------------------------------|--------------|
| NULL                  | Ryan Coogler  | Action, Adventure, Drama   | 2        | Black Panther: Wakanda Forever            | 2022-11-10   |
| NULL                  | David Yates   | Adventure, Family, Fantasy | 4        | Fantastic Beasts and Where to Find Them 3 | 2022-11-04   |`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(movies_df):
    # Filter movies_df to retain only rows where box_office_collection is null
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
3. The \`filter\` method is used on the \`movies_df\` DataFrame to filter out rows where the \`box_office_collection\` column is null.
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
            { movie_id: 4, movie_title: "Fantastic Beasts and Where to Find Them 3", director_name: "David Yates", release_date: "2022-11-04", box_office_collection: null, genre: "Adventure, Family, Fantasy" },
          ]
        },
        expected_output: [
          { movie_id: 2, movie_title: "Black Panther: Wakanda Forever", director_name: "Ryan Coogler", release_date: "2022-11-10", box_office_collection: null, genre: "Action, Adventure, Drama" },
          { movie_id: 4, movie_title: "Fantastic Beasts and Where to Find Them 3", director_name: "David Yates", release_date: "2022-11-04", box_office_collection: null, genre: "Adventure, Family, Fantasy" },
        ]
      }
    ]
  },

  // Problem 2 (ZillaCode ID: 8) - Call Center Aggregation
  {
    id: "2",
    title: "Call Center Aggregation",
    difficulty: "Medium",
    category: "Aggregations",
    description: `**Call Center**

You are given two DataFrames, \`calls_df\` and \`customers_df\`, which contain information about calls made by customers of a telecommunications company and information about the customers, respectively.

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
| date           | string  | date when the calls were made (yyyy-MM-dd)   |
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

**customers_df**
| cust_id | name    | state | tenure | occupation |
|---------|---------|-------|--------|------------|
| 1       | Alice   | NY    | 10     | doctor     |
| 2       | Bob     | CA    | 12     | lawyer     |
| 3       | Charlie | TX    | 6      | engineer   |

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
    # Return num_customers and total_duration per date
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
        F.count_distinct("cust_id").alias("num_customers"),
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
- \`F.sum()\`: Calculates sum of values

**Complexity:**
- Time: O(n log n) for join + groupby operations
- Space: O(n) for intermediate results`,
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

  // Additional problems - placeholder structure for remaining ZillaCode problems
  // You can provide the full constants.py file content to populate all 52
  {
    id: "3",
    title: "Filter with Multiple Conditions",
    difficulty: "Easy",
    category: "Filtering",
    description: `Given an \`employees_df\` DataFrame, filter to return only employees from the Engineering department who are older than 25.

**employees_df Schema:**
| Column Name | Data Type |
|-------------|-----------|
| name        | string    |
| age         | integer   |
| department  | string    |
| salary      | float     |

**Example Input:**
| name    | age | department  | salary |
|---------|-----|-------------|--------|
| Alice   | 30  | Engineering | 80000  |
| Bob     | 25  | Marketing   | 60000  |
| Charlie | 35  | Engineering | 90000  |
| Eve     | 22  | Engineering | 55000  |

**Expected Output:**
| name    | age | department  | salary |
|---------|-----|-------------|--------|
| Alice   | 30  | Engineering | 80000  |
| Charlie | 35  | Engineering | 90000  |`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(employees_df):
    # Filter for Engineering department AND age > 25
    # Write code here
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(employees_df):
    filtered_df = employees_df.filter(
        (F.col("department") == "Engineering") & (F.col("age") > 25)
    )
    return filtered_df`,
    explanation: `Use the & operator to combine multiple conditions. Each condition should be wrapped in parentheses. F.col() is used to reference columns.`,
    hints: [
      "Use & to combine conditions (AND)",
      "Wrap each condition in parentheses",
      "Use F.col('column_name') to reference columns"
    ]
  },

  {
    id: "4",
    title: "Add Calculated Column",
    difficulty: "Easy",
    category: "Transformations",
    description: `Given a \`sales_df\` DataFrame with \`quantity\` and \`unit_price\` columns, add a new column \`total_amount\` that is the product of quantity and unit_price.

**sales_df Schema:**
| Column Name | Data Type |
|-------------|-----------|
| sale_id     | integer   |
| product     | string    |
| quantity    | integer   |
| unit_price  | float     |

**Expected Output:**
Add a \`total_amount\` column = quantity * unit_price`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(sales_df):
    # Add total_amount column = quantity * unit_price
    # Write code here
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(sales_df):
    result_df = sales_df.withColumn(
        "total_amount",
        F.col("quantity") * F.col("unit_price")
    )
    return result_df`,
    explanation: `Use withColumn() to add a new column. The first argument is the column name, the second is the expression.`,
    hints: [
      "Use withColumn() to add new columns",
      "F.col() lets you reference existing columns",
      "Arithmetic operators work on column expressions"
    ]
  },

  {
    id: "5",
    title: "Group By and Count",
    difficulty: "Easy",
    category: "Aggregations",
    description: `Count the number of employees in each department.

**employees_df Schema:**
| Column Name | Data Type |
|-------------|-----------|
| emp_id      | integer   |
| name        | string    |
| department  | string    |

**Expected Output:**
| department  | count |
|-------------|-------|
| Engineering | 5     |
| Marketing   | 3     |
| HR          | 2     |`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(employees_df):
    # Count employees per department
    # Write code here
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(employees_df):
    result_df = employees_df.groupBy("department").count()
    return result_df`,
    explanation: `Use groupBy() followed by count() for simple counting. The result has the grouping column and a 'count' column.`,
    hints: [
      "groupBy() groups rows by column values",
      "count() is a simple aggregation",
      "The result column is named 'count'"
    ]
  },

  {
    id: "6",
    title: "Inner Join Two DataFrames",
    difficulty: "Medium",
    category: "Joins",
    description: `Join employees with their department details using an inner join on dept_id.

**employees_df:**
| emp_id | name    | dept_id |
|--------|---------|---------|
| 1      | Alice   | 101     |
| 2      | Bob     | 102     |
| 3      | Charlie | 101     |

**departments_df:**
| dept_id | dept_name   | location    |
|---------|-------------|-------------|
| 101     | Engineering | New York    |
| 102     | Marketing   | Los Angeles |
| 103     | HR          | Chicago     |

**Expected Output:**
Employees with their department info (only matching records).`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(employees_df, departments_df):
    # Join employees with departments on dept_id
    # Write code here
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(employees_df, departments_df):
    joined_df = employees_df.join(
        departments_df,
        employees_df.dept_id == departments_df.dept_id,
        "inner"
    ).drop(departments_df.dept_id)
    return joined_df`,
    explanation: `Use join() with the join condition and type. Drop duplicate columns after joining.`,
    hints: [
      "join() takes DataFrame, condition, and type",
      "Drop duplicate columns after join",
      "Inner join only includes matching rows"
    ]
  },

  {
    id: "7",
    title: "Window Function - Row Number",
    difficulty: "Medium",
    category: "Window Functions",
    description: `Add a row number to each employee within their department, ordered by salary descending.

**employees_df Schema:**
| Column Name | Data Type |
|-------------|-----------|
| emp_id      | integer   |
| name        | string    |
| department  | string    |
| salary      | float     |

**Expected Output:**
Add a \`rank\` column that numbers employees 1, 2, 3... within each department by salary (highest first).`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(employees_df):
    # Add row_number within each department ordered by salary desc
    # Write code here
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(employees_df):
    window_spec = W.partitionBy("department").orderBy(F.col("salary").desc())
    
    result_df = employees_df.withColumn(
        "rank",
        F.row_number().over(window_spec)
    )
    return result_df`,
    explanation: `Window functions operate over a "window" of rows. partitionBy defines groups, orderBy defines sort order within groups.`,
    hints: [
      "Create a Window specification with W.partitionBy().orderBy()",
      "Use F.row_number().over(window_spec)",
      "desc() for descending order"
    ]
  },

  {
    id: "8",
    title: "String Manipulation",
    difficulty: "Easy",
    category: "String Functions",
    description: `Given a \`users_df\` DataFrame, create a new column \`full_name\` by concatenating \`first_name\` and \`last_name\` with a space in between.

**users_df Schema:**
| Column Name | Data Type |
|-------------|-----------|
| user_id     | integer   |
| first_name  | string    |
| last_name   | string    |
| email       | string    |

**Expected Output:**
Add \`full_name\` column with "FirstName LastName" format.`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(users_df):
    # Concatenate first_name and last_name into full_name
    # Write code here
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(users_df):
    result_df = users_df.withColumn(
        "full_name",
        F.concat(F.col("first_name"), F.lit(" "), F.col("last_name"))
    )
    return result_df`,
    explanation: `F.concat() joins multiple columns/values. F.lit() creates a literal value (the space).`,
    hints: [
      "Use F.concat() to join strings",
      "F.lit(' ') creates a literal space",
      "Alternative: F.concat_ws(' ', col1, col2)"
    ]
  },

  {
    id: "9",
    title: "Fill Null Values",
    difficulty: "Easy",
    category: "Null Handling",
    description: `Replace null values in the \`salary\` column with 0.

**employees_df Schema:**
| Column Name | Data Type |
|-------------|-----------|
| emp_id      | integer   |
| name        | string    |
| salary      | float     |

**Expected Output:**
All null salaries should become 0.`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(employees_df):
    # Replace null salaries with 0
    # Write code here
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(employees_df):
    result_df = employees_df.fillna({"salary": 0})
    return result_df`,
    explanation: `fillna() replaces null values. Pass a dictionary to specify different fill values for different columns.`,
    hints: [
      "Use fillna() to replace nulls",
      "Pass a dict like {'column': value}",
      "Alternative: F.coalesce(col, F.lit(0))"
    ]
  },

  {
    id: "10",
    title: "Date Extraction",
    difficulty: "Easy",
    category: "Date Functions",
    description: `Extract the year and month from the \`order_date\` column.

**orders_df Schema:**
| Column Name | Data Type |
|-------------|-----------|
| order_id    | integer   |
| order_date  | date      |
| amount      | float     |

**Expected Output:**
Add \`order_year\` and \`order_month\` columns.`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(orders_df):
    # Extract year and month from order_date
    # Write code here
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(orders_df):
    result_df = orders_df.withColumn(
        "order_year", F.year("order_date")
    ).withColumn(
        "order_month", F.month("order_date")
    )
    return result_df`,
    explanation: `F.year() and F.month() extract components from date columns. Other options: F.day(), F.dayofweek(), F.quarter().`,
    hints: [
      "F.year() extracts the year",
      "F.month() extracts the month (1-12)",
      "Chain withColumn() calls for multiple new columns"
    ]
  },
];

export const getProblemById = (id: string): Problem | undefined => {
  return problems.find(problem => problem.id === id);
};

export const getProblemsByCategory = (category: Category): Problem[] => {
  return problems.filter(problem => problem.category === category);
};

export const getProblemsByDifficulty = (difficulty: Difficulty): Problem[] => {
  return problems.filter(problem => problem.difficulty === difficulty);
};
