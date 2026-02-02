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
  completed?: boolean;
}

// Problems inspired by ZillaCode (Apache 2.0 License) - adapted for PySpark learning
export const problems: Problem[] = [
  // ============= FILTERING =============
  {
    id: "1",
    title: "Filter Null Values",
    difficulty: "Easy",
    category: "Null Handling",
    description: `You have been given a DataFrame \`movies_df\` containing information about movies. Filter the DataFrame to retain only rows where the \`box_office_collection\` column is null.

**movies_df Schema:**
| Column Name           | Data Type |
|-----------------------|-----------|
| movie_id              | integer   |
| movie_title           | string    |
| director_name         | string    |
| release_date          | date      |
| box_office_collection | float     |
| genre                 | string    |

**Example Input:**
| movie_id | movie_title                    | director_name   | box_office_collection |
|----------|--------------------------------|-----------------|----------------------|
| 1        | The Avengers                   | Joss Whedon     | 1856.45              |
| 2        | Black Panther: Wakanda Forever | Ryan Coogler    | NULL                 |
| 3        | Jurassic World                 | Colin Trevorrow | 1234.56              |

**Expected Output:**
Only rows where box_office_collection is NULL.`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def etl(movies_df):
    # Filter movies_df to retain only rows where box_office_collection is null
    # Your code here
    
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def etl(movies_df):
    null_box_office_df = movies_df.filter(
        F.isnull("box_office_collection")
    )
    return null_box_office_df`,
    explanation: `The solution uses the \`isnull\` function from \`pyspark.sql.functions\` to filter rows where the \`box_office_collection\` column is null. The \`filter\` method iterates over each row and checks the condition.`,
    hints: [
      "Use F.isnull() to check for null values",
      "The filter() method accepts a column expression",
      "Alternative: F.col('column').isNull()"
    ]
  },
  {
    id: "2",
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

def etl(employees_df):
    # Filter for Engineering department AND age > 25
    # Your code here
    
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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
    id: "3",
    title: "Filter with OR Condition",
    difficulty: "Easy",
    category: "Filtering",
    description: `Filter a \`products_df\` DataFrame to get products that are either in the "Electronics" category OR have a price greater than 1000.

**products_df Schema:**
| Column Name | Data Type |
|-------------|-----------|
| product_id  | integer   |
| name        | string    |
| category    | string    |
| price       | float     |

**Expected Output:**
Products where category is "Electronics" OR price > 1000`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def etl(products_df):
    # Filter for Electronics category OR price > 1000
    # Your code here
    
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def etl(products_df):
    filtered_df = products_df.filter(
        (F.col("category") == "Electronics") | (F.col("price") > 1000)
    )
    return filtered_df`,
    explanation: `Use the | operator for OR conditions. Like AND conditions, wrap each in parentheses.`,
    hints: [
      "Use | for OR conditions",
      "Wrap each condition in parentheses",
      "Both conditions are checked independently"
    ]
  },

  // ============= TRANSFORMATIONS =============
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

def etl(sales_df):
    # Add total_amount column = quantity * unit_price
    # Your code here
    
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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
    title: "Rename Columns",
    difficulty: "Easy",
    category: "Transformations",
    description: `Rename the columns of a DataFrame from snake_case to camelCase:
- \`first_name\` → \`firstName\`
- \`last_name\` → \`lastName\`
- \`email_address\` → \`emailAddress\`

**users_df Schema:**
| Column Name   | Data Type |
|---------------|-----------|
| user_id       | integer   |
| first_name    | string    |
| last_name     | string    |
| email_address | string    |`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def etl(users_df):
    # Rename columns to camelCase
    # Your code here
    
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def etl(users_df):
    result_df = users_df.withColumnRenamed("first_name", "firstName") \\
                        .withColumnRenamed("last_name", "lastName") \\
                        .withColumnRenamed("email_address", "emailAddress")
    return result_df`,
    explanation: `Chain withColumnRenamed() calls to rename multiple columns. The first argument is the old name, second is the new name.`,
    hints: [
      "Use withColumnRenamed(old, new)",
      "Chain multiple rename operations",
      "Column order is preserved"
    ]
  },
  {
    id: "6",
    title: "Select and Reorder Columns",
    difficulty: "Easy",
    category: "Transformations",
    description: `Select only the \`name\`, \`email\`, and \`department\` columns from an employees DataFrame, in that specific order.

**employees_df Schema:**
| Column Name | Data Type |
|-------------|-----------|
| emp_id      | integer   |
| name        | string    |
| email       | string    |
| department  | string    |
| salary      | float     |
| hire_date   | date      |`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def etl(employees_df):
    # Select name, email, department columns only
    # Your code here
    
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def etl(employees_df):
    result_df = employees_df.select("name", "email", "department")
    return result_df`,
    explanation: `Use select() to choose specific columns in a specific order. You can pass column names as strings or use F.col().`,
    hints: [
      "Use select() with column names",
      "Columns appear in the order specified",
      "Can use strings or F.col() references"
    ]
  },

  // ============= AGGREGATIONS =============
  {
    id: "7",
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

def etl(employees_df):
    # Count employees per department
    # Your code here
    
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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
    id: "8",
    title: "Multiple Aggregations",
    difficulty: "Medium",
    category: "Aggregations",
    description: `Calculate the average salary and count of employees per department.

**employees_df Schema:**
| Column Name | Data Type |
|-------------|-----------|
| emp_id      | integer   |
| name        | string    |
| department  | string    |
| salary      | float     |

**Expected Output:**
| department  | avg_salary | emp_count |
|-------------|------------|-----------|
| Engineering | 85000.0    | 5         |
| Marketing   | 62500.0    | 3         |`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def etl(employees_df):
    # Calculate avg salary and count per department
    # Name columns: avg_salary, emp_count
    # Your code here
    
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def etl(employees_df):
    result_df = employees_df.groupBy("department").agg(
        F.avg("salary").alias("avg_salary"),
        F.count("*").alias("emp_count")
    )
    return result_df`,
    explanation: `Use agg() with multiple aggregation functions. Use alias() to name the result columns.`,
    hints: [
      "Use agg() for multiple aggregations",
      "F.avg(), F.count(), F.sum() are common functions",
      "alias() renames the output column"
    ]
  },
  {
    id: "9",
    title: "Sum and Max Aggregations",
    difficulty: "Medium",
    category: "Aggregations",
    description: `For each product category, calculate the total revenue and maximum single sale amount.

**sales_df Schema:**
| Column Name | Data Type |
|-------------|-----------|
| sale_id     | integer   |
| category    | string    |
| amount      | float     |

**Expected Output:**
| category    | total_revenue | max_sale |
|-------------|---------------|----------|
| Electronics | 50000.00      | 2500.00  |
| Clothing    | 15000.00      | 500.00   |`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def etl(sales_df):
    # Calculate total_revenue (sum) and max_sale (max) per category
    # Your code here
    
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def etl(sales_df):
    result_df = sales_df.groupBy("category").agg(
        F.sum("amount").alias("total_revenue"),
        F.max("amount").alias("max_sale")
    )
    return result_df`,
    explanation: `F.sum() calculates the total, F.max() finds the maximum value. Both are used within agg().`,
    hints: [
      "F.sum() for totals",
      "F.max() for maximum values",
      "F.min() would give minimum values"
    ]
  },

  // ============= JOINS =============
  {
    id: "10",
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

def etl(employees_df, departments_df):
    # Join employees with departments on dept_id
    # Your code here
    
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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
    id: "11",
    title: "Left Outer Join",
    difficulty: "Medium",
    category: "Joins",
    description: `Perform a left outer join to get all employees with their department info. Employees without a matching department should still appear with null department info.

Keep all employees, even if they don't have a matching department.`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def etl(employees_df, departments_df):
    # Left outer join: keep all employees
    # Your code here
    
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def etl(employees_df, departments_df):
    joined_df = employees_df.join(
        departments_df,
        employees_df.dept_id == departments_df.dept_id,
        "left"
    ).drop(departments_df.dept_id)
    return joined_df`,
    explanation: `Use "left" or "left_outer" as the join type. All rows from the left DataFrame are preserved.`,
    hints: [
      "Use 'left' or 'left_outer' join type",
      "All left DataFrame rows are kept",
      "Non-matching rows have null values"
    ]
  },
  {
    id: "12",
    title: "Join on Multiple Columns",
    difficulty: "Medium",
    category: "Joins",
    description: `Join two DataFrames on multiple columns: both \`year\` and \`quarter\`.

**sales_df:** year, quarter, region, revenue
**targets_df:** year, quarter, target_revenue

Join on year AND quarter.`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def etl(sales_df, targets_df):
    # Join on both year and quarter columns
    # Your code here
    
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def etl(sales_df, targets_df):
    joined_df = sales_df.join(
        targets_df,
        (sales_df.year == targets_df.year) & 
        (sales_df.quarter == targets_df.quarter),
        "inner"
    ).drop(targets_df.year).drop(targets_df.quarter)
    return joined_df`,
    explanation: `Combine multiple join conditions with &. Drop duplicate columns from the right DataFrame.`,
    hints: [
      "Use & to combine join conditions",
      "Wrap each condition in parentheses",
      "Drop duplicate columns from result"
    ]
  },

  // ============= WINDOW FUNCTIONS =============
  {
    id: "13",
    title: "Row Number Within Groups",
    difficulty: "Hard",
    category: "Window Functions",
    description: `Add a row number to each employee within their department, ordered by salary descending (highest salary = row 1).

**employees_df:**
| name    | department  | salary |
|---------|-------------|--------|
| Alice   | Engineering | 80000  |
| Bob     | Engineering | 90000  |
| Charlie | Marketing   | 60000  |

**Expected Output:**
| name    | department  | salary | row_num |
|---------|-------------|--------|---------|
| Bob     | Engineering | 90000  | 1       |
| Alice   | Engineering | 80000  | 2       |
| Charlie | Marketing   | 60000  | 1       |`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def etl(employees_df):
    # Add row_num partitioned by department, ordered by salary desc
    # Your code here
    
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def etl(employees_df):
    window_spec = Window.partitionBy("department").orderBy(F.desc("salary"))
    
    result_df = employees_df.withColumn(
        "row_num",
        F.row_number().over(window_spec)
    )
    return result_df`,
    explanation: `Define a window spec with partitionBy() and orderBy(). Apply row_number().over(window_spec).`,
    hints: [
      "Window.partitionBy() defines groups",
      "orderBy() defines the order within groups",
      "row_number().over() applies the function"
    ]
  },
  {
    id: "14",
    title: "Rank vs Dense Rank",
    difficulty: "Hard",
    category: "Window Functions",
    description: `Add both rank and dense_rank columns to employees within each department by salary.

**Key difference:**
- rank(): Gaps in ranking when there are ties (1, 1, 3)
- dense_rank(): No gaps in ranking (1, 1, 2)

Add columns: \`rank\` and \`dense_rank\``,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def etl(employees_df):
    # Add both rank and dense_rank columns
    # Your code here
    
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def etl(employees_df):
    window_spec = Window.partitionBy("department").orderBy(F.desc("salary"))
    
    result_df = employees_df.withColumn(
        "rank",
        F.rank().over(window_spec)
    ).withColumn(
        "dense_rank",
        F.dense_rank().over(window_spec)
    )
    return result_df`,
    explanation: `rank() leaves gaps after ties, dense_rank() doesn't. Both are applied over the same window spec.`,
    hints: [
      "rank() creates gaps: 1, 1, 3",
      "dense_rank() no gaps: 1, 1, 2",
      "Same window spec for both"
    ]
  },
  {
    id: "15",
    title: "Running Total with Window",
    difficulty: "Hard",
    category: "Window Functions",
    description: `Calculate a running total of sales amount, ordered by date within each region.

**sales_df:**
| region | sale_date  | amount |
|--------|------------|--------|
| East   | 2024-01-01 | 100    |
| East   | 2024-01-02 | 150    |
| East   | 2024-01-03 | 200    |

**Expected Output:**
| region | sale_date  | amount | running_total |
|--------|------------|--------|---------------|
| East   | 2024-01-01 | 100    | 100           |
| East   | 2024-01-02 | 150    | 250           |
| East   | 2024-01-03 | 200    | 450           |`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def etl(sales_df):
    # Add running_total column
    # Your code here
    
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def etl(sales_df):
    window_spec = Window.partitionBy("region") \\
                        .orderBy("sale_date") \\
                        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    
    result_df = sales_df.withColumn(
        "running_total",
        F.sum("amount").over(window_spec)
    )
    return result_df`,
    explanation: `Use rowsBetween() to define the window frame. unboundedPreceding to currentRow gives a running total.`,
    hints: [
      "rowsBetween defines the frame",
      "unboundedPreceding = from start",
      "currentRow = up to current row"
    ]
  },
  {
    id: "16",
    title: "Lag and Lead Functions",
    difficulty: "Hard",
    category: "Window Functions",
    description: `For each sale, add columns showing the previous day's amount (prev_amount) and next day's amount (next_amount).

Use LAG for previous and LEAD for next values.`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def etl(sales_df):
    # Add prev_amount (lag) and next_amount (lead) columns
    # Your code here
    
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def etl(sales_df):
    window_spec = Window.partitionBy("region").orderBy("sale_date")
    
    result_df = sales_df.withColumn(
        "prev_amount",
        F.lag("amount", 1).over(window_spec)
    ).withColumn(
        "next_amount",
        F.lead("amount", 1).over(window_spec)
    )
    return result_df`,
    explanation: `lag(col, n) gets the value n rows before. lead(col, n) gets the value n rows after. First/last rows have null.`,
    hints: [
      "lag() looks backward",
      "lead() looks forward",
      "Second arg is number of rows offset"
    ]
  },

  // ============= STRING FUNCTIONS =============
  {
    id: "17",
    title: "String Concatenation",
    difficulty: "Easy",
    category: "String Functions",
    description: `Create a \`full_name\` column by concatenating \`first_name\` and \`last_name\` with a space between them.

**users_df:**
| user_id | first_name | last_name |
|---------|------------|-----------|
| 1       | John       | Doe       |
| 2       | Jane       | Smith     |

**Expected Output:**
| user_id | first_name | last_name | full_name  |
|---------|------------|-----------|------------|
| 1       | John       | Doe       | John Doe   |
| 2       | Jane       | Smith     | Jane Smith |`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def etl(users_df):
    # Create full_name = first_name + " " + last_name
    # Your code here
    
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def etl(users_df):
    result_df = users_df.withColumn(
        "full_name",
        F.concat(F.col("first_name"), F.lit(" "), F.col("last_name"))
    )
    return result_df`,
    explanation: `Use F.concat() to join columns. F.lit() creates a literal value (the space).`,
    hints: [
      "F.concat() joins multiple columns",
      "F.lit() for literal values",
      "Alternative: F.concat_ws(' ', col1, col2)"
    ]
  },
  {
    id: "18",
    title: "String Case Conversion",
    difficulty: "Easy",
    category: "String Functions",
    description: `Convert the \`email\` column to lowercase and the \`department\` column to uppercase.`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def etl(employees_df):
    # email to lowercase, department to uppercase
    # Your code here
    
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def etl(employees_df):
    result_df = employees_df.withColumn(
        "email",
        F.lower(F.col("email"))
    ).withColumn(
        "department",
        F.upper(F.col("department"))
    )
    return result_df`,
    explanation: `F.lower() converts to lowercase, F.upper() converts to uppercase.`,
    hints: [
      "F.lower() for lowercase",
      "F.upper() for uppercase",
      "F.initcap() for title case"
    ]
  },
  {
    id: "19",
    title: "Extract Substring with Regex",
    difficulty: "Medium",
    category: "String Functions",
    description: `Extract the domain from email addresses. For "john@example.com", extract "example.com".

Use regex to extract everything after the @ symbol.`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def etl(users_df):
    # Extract domain from email column
    # Your code here
    
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def etl(users_df):
    result_df = users_df.withColumn(
        "domain",
        F.regexp_extract(F.col("email"), r"@(.+)$", 1)
    )
    return result_df`,
    explanation: `regexp_extract(col, pattern, group) extracts regex matches. Group 1 is the first capture group (in parentheses).`,
    hints: [
      "regexp_extract for pattern matching",
      "Use capture groups with ()",
      "Group 0 = full match, 1+ = capture groups"
    ]
  },
  {
    id: "20",
    title: "Split String to Array",
    difficulty: "Medium",
    category: "String Functions",
    description: `Split a comma-separated \`tags\` column into an array, then explode it to create one row per tag.

**posts_df:**
| post_id | tags                  |
|---------|-----------------------|
| 1       | python,spark,data     |
| 2       | sql,analytics         |

**Expected Output:**
| post_id | tag       |
|---------|-----------|
| 1       | python    |
| 1       | spark     |
| 1       | data      |
| 2       | sql       |
| 2       | analytics |`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def etl(posts_df):
    # Split tags and explode into separate rows
    # Your code here
    
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def etl(posts_df):
    result_df = posts_df.withColumn(
        "tag",
        F.explode(F.split(F.col("tags"), ","))
    ).drop("tags")
    return result_df`,
    explanation: `F.split() creates an array from a delimited string. F.explode() creates one row per array element.`,
    hints: [
      "F.split(col, delimiter) creates array",
      "F.explode() expands array to rows",
      "Each array element becomes a row"
    ]
  },

  // ============= DATE FUNCTIONS =============
  {
    id: "21",
    title: "Extract Date Parts",
    difficulty: "Easy",
    category: "Date Functions",
    description: `From an \`order_date\` column, extract the year, month, and day into separate columns.

Add columns: \`order_year\`, \`order_month\`, \`order_day\``,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def etl(orders_df):
    # Extract year, month, day from order_date
    # Your code here
    
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def etl(orders_df):
    result_df = orders_df.withColumn(
        "order_year", F.year("order_date")
    ).withColumn(
        "order_month", F.month("order_date")
    ).withColumn(
        "order_day", F.dayofmonth("order_date")
    )
    return result_df`,
    explanation: `Use F.year(), F.month(), F.dayofmonth() to extract date components.`,
    hints: [
      "F.year() extracts year",
      "F.month() extracts month (1-12)",
      "F.dayofmonth() extracts day (1-31)"
    ]
  },
  {
    id: "22",
    title: "Date Arithmetic",
    difficulty: "Medium",
    category: "Date Functions",
    description: `Add columns for:
- \`delivery_date\`: order_date + 7 days
- \`days_since_order\`: days between order_date and today`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def etl(orders_df):
    # Add delivery_date (order_date + 7) and days_since_order
    # Your code here
    
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def etl(orders_df):
    result_df = orders_df.withColumn(
        "delivery_date",
        F.date_add("order_date", 7)
    ).withColumn(
        "days_since_order",
        F.datediff(F.current_date(), F.col("order_date"))
    )
    return result_df`,
    explanation: `date_add() adds days to a date. datediff() calculates days between two dates.`,
    hints: [
      "F.date_add(col, days) adds days",
      "F.date_sub(col, days) subtracts days",
      "F.datediff(end, start) returns difference"
    ]
  },
  {
    id: "23",
    title: "Format Date to String",
    difficulty: "Medium",
    category: "Date Functions",
    description: `Convert a date column to different string formats:
- \`formatted_date\`: "January 15, 2024" format
- \`iso_date\`: "2024-01-15" format`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def etl(events_df):
    # Format date column to different string formats
    # Your code here
    
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def etl(events_df):
    result_df = events_df.withColumn(
        "formatted_date",
        F.date_format("event_date", "MMMM dd, yyyy")
    ).withColumn(
        "iso_date",
        F.date_format("event_date", "yyyy-MM-dd")
    )
    return result_df`,
    explanation: `date_format() converts dates to strings. Use Java SimpleDateFormat patterns.`,
    hints: [
      "yyyy = 4-digit year",
      "MM = 2-digit month, MMMM = full name",
      "dd = 2-digit day"
    ]
  },

  // ============= NULL HANDLING =============
  {
    id: "24",
    title: "Fill Null Values",
    difficulty: "Easy",
    category: "Null Handling",
    description: `Replace null values in the \`salary\` column with 0 and null values in the \`department\` column with "Unknown".`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def etl(employees_df):
    # Fill nulls: salary with 0, department with "Unknown"
    # Your code here
    
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def etl(employees_df):
    result_df = employees_df.fillna({
        "salary": 0,
        "department": "Unknown"
    })
    return result_df`,
    explanation: `fillna() replaces null values. Pass a dict to specify different values for different columns.`,
    hints: [
      "fillna(value) fills all columns",
      "fillna({col: val}) for specific columns",
      "Different types need separate values"
    ]
  },
  {
    id: "25",
    title: "Coalesce Multiple Columns",
    difficulty: "Medium",
    category: "Null Handling",
    description: `Create a \`contact_phone\` column that uses \`mobile_phone\` if available, otherwise \`home_phone\`, otherwise \`work_phone\`.

Use COALESCE logic: first non-null value wins.`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def etl(contacts_df):
    # Coalesce: mobile_phone > home_phone > work_phone
    # Your code here
    
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def etl(contacts_df):
    result_df = contacts_df.withColumn(
        "contact_phone",
        F.coalesce(
            F.col("mobile_phone"),
            F.col("home_phone"),
            F.col("work_phone")
        )
    )
    return result_df`,
    explanation: `coalesce() returns the first non-null value from the list of columns.`,
    hints: [
      "coalesce takes multiple columns",
      "Returns first non-null value",
      "Order matters - check priority columns first"
    ]
  },
  {
    id: "26",
    title: "Drop Rows with Nulls",
    difficulty: "Easy",
    category: "Null Handling",
    description: `Remove rows where any of the required columns (\`name\`, \`email\`) contain null values.`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def etl(users_df):
    # Drop rows where name or email is null
    # Your code here
    
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def etl(users_df):
    result_df = users_df.dropna(subset=["name", "email"])
    return result_df`,
    explanation: `dropna() removes rows with nulls. Use subset to specify which columns to check.`,
    hints: [
      "dropna() removes null rows",
      "subset=[cols] checks specific columns",
      "how='all' only drops if all are null"
    ]
  },

  // ============= PIVOTING =============
  {
    id: "27",
    title: "Pivot Table",
    difficulty: "Hard",
    category: "Pivoting",
    description: `Create a pivot table showing total sales by region (rows) and quarter (columns).

**sales_df:**
| region | quarter | amount |
|--------|---------|--------|
| East   | Q1      | 1000   |
| East   | Q2      | 1500   |
| West   | Q1      | 800    |

**Expected Output:**
| region | Q1   | Q2   | Q3   | Q4   |
|--------|------|------|------|------|
| East   | 1000 | 1500 | null | null |
| West   | 800  | null | null | null |`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def etl(sales_df):
    # Pivot: region as rows, quarter as columns, sum of amount
    # Your code here
    
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def etl(sales_df):
    result_df = sales_df.groupBy("region") \\
        .pivot("quarter", ["Q1", "Q2", "Q3", "Q4"]) \\
        .agg(F.sum("amount"))
    return result_df`,
    explanation: `pivot() creates columns from row values. Specify values list for better performance and predictable columns.`,
    hints: [
      "groupBy the row dimension",
      "pivot() the column dimension",
      "agg() defines the cell values"
    ]
  },
  {
    id: "28",
    title: "Unpivot (Melt) Table",
    difficulty: "Hard",
    category: "Pivoting",
    description: `Convert a wide table back to long format. Transform quarterly columns (Q1, Q2, Q3, Q4) into quarter and value columns.

**Input (wide):**
| region | Q1   | Q2   |
|--------|------|------|
| East   | 1000 | 1500 |

**Output (long):**
| region | quarter | amount |
|--------|---------|--------|
| East   | Q1      | 1000   |
| East   | Q2      | 1500   |`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def etl(wide_df):
    # Unpivot quarterly columns to quarter and amount
    # Your code here
    
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def etl(wide_df):
    result_df = wide_df.selectExpr(
        "region",
        "stack(4, 'Q1', Q1, 'Q2', Q2, 'Q3', Q3, 'Q4', Q4) as (quarter, amount)"
    ).filter(F.col("amount").isNotNull())
    return result_df`,
    explanation: `Use stack() in selectExpr to unpivot. Arguments: count, then pairs of (name, value) for each column.`,
    hints: [
      "stack() unpivots columns",
      "First arg is number of columns",
      "Then pairs of (name, value)"
    ]
  },

  // ============= UDFs =============
  {
    id: "29",
    title: "Simple UDF",
    difficulty: "Medium",
    category: "UDFs",
    description: `Create a UDF that categorizes employees by age:
- Under 25: "Junior"
- 25-35: "Mid-level"
- Over 35: "Senior"

Add an \`age_group\` column using this UDF.`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

def etl(employees_df):
    # Create and apply age categorization UDF
    # Your code here
    
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

def etl(employees_df):
    def categorize_age(age):
        if age is None:
            return "Unknown"
        elif age < 25:
            return "Junior"
        elif age <= 35:
            return "Mid-level"
        else:
            return "Senior"
    
    categorize_udf = F.udf(categorize_age, StringType())
    
    result_df = employees_df.withColumn(
        "age_group",
        categorize_udf(F.col("age"))
    )
    return result_df`,
    explanation: `Define a Python function, wrap with F.udf() specifying return type, then apply to column.`,
    hints: [
      "Define a regular Python function",
      "Wrap with F.udf(func, return_type)",
      "Handle null values in the function"
    ]
  },
  {
    id: "30",
    title: "UDF with Multiple Inputs",
    difficulty: "Hard",
    category: "UDFs",
    description: `Create a UDF that calculates a custom score based on multiple columns:
score = (rating * 10) + (reviews / 100)

Apply this to create a \`popularity_score\` column.`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType

def etl(products_df):
    # Create UDF with rating and reviews inputs
    # Your code here
    
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType

def etl(products_df):
    def calculate_score(rating, reviews):
        if rating is None or reviews is None:
            return 0.0
        return float((rating * 10) + (reviews / 100))
    
    score_udf = F.udf(calculate_score, FloatType())
    
    result_df = products_df.withColumn(
        "popularity_score",
        score_udf(F.col("rating"), F.col("reviews"))
    )
    return result_df`,
    explanation: `UDFs can take multiple column arguments. Pass multiple F.col() references when calling.`,
    hints: [
      "UDFs can accept multiple arguments",
      "Pass multiple columns when calling",
      "Always handle null inputs"
    ]
  },
];

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

export const getProblemsByCategory = (category: Category): Problem[] => {
  return problems.filter(p => p.category === category);
};

export const getProblemById = (id: string): Problem | undefined => {
  return problems.find(p => p.id === id);
};
