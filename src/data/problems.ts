export type Difficulty = "Easy" | "Medium" | "Hard";
export type Category = "DataFrame Basics" | "Transformations" | "Aggregations" | "Joins" | "Window Functions" | "SQL" | "UDFs";

export interface Problem {
  id: string;
  title: string;
  difficulty: Difficulty;
  category: Category;
  description: string;
  starterCode: string;
  solution: string;
  testCases: string;
  hints: string[];
  completed?: boolean;
}

export const problems: Problem[] = [
  {
    id: "1",
    title: "Create a DataFrame",
    difficulty: "Easy",
    category: "DataFrame Basics",
    description: `Create a PySpark DataFrame from the given data.

You are given a list of tuples containing employee information (name, age, department).

**Input:**
\`\`\`python
data = [
    ("Alice", 30, "Engineering"),
    ("Bob", 25, "Marketing"),
    ("Charlie", 35, "Engineering"),
    ("Diana", 28, "HR")
]
\`\`\`

**Expected Output:**
A DataFrame with columns: name, age, department

**Schema:**
- name: StringType
- age: IntegerType  
- department: StringType`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def solution(spark: SparkSession):
    data = [
        ("Alice", 30, "Engineering"),
        ("Bob", 25, "Marketing"),
        ("Charlie", 35, "Engineering"),
        ("Diana", 28, "HR")
    ]
    
    # Your code here
    # Create a DataFrame with the given data
    # Define the schema with columns: name, age, department
    
    return df`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def solution(spark: SparkSession):
    data = [
        ("Alice", 30, "Engineering"),
        ("Bob", 25, "Marketing"),
        ("Charlie", 35, "Engineering"),
        ("Diana", 28, "HR")
    ]
    
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("department", StringType(), True)
    ])
    
    df = spark.createDataFrame(data, schema)
    return df`,
    testCases: `assert df.count() == 4
assert df.columns == ["name", "age", "department"]`,
    hints: [
      "Use StructType and StructField to define the schema",
      "spark.createDataFrame() takes data and schema as arguments"
    ]
  },
  {
    id: "2",
    title: "Filter DataFrame Rows",
    difficulty: "Easy",
    category: "Transformations",
    description: `Filter a DataFrame to return only employees from the Engineering department who are older than 25.

**Input DataFrame:**
| name    | age | department  |
|---------|-----|-------------|
| Alice   | 30  | Engineering |
| Bob     | 25  | Marketing   |
| Charlie | 35  | Engineering |
| Diana   | 28  | HR          |
| Eve     | 22  | Engineering |

**Expected Output:**
| name    | age | department  |
|---------|-----|-------------|
| Alice   | 30  | Engineering |
| Charlie | 35  | Engineering |`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def solution(spark: SparkSession, df):
    # Filter for Engineering department AND age > 25
    # Your code here
    
    return filtered_df`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def solution(spark: SparkSession, df):
    filtered_df = df.filter(
        (col("department") == "Engineering") & (col("age") > 25)
    )
    return filtered_df`,
    testCases: `assert filtered_df.count() == 2
assert all(row.department == "Engineering" for row in filtered_df.collect())`,
    hints: [
      "Use the filter() or where() method",
      "Combine conditions with & (and) operator",
      "Use col() to reference columns"
    ]
  },
  {
    id: "3",
    title: "Group By and Aggregate",
    difficulty: "Medium",
    category: "Aggregations",
    description: `Calculate the average salary and count of employees per department.

**Input DataFrame:**
| name    | department  | salary |
|---------|-------------|--------|
| Alice   | Engineering | 80000  |
| Bob     | Marketing   | 60000  |
| Charlie | Engineering | 90000  |
| Diana   | HR          | 55000  |
| Eve     | Marketing   | 65000  |

**Expected Output:**
| department  | avg_salary | emp_count |
|-------------|------------|-----------|
| Engineering | 85000.0    | 2         |
| HR          | 55000.0    | 1         |
| Marketing   | 62500.0    | 2         |`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, col

def solution(spark: SparkSession, df):
    # Group by department
    # Calculate average salary (as avg_salary) and count (as emp_count)
    # Your code here
    
    return result_df`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, col

def solution(spark: SparkSession, df):
    result_df = df.groupBy("department").agg(
        avg("salary").alias("avg_salary"),
        count("*").alias("emp_count")
    )
    return result_df`,
    testCases: `assert result_df.count() == 3
assert "avg_salary" in result_df.columns
assert "emp_count" in result_df.columns`,
    hints: [
      "Use groupBy() followed by agg()",
      "Use alias() to rename aggregated columns",
      "avg() and count() are aggregation functions"
    ]
  },
  {
    id: "4",
    title: "Join Two DataFrames",
    difficulty: "Medium",
    category: "Joins",
    description: `Join the employees DataFrame with the departments DataFrame to get the department location for each employee.

**Employees DataFrame:**
| emp_id | name    | dept_id |
|--------|---------|---------|
| 1      | Alice   | 101     |
| 2      | Bob     | 102     |
| 3      | Charlie | 101     |

**Departments DataFrame:**
| dept_id | dept_name   | location    |
|---------|-------------|-------------|
| 101     | Engineering | New York    |
| 102     | Marketing   | Los Angeles |
| 103     | HR          | Chicago     |

**Expected Output (inner join):**
| emp_id | name    | dept_id | dept_name   | location    |
|--------|---------|---------|-------------|-------------|
| 1      | Alice   | 101     | Engineering | New York    |
| 2      | Bob     | 102     | Marketing   | Los Angeles |
| 3      | Charlie | 101     | Engineering | New York    |`,
    starterCode: `from pyspark.sql import SparkSession

def solution(spark: SparkSession, employees_df, departments_df):
    # Perform an inner join on dept_id
    # Your code here
    
    return joined_df`,
    solution: `from pyspark.sql import SparkSession

def solution(spark: SparkSession, employees_df, departments_df):
    joined_df = employees_df.join(
        departments_df,
        employees_df.dept_id == departments_df.dept_id,
        "inner"
    ).drop(departments_df.dept_id)
    return joined_df`,
    testCases: `assert joined_df.count() == 3
assert "location" in joined_df.columns`,
    hints: [
      "Use the join() method with the join condition",
      "Specify 'inner' as the join type",
      "Drop duplicate columns after joining"
    ]
  },
  {
    id: "5",
    title: "Window Function - Ranking",
    difficulty: "Hard",
    category: "Window Functions",
    description: `Rank employees within each department by their salary (highest salary = rank 1).

**Input DataFrame:**
| name    | department  | salary |
|---------|-------------|--------|
| Alice   | Engineering | 80000  |
| Bob     | Engineering | 90000  |
| Charlie | Marketing   | 60000  |
| Diana   | Marketing   | 65000  |
| Eve     | Engineering | 85000  |

**Expected Output:**
| name    | department  | salary | rank |
|---------|-------------|--------|------|
| Bob     | Engineering | 90000  | 1    |
| Eve     | Engineering | 85000  | 2    |
| Alice   | Engineering | 80000  | 3    |
| Diana   | Marketing   | 65000  | 1    |
| Charlie | Marketing   | 60000  | 2    |`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, desc

def solution(spark: SparkSession, df):
    # Create a window specification partitioned by department, ordered by salary descending
    # Apply rank() function
    # Your code here
    
    return ranked_df`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, desc

def solution(spark: SparkSession, df):
    window_spec = Window.partitionBy("department").orderBy(desc("salary"))
    
    ranked_df = df.withColumn("rank", rank().over(window_spec))
    return ranked_df`,
    testCases: `assert "rank" in ranked_df.columns
eng_top = ranked_df.filter(ranked_df.department == "Engineering").orderBy("rank").first()
assert eng_top.name == "Bob"`,
    hints: [
      "Use Window.partitionBy() to partition by department",
      "Use orderBy(desc('salary')) for descending order",
      "Apply rank().over(window_spec) with withColumn()"
    ]
  },
  {
    id: "6",
    title: "Create and Use a UDF",
    difficulty: "Hard",
    category: "UDFs",
    description: `Create a User Defined Function (UDF) that categorizes employees by age group:
- Under 25: "Junior"
- 25-35: "Mid-level"  
- Over 35: "Senior"

**Input DataFrame:**
| name    | age |
|---------|-----|
| Alice   | 22  |
| Bob     | 30  |
| Charlie | 40  |
| Diana   | 25  |

**Expected Output:**
| name    | age | age_group |
|---------|-----|-----------|
| Alice   | 22  | Junior    |
| Bob     | 30  | Mid-level |
| Charlie | 40  | Senior    |
| Diana   | 25  | Mid-level |`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def solution(spark: SparkSession, df):
    # Define the age categorization function
    def categorize_age(age):
        # Your logic here
        pass
    
    # Register as UDF
    # Apply to DataFrame
    # Your code here
    
    return result_df`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def solution(spark: SparkSession, df):
    def categorize_age(age):
        if age < 25:
            return "Junior"
        elif age <= 35:
            return "Mid-level"
        else:
            return "Senior"
    
    categorize_age_udf = udf(categorize_age, StringType())
    
    result_df = df.withColumn("age_group", categorize_age_udf(df.age))
    return result_df`,
    testCases: `assert "age_group" in result_df.columns
alice = result_df.filter(result_df.name == "Alice").first()
assert alice.age_group == "Junior"`,
    hints: [
      "Define a Python function first",
      "Use udf() to convert it to a Spark UDF",
      "Specify the return type (StringType)",
      "Apply with withColumn()"
    ]
  }
];

export const categories: Category[] = [
  "DataFrame Basics",
  "Transformations", 
  "Aggregations",
  "Joins",
  "Window Functions",
  "SQL",
  "UDFs"
];

export const getProblemsByCategory = (category: Category): Problem[] => {
  return problems.filter(p => p.category === category);
};

export const getProblemById = (id: string): Problem | undefined => {
  return problems.find(p => p.id === id);
};
