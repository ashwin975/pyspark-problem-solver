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
  | "Pivoting";

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
  "Pivoting"
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

// Problems from ZillaCode PDF - PySpark learning platform
export const problems: Problem[] = [
  // Problem 1 - Filter Null Values (Movies)
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
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(movies_df):
    # Write code here
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

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
3. The \`filter\` method is used on the \`movies_df\` DataFrame to filter out rows where the \`box_office_collection\` column is null using the \`isnull\` function.
4. The resulting DataFrame that contains only the rows where the \`box_office_collection\` column is null is returned.

**Complexity:**
- Space: O(n) where n is the number of rows in the input DataFrame
- Time: O(n) - linear scan of all rows`,
    hints: [
      "Use F.isnull() to check for null values in a column",
      "The filter() method accepts a column expression that returns boolean",
      "Alternative syntax: F.col('box_office_collection').isNull()"
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
          { box_office_collection: null, director_name: "Ryan Coogler", genre: "Action, Adventure, Drama", movie_id: 2, movie_title: "Black Panther: Wakanda Forever", release_date: "2022-11-10" },
          { box_office_collection: null, director_name: "David Yates", genre: "Adventure, Family, Fantasy", movie_id: 4, movie_title: "Fantastic Beasts and Where to Find Them 3", release_date: "2022-11-04" },
        ]
      }
    ]
  },

  // Problem 2 - Call Center Aggregation
  {
    id: "2",
    title: "Call Center Aggregation",
    difficulty: "Medium",
    category: "Aggregations",
    description: `**Call Center**

You are given two DataFrames, \`calls_df\` and \`customers_df\`, which contain information about calls made by customers of a telecommunications company.

**calls_df Schema:**

| Column   | Type    | Description                                          |
|----------|---------|------------------------------------------------------|
| call_id  | integer | unique identifier of each call                       |
| cust_id  | integer | unique identifier of the customer who made the call  |
| date     | string  | date when the call was made in the format "yyyy-MM-dd"|
| duration | integer | duration of the call in seconds                      |

**customers_df Schema:**

| Column     | Type    | Description                                    |
|------------|---------|------------------------------------------------|
| cust_id    | integer | unique identifier of each customer             |
| name       | string  | name of the customer                           |
| state      | string  | state where the customer lives                 |
| tenure     | integer | number of months customer has been with company|
| occupation | string  | occupation of the customer                     |

Write a function that returns the number of distinct customers who made calls on each date, along with the total duration of calls made on each date.

**Output Schema:**

| Column         | Type    | Description                                      |
|----------------|---------|--------------------------------------------------|
| date           | string  | date when the calls were made (yyyy-MM-dd)       |
| num_customers  | integer | number of distinct customers who made calls      |
| total_duration | integer | total duration of calls made on that date        |

You may assume that the upstream DataFrames are not empty.

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
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(calls_df, customers_df):
    # Write code here
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

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
    explanation: `The PySpark solution joins the calls and customers DataFrames, then aggregates by date.

**Step-by-step:**
1. Join \`calls_df\` with \`customers_df\` on \`cust_id\` using inner join
2. Group the result by \`date\` column
3. Use \`F.count_distinct("cust_id")\` to count unique customers per date
4. Use \`F.sum("duration")\` to get total call duration per date
5. Return the aggregated DataFrame

**Complexity:**
- Space: O(n) where n is the number of rows
- Time: O(n log n) due to join and groupby operations`,
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

  // Problem 3 - Mountain Climbing (Window Functions)
  {
    id: "3",
    title: "Mountain Climbing - Last Climber",
    difficulty: "Hard",
    category: "Window Functions",
    description: `**Mountain Climbing**

You are given two DataFrames: \`mountain_info\` and \`mountain_climbers\`.

**mountain_info Schema:**

| Column Name | Data Type | Description                              |
|-------------|-----------|------------------------------------------|
| name        | string    | name of the mountain                     |
| height      | integer   | height of the mountain in meters         |
| country     | string    | country where the mountain is located    |
| range       | string    | mountain range name                      |

**mountain_climbers Schema:**

| Column Name   | Data Type | Description                           |
|---------------|-----------|---------------------------------------|
| climber_name  | string    | name of the climber                   |
| mountain_name | string    | name of the mountain climbed          |
| climb_date    | date      | date when the mountain was climbed    |
| climb_time    | double    | time taken to climb in hours          |

Write a function that returns the name of the mountain, the name of the climber who climbed the mountain last, and the date and time when they climbed it last. The output should only contain mountains that have been climbed by at least one climber.

**Output Schema:**

| Column Name       | Data Type |
|-------------------|-----------|
| mountain_name     | string    |
| last_climber_name | string    |
| last_climb_date   | date      |
| last_climb_time   | double    |

**Example**

**mountain_info**
| name              | height | country  | range       |
|-------------------|--------|----------|-------------|
| Mount Everest     | 8848   | Nepal    | Himalayas   |
| Mount Kilimanjaro | 5895   | Tanzania | Kilimanjaro |
| Mount Denali      | 6190   | USA      | Alaska      |
| Mount Fuji        | 3776   | Japan    | Fuji        |
| Mont Blanc        | 4808   | France   | Alps        |

**mountain_climbers**
| climber_name | mountain_name     | climb_date | climb_time |
|--------------|-------------------|------------|------------|
| John         | Mount Everest     | 2020-01-01 | 8.5        |
| Jane         | Mount Everest     | 2022-02-02 | 9.0        |
| Jim          | Mount Kilimanjaro | 2021-03-03 | 6.0        |
| Jess         | Mount Kilimanjaro | 2022-04-04 | 7.0        |
| Joe          | Mount Denali      | 2022-05-05 | 10.0       |
| Jill         | Mount Denali      | 2021-06-06 | 11.0       |
| Jack         | Mount Fuji        | 2022-07-07 | 4.0        |
| Jules        | Mount Fuji        | 2021-08-08 | 5.0        |
| Jean         | Mont Blanc        | 2020-09-09 | 12.0       |
| Josh         | Mont Blanc        | 2022-10-10 | 13.0       |

**Output**
| climb_date | climb_time | climber_name | mountain_name     |
|------------|------------|--------------|-------------------|
| 2022-02-02 | 9.0        | Jane         | Mount Everest     |
| 2022-04-04 | 7.0        | Jess         | Mount Kilimanjaro |
| 2022-05-05 | 10.0       | Joe          | Mount Denali      |
| 2022-07-07 | 4.0        | Jack         | Mount Fuji        |
| 2022-10-10 | 13.0       | Josh         | Mont Blanc        |`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(mountain_info, mountain_climbers):
    # Write code here
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(mountain_info, mountain_climbers):
    # Filter for mountains that have been climbed at least once
    climbed_mountains = mountain_climbers.select(
        "mountain_name"
    ).distinct()

    # Join the mountain_info and mountain_climbers DataFrames
    mountain_joined = mountain_info.join(
        climbed_mountains,
        mountain_info.name == climbed_mountains.mountain_name,
        "inner",
    )

    # Use a window function to get the latest climb for each mountain
    window = W.partitionBy(
        "mountain_name"
    ).orderBy(
        F.desc("climb_date"),
        F.desc("climb_time")
    )
    latest_climb = (
        mountain_climbers.select(
            "*",
            F.rank().over(window).alias("rank"),
        )
        .filter("rank == 1")
        .drop("rank")
    )
    
    # Join with mountain_info
    mountain_climb_joined = latest_climb.join(
        mountain_info,
        latest_climb.mountain_name == mountain_info.name,
        "inner",
    )

    # Select the necessary columns for the output DataFrame
    output = mountain_climb_joined.select(
        "mountain_name",
        "climber_name",
        "climb_date",
        "climb_time",
    )

    return output`,
    explanation: `The solution uses Window functions to find the most recent climb for each mountain.

**Step-by-step:**
1. Create a window partitioned by \`mountain_name\`, ordered by \`climb_date\` and \`climb_time\` descending
2. Use \`F.rank().over(window)\` to rank climbs within each mountain
3. Filter for rank == 1 to get only the most recent climb
4. Join with \`mountain_info\` to get mountain details
5. Select required output columns

**Key Concepts:**
- Window functions partition data into groups
- \`orderBy(F.desc(...))\` sorts descending (latest first)
- \`rank()\` assigns sequential numbers within partitions`,
    hints: [
      "Use Window.partitionBy('mountain_name') to group by mountain",
      "Order by climb_date descending to get the latest climb first",
      "Use F.rank() or F.row_number() to identify the first row",
      "Filter where rank == 1 to get only the latest climb per mountain"
    ],
    testCases: [
      {
        input: {
          mountain_info: [
            { name: "Mount Everest", height: 8848, country: "Nepal", range: "Himalayas" },
            { name: "Mount Kilimanjaro", height: 5895, country: "Tanzania", range: "Kilimanjaro" },
            { name: "Mount Denali", height: 6190, country: "USA", range: "Alaska" },
          ],
          mountain_climbers: [
            { climber_name: "John", mountain_name: "Mount Everest", climb_date: "2020-01-01", climb_time: 8.5 },
            { climber_name: "Jane", mountain_name: "Mount Everest", climb_date: "2022-02-02", climb_time: 9.0 },
            { climber_name: "Jim", mountain_name: "Mount Kilimanjaro", climb_date: "2021-03-03", climb_time: 6.0 },
            { climber_name: "Jess", mountain_name: "Mount Kilimanjaro", climb_date: "2022-04-04", climb_time: 7.0 },
          ]
        },
        expected_output: [
          { mountain_name: "Mount Everest", climber_name: "Jane", climb_date: "2022-02-02", climb_time: 9.0 },
          { mountain_name: "Mount Kilimanjaro", climber_name: "Jess", climb_date: "2022-04-04", climb_time: 7.0 },
        ]
      }
    ]
  },

  // Problem 4 - Rental Income (Pivot)
  {
    id: "4",
    title: "Total Rental Income",
    difficulty: "Hard",
    category: "Pivoting",
    description: `**Rental Income**

You are given two DataFrames: \`properties_df\` and \`landlords_df\`.

**properties_df Schema:**

| Column        | Type    | Description                        |
|---------------|---------|------------------------------------|
| property_id   | integer | unique identifier of property      |
| landlord_id   | integer | unique identifier of landlord      |
| property_type | string  | type of property (Apartment/Condo/House) |
| rent          | integer | monthly rent amount                |
| square_feet   | integer | size of property                   |
| city          | string  | city where property is located     |

**landlords_df Schema:**

| Column      | Type    | Description                    |
|-------------|---------|--------------------------------|
| landlord_id | integer | unique identifier of landlord  |
| first_name  | string  | landlord's first name          |
| last_name   | string  | landlord's last name           |
| email       | string  | landlord's email               |
| phone       | string  | landlord's phone number        |

Write a function to calculate the total rental income for each landlord across all their properties. Return the landlord_id, landlord_name (full name), and total_rental_income. Only include landlords who own at least one property.

**Output Schema:**

| Column              | Type    | Description                    |
|---------------------|---------|--------------------------------|
| landlord_id         | integer | unique identifier of landlord  |
| landlord_name       | string  | full name of landlord          |
| total_rental_income | float   | sum of rent from all properties|

**Example**

**properties_df**
| property_id | landlord_id | property_type | rent | square_feet | city     |
|-------------|-------------|---------------|------|-------------|----------|
| 1           | 101         | Apartment     | 1200 | 800         | Seattle  |
| 2           | 101         | Condo         | 1500 | 1000        | Seattle  |
| 3           | 102         | House         | 2000 | 1500        | Bellevue |
| 4           | 103         | Apartment     | 1300 | 900         | Redmond  |
| 5           | 103         | House         | 1500 | 1200        | Redmond  |

**landlords_df**
| landlord_id | first_name | last_name | email                      | phone        |
|-------------|------------|-----------|----------------------------|--------------|
| 101         | John       | Smith     | john.smith@example.com     | 555-123-4567 |
| 102         | Jane       | Doe       | jane.doe@example.com       | 555-234-5678 |
| 103         | Bob        | Johnson   | bob.johnson@example.com    | 555-345-6789 |

**Output**
| landlord_id | landlord_name | total_rental_income |
|-------------|---------------|---------------------|
| 101         | John Smith    | 2700.0              |
| 102         | Jane Doe      | 2000.0              |
| 103         | Bob Johnson   | 2800.0              |`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(properties_df, landlords_df):
    # Write code here
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(properties_df, landlords_df):
    # Pivot the Properties DataFrame
    properties_pivot_df = properties_df.groupBy(
        "landlord_id"
    ).pivot("property_type").agg(
        F.sum("rent")
    )

    # Join with the Landlords DataFrame
    rental_income_df = properties_pivot_df.join(
        landlords_df, "landlord_id"
    )

    # Calculate total rental income and create landlord_name
    rental_income_df = rental_income_df.select(
        F.col("landlord_id"),
        F.concat(
            F.col("first_name"),
            F.lit(" "),
            F.col("last_name")
        ).alias("landlord_name"),
        (
            F.coalesce(F.col("Apartment"), F.lit(0)) +
            F.coalesce(F.col("Condo"), F.lit(0)) +
            F.coalesce(F.col("House"), F.lit(0))
        ).cast("float").alias("total_rental_income")
    )

    # Sort by landlord_id
    rental_income_df = rental_income_df.sort("landlord_id")

    return rental_income_df`,
    explanation: `The solution pivots the properties DataFrame and joins with landlords to calculate total income.

**Step-by-step:**
1. Pivot \`properties_df\` by \`landlord_id\` and \`property_type\` to sum rent for each type
2. Join with \`landlords_df\` on \`landlord_id\`
3. Concatenate first_name and last_name into \`landlord_name\`
4. Sum all property type columns using \`coalesce\` to handle nulls
5. Sort by landlord_id and return

**Key Concepts:**
- \`pivot()\` transforms rows into columns
- \`coalesce()\` returns first non-null value (used to treat null as 0)
- \`F.concat()\` combines strings`,
    hints: [
      "Use groupBy and pivot to transform property types into columns",
      "Use F.coalesce() to handle null values when summing",
      "Concatenate first_name and last_name with F.concat()",
      "Remember to cast the total to float"
    ],
    testCases: [
      {
        input: {
          properties_df: [
            { property_id: 1, landlord_id: 101, property_type: "Apartment", rent: 1200, square_feet: 800, city: "Seattle" },
            { property_id: 2, landlord_id: 101, property_type: "Condo", rent: 1500, square_feet: 1000, city: "Seattle" },
            { property_id: 3, landlord_id: 102, property_type: "House", rent: 2000, square_feet: 1500, city: "Bellevue" },
          ],
          landlords_df: [
            { landlord_id: 101, first_name: "John", last_name: "Smith", email: "john.smith@example.com", phone: "555-123-4567" },
            { landlord_id: 102, first_name: "Jane", last_name: "Doe", email: "jane.doe@example.com", phone: "555-234-5678" },
          ]
        },
        expected_output: [
          { landlord_id: 101, landlord_name: "John Smith", total_rental_income: 2700.0 },
          { landlord_id: 102, landlord_name: "Jane Doe", total_rental_income: 2000.0 },
        ]
      }
    ]
  },

  // Problem 5 - PE Firms Full Join
  {
    id: "5",
    title: "PE Firms Full Join",
    difficulty: "Medium",
    category: "Joins",
    description: `**Private Equity Data**

You are given three DataFrames containing information about private equity firms, their funds, and investments.

**pe_firms Schema:**

| Column       | Type    | Description                    |
|--------------|---------|--------------------------------|
| firm_id      | integer | unique identifier of firm      |
| firm_name    | string  | name of the firm               |
| founded_year | integer | year the firm was founded      |
| location     | string  | location of the firm           |

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

| Column            | Type    | Description               |
|-------------------|---------|---------------------------|
| investment_id     | integer | unique investment id      |
| fund_id           | integer | fund making investment    |
| company_name      | string  | company invested in       |
| investment_amount | float   | amount invested           |
| investment_date   | date    | date of investment        |

Write a function to perform a full outer join on all three DataFrames, keeping all records from all tables. Filter out any rows where all columns are null.`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(pe_firms, pe_funds, pe_investments):
    # Write code here
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(pe_firms, pe_funds, pe_investments):
    # join the three dataframes using full join
    joined_df = pe_firms.join(
        pe_funds, 'firm_id', 'outer'
    ).join(pe_investments, 'fund_id', 'outer')

    # filter out any rows where all columns are null
    joined_df = joined_df.dropna(how='all')

    return joined_df`,
    explanation: `The solution performs full outer joins to combine all three DataFrames while preserving all records.

**Step-by-step:**
1. Join \`pe_firms\` with \`pe_funds\` on \`firm_id\` using outer join
2. Join the result with \`pe_investments\` on \`fund_id\` using outer join
3. Drop rows where all columns are null using \`dropna(how='all')\`
4. Return the combined DataFrame

**Key Concepts:**
- Full outer join preserves all records from both sides
- Chain joins for multiple DataFrames
- \`dropna(how='all')\` removes only completely null rows`,
    hints: [
      "Use 'outer' as the join type for full outer join",
      "Chain multiple joins together",
      "The join column should match between DataFrames",
      "dropna(how='all') removes rows with all nulls"
    ]
  },

  // Problem 6 - Correcting Social Media Posts (Regex Replace)
  {
    id: "6",
    title: "Correcting Social Media Posts",
    difficulty: "Easy",
    category: "Regex",
    description: `**Correcting Social Media Posts**

You are given a DataFrame named \`social_media\` containing information about social media posts.

**social_media Schema:**

| Column Name | Data Type | Description                                                          |
|-------------|-----------|----------------------------------------------------------------------|
| id          | integer   | The unique identifier for each social media post                     |
| text        | string    | The text content of the social media post                            |
| date        | string    | The date when the post was made in the format "YYYY-MM-DD"           |
| likes       | integer   | The number of likes the post received                                |
| comments    | integer   | The number of comments the post received                             |
| shares      | integer   | The number of shares the post received                               |
| platform    | string    | The platform (e.g., "Twitter", "Facebook", or "Instagram")           |

Write a function that returns the same schema as \`social_media\`, but with any mentions of the word "Python" in the \`text\` column replaced with the word "PySpark".

**Example**

**social_media**
| id | text                                          | date       | likes | comments | shares | platform  |
|----|-----------------------------------------------|------------|-------|----------|--------|-----------|
| 1  | This is a Python post.                        | 2022-03-01 | 10    | 3        | 2      | Twitter   |
| 2  | Another post about Python.                    | 2022-03-02 | 20    | 5        | 3      | Instagram |
| 3  | Python is great for data analysis.            | 2022-03-03 | 30    | 2        | 4      | Facebook  |
| 4  | I'm learning Python for machine learning.     | 2022-03-04 | 40    | 7        | 5      | Twitter   |

**Output**
| id | text                                          | date       | likes | comments | shares | platform  |
|----|-----------------------------------------------|------------|-------|----------|--------|-----------|
| 1  | This is a PySpark post.                       | 2022-03-01 | 10    | 3        | 2      | Twitter   |
| 2  | Another post about PySpark.                   | 2022-03-02 | 20    | 5        | 3      | Instagram |
| 3  | PySpark is great for data analysis.           | 2022-03-03 | 30    | 2        | 4      | Facebook  |
| 4  | I'm learning PySpark for machine learning.    | 2022-03-04 | 40    | 7        | 5      | Twitter   |`,
    starterCode: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(social_media):
    # Write code here
    pass`,
    solution: `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(social_media):
    new_social_media = social_media.withColumn(
        "text",
        F.regexp_replace(
            "text", "Python", "PySpark"
        ),
    )
    return new_social_media`,
    explanation: `The solution uses the \`regexp_replace\` function to replace all instances of "Python" with "PySpark".

**Step-by-step:**
1. Use \`withColumn\` to modify the \`text\` column
2. Apply \`F.regexp_replace()\` with three arguments:
   - Column name: "text"
   - Pattern to match: "Python"
   - Replacement string: "PySpark"
3. Return the modified DataFrame

**Key Concepts:**
- \`regexp_replace\` replaces matching patterns
- \`withColumn\` adds or replaces a column
- Pattern matching is case-sensitive by default`,
    hints: [
      "Use F.regexp_replace() to replace text patterns",
      "The first argument is the column name",
      "The second argument is the pattern to match",
      "The third argument is the replacement string"
    ],
    testCases: [
      {
        input: {
          social_media: [
            { id: 1, text: "This is a Python post.", date: "2022-03-01", likes: 10, comments: 3, shares: 2, platform: "Twitter" },
            { id: 2, text: "Another post about Python.", date: "2022-03-02", likes: 20, comments: 5, shares: 3, platform: "Instagram" },
          ]
        },
        expected_output: [
          { id: 1, text: "This is a PySpark post.", date: "2022-03-01", likes: 10, comments: 3, shares: 2, platform: "Twitter" },
          { id: 2, text: "Another post about PySpark.", date: "2022-03-02", likes: 20, comments: 5, shares: 3, platform: "Instagram" },
        ]
      }
    ]
  }
];

export const getProblemById = (id: string): Problem | undefined => {
  return problems.find((p) => p.id === id);
};
