# Large-Scale Online Retail Sales Analysis Using Hadoop MapReduce

> **Module:** Cloud Computing (EE7222/EC7204) — University of Ruhuna, Faculty of Engineering  
> **Semester:** 7 | February 2026  
> **Dataset:** [Online Retail Dataset — Kaggle](https://www.kaggle.com/datasets/vijayuv/onlineretail)

---

## Table of Contents

- [Project Overview](#project-overview)
- [Dataset](#dataset)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Setup & Installation](#setup--installation)
- [How to Run](#how-to-run)
  - [Task 1: Sales per Country](#task-1-sales-per-country)
  - [Task 2: Most Sold Products](#task-2-most-sold-products)
  - [Task 3: Most Active Customers](#task-3-most-active-customers)
- [Expected Output](#expected-output)
- [Results Summary](#results-summary)

---

## Project Overview

This project implements three MapReduce jobs using Apache Hadoop to analyze a large-scale online retail transaction dataset (~541,909 records). The three analysis tasks are:

1. **Total Sales Revenue per Country** — `Revenue = Quantity × UnitPrice`
2. **Most Sold Products** — ranked by total quantity sold
3. **Most Active Customers** — ranked by total number of purchases

All jobs run in **Hadoop local mode** (no cluster required) and use `NLineInputFormat` to split the input CSV across 3 parallel mappers.

---

## Dataset

| Field | Details |
|---|---|
| Name | Online Retail Dataset |
| Source | Kaggle |
| Records | ~541,909 rows |
| Format | CSV (UTF-8) |
| File | `OnlineRetail.csv` |

**CSV Columns:** `InvoiceNo`, `StockCode`, `Description`, `Quantity`, `InvoiceDate`, `UnitPrice`, `CustomerID`, `Country`

> Download the dataset from Kaggle and place `OnlineRetail.csv` in the project root directory before running any jobs.

---

## Project Structure

```
Online-Retail-Analysis-Using-MapReduce/
│
├── OnlineRetail.csv                  # Input dataset (download from Kaggle)
│
├── src/
│   └── com/retail/
│       ├── SalesPerCountry.java      # Task 1 — Sales per country
│       ├── TopProducts.java          # Task 2 — Most sold products (2-phase)
│       └── TopCustomers.java         # Task 3 — Most active customers (2-phase)
│
├── output_sales/                     # Output: Task 1 results (generated at runtime)
├── output_products_temp/             # Intermediate: Task 2 Phase 1 (generated at runtime)
├── output_products/                  # Output: Task 2 final sorted results (generated at runtime)
├── output_customers_temp/            # Intermediate: Task 3 Phase 1 (generated at runtime)
├── output_customers/                 # Output: Task 3 final sorted results (generated at runtime)
│
└── README.md
```

---

## Prerequisites

| Requirement | Version | Notes |
|---|---|---|
| Java (JDK) | 8 or 11 | Must be on PATH |
| Apache Hadoop | 3.x | Local mode — no cluster needed |
| Maven (optional) | 3.x | For dependency management |

### Verify installations

```bash
java -version
hadoop version
```

---

## Setup & Installation

### 1. Install Java

```bash
# Ubuntu/Debian
sudo apt update
sudo apt install openjdk-11-jdk -y

# Verify
java -version
```

### 2. Install Hadoop (Local Mode)

```bash
# Download Hadoop (example: 3.3.6)
wget https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -xzf hadoop-3.3.6.tar.gz
sudo mv hadoop-3.3.6 /usr/local/hadoop
```

Add to your `~/.bashrc` or `~/.zshrc`:

```bash
export HADOOP_HOME=/usr/local/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
```

Then reload:

```bash
source ~/.bashrc
```

### 3. Configure Hadoop for Local Mode

Edit `$HADOOP_HOME/etc/hadoop/core-site.xml`:

```xml
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>file:///</value>
  </property>
</configuration>
```

Edit `$HADOOP_HOME/etc/hadoop/mapred-site.xml`:

```xml
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>local</value>
  </property>
</configuration>
```

### 4. Clone / Download this project

```bash
git clone <your-repo-url>
cd Online-Retail-Analysis-Using-MapReduce
```

### 5. Download the Dataset

Download `OnlineRetail.csv` from [Kaggle](https://www.kaggle.com/datasets/vijayuv/onlineretail) and place it in the project root:

```
Online-Retail-Analysis-Using-MapReduce/
└── OnlineRetail.csv    ← here
```

### 6. Compile the Source Code

```bash
# Create output directory for compiled classes
mkdir -p out

# Compile (adjust Hadoop jar path as needed)
javac -classpath "$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/mapreduce/*" \
  -d out \
  src/com/retail/SalesPerCountry.java \
  src/com/retail/TopProducts.java \
  src/com/retail/TopCustomers.java

# Package into a JAR
jar -cvf retail-analysis.jar -C out .
```

---

## How to Run

> Make sure `OnlineRetail.csv` is in your current working directory before running each command.  
> Each task writes to its own output folder. **Delete the output folder before re-running** or Hadoop will throw an error.

---

### Task 1: Sales per Country

Calculates total revenue (`Quantity × UnitPrice`) per country.

```bash
hadoop jar retail-analysis.jar com.retail.SalesPerCountry
```

**Output location:** `output_sales/part-r-00000`

**Sample output format:**
```
Australia       138521.32
Austria         10154.32
Bahrain         548.4
Belgium         40910.96
...
United Kingdom  8187806.36
```

---

### Task 2: Most Sold Products

Identifies products ranked by total quantity sold. Runs as a **2-phase chained job**:
- Phase 1 aggregates quantity per product → `output_products_temp/`
- Phase 2 sorts by quantity descending → `output_products/`

```bash
hadoop jar retail-analysis.jar com.retail.TopProducts
```

**Output location:** `output_products/part-r-00000`

**Sample output format:**
```
80995   PAPER CRAFT , LITTLE BIRDIE
78033   MEDIUM CERAMIC TOP STORAGE JAR
55047   WORLD WAR 2 GLIDERS ASSTD DESIGNS
48478   JUMBO BAG RED RETROSPOT
37895   WHITE HANGING HEART T-LIGHT HOLDER
```

---

### Task 3: Most Active Customers

Identifies customers ranked by total number of purchases. Also a **2-phase chained job**:
- Phase 1 aggregates purchase count per CustomerID → `output_customers_temp/`
- Phase 2 sorts by count descending → `output_customers/`

```bash
hadoop jar retail-analysis.jar com.retail.TopCustomers
```

**Output location:** `output_customers/part-r-00000`

**Sample output format:**
```
7847    17841
5897    14096
5170    12748
4306    14911
...
```
*(Format: `purchase_count  CustomerID`)*

---

### Re-running Jobs

If you need to re-run any task, delete the output directories first:

```bash
# Task 1
rm -rf output_sales

# Task 2
rm -rf output_products_temp output_products

# Task 3
rm -rf output_customers_temp output_customers
```

---

## Expected Output

### Execution Summary (from test run on full dataset)

| Task | Mappers | Input Records | Filtered Rows | Output Records | Duration |
|---|---|---|---|---|---|
| Sales per Country | 3 | 541,910 | 1 (header) | 38 countries | ~8 sec |
| Most Sold Products | 3 + 1 | 541,910 | 11,217 | 4,065 products | ~10 sec |
| Most Active Customers | 3 + 1 | 541,910 | 134,081 | 4,372 customers | ~9 sec |

### Top 5 Products by Quantity Sold

| Rank | Product | Units Sold |
|---|---|---|
| 1 | PAPER CRAFT, LITTLE BIRDIE | 80,995 |
| 2 | MEDIUM CERAMIC TOP STORAGE JAR | 78,033 |
| 3 | WORLD WAR 2 GLIDERS ASSTD DESIGNS | 55,047 |
| 4 | JUMBO BAG RED RETROSPOT | 48,478 |
| 5 | WHITE HANGING HEART T-LIGHT HOLDER | 37,895 |

---

## Results Summary

**Sales per Country:** Revenue is concentrated in a small number of countries, with the United Kingdom dominating as expected for a UK-based retailer. A total of 38 unique countries were identified across all transactions.

**Most Sold Products:** The top products are predominantly home decor, novelty gifts, and kitchenware. The combiner reduced ~541,909 mapper outputs down to just 97 intermediate records before the reducer, demonstrating high efficiency for low-cardinality key aggregation.

**Most Active Customers:** Approximately 24.7% of all transactions (134,081 rows) had no CustomerID, indicating a significant proportion of anonymous/guest purchases. Of the 4,372 identifiable customers, a classic long-tail distribution was observed — a small number of high-frequency buyers account for a disproportionate share of total orders.

---

*University of Ruhuna | Faculty of Engineering | EE7222/EC7204 Cloud Computing | 2026*