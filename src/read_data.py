from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Read Data").getOrCreate()

# List the following details of each employee: employee number, last name, first name, sex, and salary.

emp = spark.read.csv("C:\workspace\python-data-engineering-scripts\sample_data\employees.csv", header=True, inferSchema=True)

sal = spark.read.csv("C:\workspace\python-data-engineering-scripts\sample_data\salaries.csv", header=True, inferSchema=True)

# Join the employees and salaries tables to show the employee number, last name, first name, and salary.
emp_sal = emp.join(sal, emp.emp_no == sal.emp_no, "inner").select(emp.emp_no, emp.last_name, emp.first_name, sal.salary)

#emp_sal.show()

# List the first name, last name, and hire date for employees who were hired in 1986.
from pyspark.sql.functions import col, year, to_date, to_timestamp, date_format

#emp_hire = emp.filter(col("hire_date").like("1986%")).select(emp.first_name, emp.last_name, emp.hire_date)

emp_hire = emp.withColumn("year", emp.hire_date.substr(-4,4)).filter(col("year") == "1986")

print("Total emp hired in 1986: ", emp_hire.count())

# List the manager of each department with the following information:
# department number, department name, the manager’s employee number, last name, first name.

dept = spark.read.csv("C:\workspace\python-data-engineering-scripts\sample_data\departments.csv", header=True, inferSchema=True)
dept_emp = spark.read.csv("C:\workspace\python-data-engineering-scripts\sample_data\dept_emp.csv", header=True, inferSchema=True)
dept_manager = spark.read.csv("C:\workspace\python-data-engineering-scripts\sample_data\dept_manager.csv", header=True, inferSchema=True)

df = (dept.join(dept_manager, dept.dept_no == dept_manager.dept_no, "inner")
      .join(emp, dept_manager.emp_no == emp.emp_no, "inner")
      .select(dept.dept_no, dept.dept_name, emp.emp_no, emp.last_name, emp.first_name))

# df.show()

# List the department of each employee with the following information: employee number, last name, first name, and department name.

dfDeptEmp = ((emp.join(dept_emp, emp.emp_no == dept_emp.emp_no, "inner"))
      .join(dept, dept.dept_no == dept_emp.dept_no, "inner"))

dfSelect = dfDeptEmp.select(emp.emp_no, emp.last_name, emp.first_name, dept.dept_name)
# dfSelect.show()

# List first name, last name, and sex for employees whose first name is “Hercules” and last names begin with “B.”
dfEmp = emp.select(emp.first_name, emp.last_name, emp.sex).filter(
      (col("first_name") == "Hercules")
      &(col("last_name").startswith("B")))

# dfEmp.show()

# List all employees in the Sales department, including their employee number, last name, first name, and department name.
dfSales = (dept.join(dept_emp, dept.dept_no == dept_emp.dept_no, "inner")
           .join(emp, emp.emp_no == dept_emp.emp_no, "inner")
           .select(emp.emp_no, emp.last_name, emp.first_name, dept.dept_name)
           .filter(dept.dept_name == "Sales"))

# dfSales.show()

# List all employees in the Sales and Development departments, including their employee number, last name, first name, and department name.
dfSalesDev = (dept.join(dept_emp, dept.dept_no == dept_emp.dept_no, "inner")
           .join(emp, emp.emp_no == dept_emp.emp_no, "inner")
           .select(emp.emp_no, emp.last_name, emp.first_name, dept.dept_name)
           .filter((dept.dept_name == "Sales") | (dept.dept_name == "Development")))

# dfSalesDev.show()

# In descending order, list the frequency count of employee last names, i.e., how many employees share each last name.
from pyspark.sql.functions import desc
dfLastName = (emp.groupBy("last_name").count()
              .select("last_name", col("count").alias("frequency"))
              .sort(desc("frequency")))

dfLastName.show()