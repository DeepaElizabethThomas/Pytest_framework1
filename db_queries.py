# Extract employee data
QUERY_EXTRACT_EMPLOYEES = "SELECT * FROM employee"

# Transformation: Convert name to lowercase, calculate total_salary
QUERY_TRANSFORM_EMPLOYEES = """
SELECT 
    LOWER(name) AS name, 
    department, 
    salary, 
    commission, 
    (salary + commission) AS total_salary
FROM employee
"""

# Create target table `employee_details`
QUERY_CREATE_EMPLOYEE_DETAILS = """
IF OBJECT_ID('employee_details', 'U') IS NOT NULL DROP TABLE employee_details;
CREATE TABLE employee_details (
    name VARCHAR(255),
    department_id INT,
    salary FLOAT,
    commission FLOAT,
    total_salary FLOAT
);
"""

QUERY_LOAD_EMPLOYEE_DETAILS = """
INSERT INTO employee_details (name, department_id, salary, commission, total_salary)
VALUES (:name, :department_id, :salary, :commission, :total_salary)
"""

# Create `Department_Average` table
QUERY_CREATE_DEPARTMENT_AVERAGE = """
IF OBJECT_ID('Department_Average', 'U') IS NOT NULL DROP TABLE dbo.Department_Average;
CREATE TABLE Department_Average (
    department_id NVARCHAR(255),
    avg_salary FLOAT
);
"""

# Calculate department-wise average salary
QUERY_CALCULATE_DEPARTMENT_AVERAGE = """
INSERT INTO dbo.Department_Average (department_id, avg_salary)
SELECT department_id, AVG(total_salary) 
FROM employee_details 
GROUP BY department_id;
"""
