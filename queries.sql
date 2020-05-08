-- isLabPopular
SELECT COUNT(*)
FROM vaccines_labs_view vl
WHERE vl.lab_id = 30
AND vl.vaccine_productivity < 20;

-- getTotalWages
SELECT COALESCE(SUM(salary), 0)
FROM employees
JOIN employees_labs el ON employees.id = el.employee_id
JOIN labs l ON el.lab_id = l.id
WHERE l.active = TRUE
AND l.id = 17;

-- getBestLab
SELECT elv.lab_id
 FROM employees_labs_view elv
WHERE elv.employee_city = elv.lab_city
GROUP BY elv.lab_id
ORDER BY count(elv.employee_id) DESC,elv.lab_id;

-- getMostPopularCity
SELECT elv.employee_city
FROM employees_labs_view elv
GROUP BY elv.employee_city
ORDER BY count(elv.employee_city) DESC
LIMIT 1;

-- getPopularLabs
SELECT l.id
FROM labs l
WHERE l.id NOT IN (
    SELECT vlv.lab_id
    FROM vaccines_labs_view vlv
    WHERE vlv.vaccine_productivity < 20
)
AND l.id IN (SELECT vlv2.lab_id FROM vaccines_labs_view vlv2)
ORDER BY l.id
LIMIT 3;


-- getMostRatedVaccines
SELECT v.id
FROM vaccines v
ORDER BY (v.stock + v.productivity - v.cost) DESC, id
LIMIT 10;

-- getCloseEmployees
SELECT elv1.employee_id
FROM employees_labs_view elv1
WHERE elv1.lab_city IN (SELECT elv2.lab_city FROM employees_labs_view elv2 WHERE elv2.employee_id = 20)
AND elv1.employee_id != 20
GROUP BY elv1.employee_id
HAVING 100 * count(elv1.employee_id) / (SELECT count(*) FROM (SELECT distinct elv3.lab_city
                                    FROM employees_labs_view elv3
                                   WHERE elv3.employee_id = 20) as distinct_cities) >= 50
ORDER BY elv1.employee_id
LIMIT 10;
