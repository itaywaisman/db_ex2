package corona;


import corona.business.Employee;
import corona.business.Lab;
import corona.business.ReturnValue;
import corona.business.Vaccine;
import corona.data.DBConnector;
import corona.data.PostgreSQLErrorCodes;

import java.sql.*;
import java.util.ArrayList;

import static corona.business.ReturnValue.*;


public class Solution {

    ////////////////////////////////
    // Basic database methods
    ////////////////////////////////

    public static void createTables() {
        try {

            Connection connection = DBConnector.getConnection();
            assert connection != null;

            PreparedStatement createLabsStatement = connection.prepareStatement(
                    "CREATE TABLE labs (" +
                        "   id integer PRIMARY KEY," +
                        "   name text NOT NULL," +
                        "   city text NOT NULL," +
                        "   active boolean NOT NULL," +
                        "   constraint id_nonnegative check (id > 0)" +
                        ")"
            );
            createLabsStatement.executeUpdate();
            createLabsStatement.closeOnCompletion();

            PreparedStatement createEmployeeStatement = connection.prepareStatement(
                    "CREATE TABLE employees (" +
                        "   id integer PRIMARY KEY," +
                        "   name text NOT NULL," +
                        "   city text NOT NULL," +
                        "   constraint id_nonnegative check (id > 0)" +
                        ")"
            );
            createEmployeeStatement.executeUpdate();
            createEmployeeStatement.closeOnCompletion();


            PreparedStatement createVaccineStatement = connection.prepareStatement(
                    "CREATE TABLE vaccines (" +
                        "   id integer PRIMARY KEY," +
                        "   name text NOT NULL," +
                        "   cost integer NOT NULL," +
                        "   stock integer NOT NULL," +
                        "   productivity integer NOT NULL," +
                        "   total_sales integer NOT NULL DEFAULT 0," +
                        "   constraint id_nonnegative check (id > 0)," +
                        "   constraint cost_nonnegative check (cost >= 0)," +
                        "   constraint stock_nonnegative check (stock >= 0)," +
                        "   constraint productivity_nonnegative check (productivity >= 0)," +
                        "   constraint total_sales_nonnegative check (total_sales >= 0)" +
                        ")"
            );
            createVaccineStatement.executeUpdate();
            createVaccineStatement.closeOnCompletion();

            PreparedStatement createEmployeeLabsStatement = connection.prepareStatement(
                    "CREATE TABLE employees_labs (" +
                        "   employee_id integer REFERENCES employees(id)," +
                        "   lab_id integer REFERENCES labs(id)," +
                        "   salary integer NOT NULL," +
                        "   constraint salary_nonnegative check (salary >= 0)," +
                        "   PRIMARY KEY (employee_id, lab_id)" +
                        ")"
            );
            createEmployeeLabsStatement.executeUpdate();
            createEmployeeLabsStatement.closeOnCompletion();

            PreparedStatement createVaccinesLabsStatement = connection.prepareStatement(
                    "CREATE TABLE vaccines_labs (" +
                        "   vaccine_id integer REFERENCES vaccines(id)," +
                        "   lab_id integer REFERENCES labs(id)," +
                        "   PRIMARY KEY (vaccine_id, lab_id)" +
                        ")"
            );
            createVaccinesLabsStatement.executeUpdate();
            createVaccinesLabsStatement.closeOnCompletion();

            PreparedStatement createVaccinesLabsViewStatement = connection.prepareStatement(
                    "CREATE OR REPLACE VIEW public.vaccines_labs_view " +
                        "AS " +
                        "SELECT v.id AS vaccine_id, " +
                        "   l.id AS lab_id, " +
                        "   v.name AS vaccine_name, " +
                        "   v.cost AS vaccine_cost, " +
                        "   v.productivity AS vaccine_productivity, " +
                        "   l.name AS lab_name, " +
                        "   l.city AS lab_city, " +
                        "   l.active AS is_lab_active " +
                        "FROM vaccines v " +
                        "JOIN vaccines_labs vl ON vl.vaccine_id = v.id " +
                        "JOIN labs l ON l.id = vl.lab_id; " +
                        "" +
                        "ALTER TABLE public.vaccines_labs_view " +
                        "    OWNER TO java;"
            );
            createVaccinesLabsViewStatement.executeUpdate();
            createVaccinesLabsViewStatement.closeOnCompletion();


            PreparedStatement createEmployeesLabsViewStatement = connection.prepareStatement(
                    "CREATE OR REPLACE VIEW public.employees_labs_view " +
                        "AS " +
                        "SELECT e.id AS employee_id, " +
                        "   l.id AS lab_id, " +
                        "   e.name AS employee_name, " +
                        "   e.city AS employee_city, " +
                        "   l.name AS lab_name, " +
                        "   l.city AS lab_city, " +
                        "   l.active AS is_lab_active " +
                        "FROM employees e " +
                        "JOIN employees_labs el ON el.employee_id = e.id " +
                        "JOIN labs l ON l.id = el.lab_id; " +
                        "" +
                        "ALTER TABLE public.employees_labs_view " +
                        "    OWNER TO java;"
            );
            createEmployeesLabsViewStatement.executeUpdate();
            createEmployeesLabsViewStatement.closeOnCompletion();

            connection.close();
        } catch(Exception e) {
            System.out.println("Failed to create tables!"+ e);
        }
    }

    public static void clearTables() {
        try {
            Connection connection = DBConnector.getConnection();
            assert connection != null;

            PreparedStatement createLabsStatement = connection.prepareStatement(
                    "TRUNCATE labs, employees, vaccines, employees_labs, vaccines_labs"
            );
            createLabsStatement.execute();
            createLabsStatement.closeOnCompletion();

            connection.close();
        } catch(Exception e) {
            System.out.println("Failed to clear tables!"+ e);
        }
    }

    public static void dropTables() {
        try {
            Connection connection = DBConnector.getConnection();
            assert connection != null;

            PreparedStatement dropLabsStatement = connection.prepareStatement(
                    "DROP TABLE labs, employees, vaccines, employees_labs, vaccines_labs CASCADE"
            );
            dropLabsStatement.execute();
            dropLabsStatement.closeOnCompletion();

            connection.close();
        } catch(Exception e) {
            System.out.println("Failed to drop tables!" + e);
        }
    }

    ////////////////////////////////
    // Basic CRUD methods
    ////////////////////////////////

    public static ReturnValue addLab(Lab lab) {

        try {
            Connection connection = DBConnector.getConnection();
            if(connection == null) return ReturnValue.ERROR;

            PreparedStatement createLabStatement = connection.prepareStatement(
                    "INSERT INTO labs(id, name, city, active) " +
                        "VALUES (?, ?, ?, ?)"
            );
            createLabStatement.setInt(1, lab.getId());
            createLabStatement.setString(2, lab.getName());
            createLabStatement.setString(3, lab.getCity());
            createLabStatement.setBoolean(4, lab.getIsActive());

            int affectedRows = createLabStatement.executeUpdate();

            if (affectedRows == 1)
                return ReturnValue.OK;

        } catch(SQLException exception) {
            return convertSqlState(exception.getSQLState());
        }

        return OK;
    }

    public static Lab getLabProfile(Integer labID) {
        Lab lab;
        try {
            Connection connection = DBConnector.getConnection();
            if(connection == null)  return Lab.badLab();

            PreparedStatement createLabStatement = connection.prepareStatement(
                    "SELECT l.id, l.name, l.city, l.active " +
                        "FROM labs l " +
                        "WHERE l.id = ?"
            );
            createLabStatement.setInt(1, labID);

            ResultSet resultSet = createLabStatement.executeQuery();

            lab = readLab(resultSet);

            resultSet.close();
            createLabStatement.close();
            connection.close();



        } catch(SQLException exception) {
            return Lab.badLab();
        }

        if(lab == null) lab = Lab.badLab();
        return lab;
    }

    public static ReturnValue deleteLab(Lab lab) {

        try {
            Connection connection = DBConnector.getConnection();
            if(connection == null)  return ReturnValue.ERROR;

            PreparedStatement deleteLabStatement = connection.prepareStatement(
                    "DELETE FROM labs " +
                        "WHERE id = ?"
            );
            deleteLabStatement.setInt(1, lab.getId());

            int affectedRows = deleteLabStatement.executeUpdate();

            deleteLabStatement.close();
            connection.close();

            if(affectedRows != 1) return NOT_EXISTS;

        } catch(SQLException exception) {
            return ReturnValue.ERROR;
        }

        return OK;
    }

    public static ReturnValue addEmployee(Employee employee) {

        try {
            Connection connection = DBConnector.getConnection();
            if(connection == null) return ReturnValue.ERROR;

            PreparedStatement createEmployeeStatement = connection.prepareStatement(
                    "INSERT INTO public.employees(" +
                        "id, name, city)" +
                        "VALUES (?, ?, ?)"
            );
            createEmployeeStatement.setInt(1, employee.getId());
            createEmployeeStatement.setString(2, employee.getName());
            createEmployeeStatement.setString(3, employee.getCity());

            int affectedRows = createEmployeeStatement.executeUpdate();

            if (affectedRows != 1) return ReturnValue.ERROR;

        } catch(SQLException exception) {
            return convertSqlState(exception.getSQLState());
        }

        return OK;
    }

    public static Employee getEmployeeProfile(Integer employeeID) {

        Employee employee;

        try {
            Connection connection = DBConnector.getConnection();
            if(connection == null)  return Employee.badEmployee();

            PreparedStatement queryEmployeeStatement = connection.prepareStatement(
                    "SELECT e.id, e.name, e.city " +
                        "FROM employees e " +
                        "WHERE e.id = ?"
            );
            queryEmployeeStatement.setInt(1, employeeID);

            ResultSet resultSet = queryEmployeeStatement.executeQuery();

            employee = readEmployee(resultSet);

            resultSet.close();
            queryEmployeeStatement.close();
            connection.close();

        } catch(SQLException exception) {
            return Employee.badEmployee();
        }

        if(employee == null) return Employee.badEmployee();
        return employee;
    }

    public static ReturnValue deleteEmployee(Employee employee) {

        try {
            Connection connection = DBConnector.getConnection();
            if(connection == null)  return ReturnValue.ERROR;

            PreparedStatement deleteEmployeeStatement = connection.prepareStatement(
                    "DELETE FROM employees " +
                        "WHERE id = ?"
            );
            deleteEmployeeStatement.setInt(1, employee.getId());

            int affectedRows = deleteEmployeeStatement.executeUpdate();

            deleteEmployeeStatement.close();
            connection.close();

            if(affectedRows != 1) return NOT_EXISTS;

        } catch(SQLException exception) {
            return ReturnValue.ERROR;
        }

        return OK;
    }

    public static ReturnValue addVaccine(Vaccine vaccine) {

        try {
            Connection connection = DBConnector.getConnection();
            if(connection == null) return ReturnValue.ERROR;

            PreparedStatement createEmployeeStatement = connection.prepareStatement(
                    "INSERT INTO public.vaccines(" +
                        "id, name, cost, stock, productivity)" +
                        "VALUES (?, ?, ?, ?, ?)"
            );
            createEmployeeStatement.setInt(1, vaccine.getId());
            createEmployeeStatement.setString(2, vaccine.getName());
            createEmployeeStatement.setInt(3, vaccine.getCost());
            createEmployeeStatement.setInt(4, vaccine.getUnits());
            createEmployeeStatement.setInt(5, vaccine.getProductivity());

            int affectedRows = createEmployeeStatement.executeUpdate();

            if (affectedRows != 1) return ReturnValue.ERROR;

        } catch(SQLException exception) {
            return convertSqlState(exception.getSQLState());
        }

        return OK;
    }

    public static Vaccine getVaccineProfile(Integer vaccineID) {
        Vaccine vaccine;

        try {
            Connection connection = DBConnector.getConnection();
            if(connection == null)  return Vaccine.badVaccine();

            PreparedStatement queryVaccineStatement = connection.prepareStatement(
                    "SELECT v.id, v.name, v.cost, v.stock, v.productivity " +
                        "FROM vaccines v " +
                        "WHERE v.id = ?"
            );
            queryVaccineStatement.setInt(1, vaccineID);

            ResultSet resultSet = queryVaccineStatement.executeQuery();

            vaccine = readVaccine(resultSet);

            resultSet.close();
            queryVaccineStatement.close();
            connection.close();

        } catch(SQLException exception) {
            return Vaccine.badVaccine();
        }

        if(vaccine == null) return Vaccine.badVaccine();
        return vaccine;
    }

    public static ReturnValue deleteVaccine(Vaccine vaccine) {
        try {
            Connection connection = DBConnector.getConnection();
            if(connection == null)  return ReturnValue.ERROR;

            PreparedStatement deleteVaccineStatement = connection.prepareStatement(
                    "DELETE FROM vaccines " +
                        "WHERE id = ?"
            );
            deleteVaccineStatement.setInt(1, vaccine.getId());

            int affectedRows = deleteVaccineStatement.executeUpdate();

            deleteVaccineStatement.close();
            connection.close();

            if(affectedRows != 1) return NOT_EXISTS;

        } catch(SQLException exception) {
            return ReturnValue.ERROR;
        }

        return OK;
    }

    ////////////////////////////////
    // Basic CRUD methods
    ////////////////////////////////


    public static ReturnValue employeeJoinLab(Integer employeeID, Integer labID, Integer salary) {

        try {
            Connection connection = DBConnector.getConnection();
            if(connection == null) return ReturnValue.ERROR;

            PreparedStatement createStatement = connection.prepareStatement(
                    "INSERT INTO public.employees_labs(employee_id, lab_id, salary) " +
                        "VALUES (?, ?, ?)"
            );
            createStatement.setInt(1, employeeID);
            createStatement.setInt(2, labID);
            createStatement.setInt(3, salary);

            int affectedRows = createStatement.executeUpdate();

            if (affectedRows != 1) return ReturnValue.ERROR;

        } catch(SQLException exception) {
            return convertSqlState(exception.getSQLState());
        }

        return OK;
    }

    public static ReturnValue employeeLeftLab(Integer labID, Integer employeeID) {

        try {
            Connection connection = DBConnector.getConnection();
            if(connection == null)  return ReturnValue.ERROR;

            PreparedStatement deleteStatement = connection.prepareStatement(
                    "DELETE FROM public.employees_labs el " +
                        "WHERE el.employee_id = ? " +
                        "AND el.lab_id = ?"
            );
            deleteStatement.setInt(1, employeeID);
            deleteStatement.setInt(2, labID);

            int affectedRows = deleteStatement.executeUpdate();

            deleteStatement.close();
            connection.close();

            if(affectedRows != 1) return NOT_EXISTS;

        } catch(SQLException exception) {
            return ReturnValue.ERROR;
        }

        return OK;
    }

    public static ReturnValue labProduceVaccine(Integer vaccineID, Integer labID) {

        try {
            Connection connection = DBConnector.getConnection();
            if(connection == null) return ReturnValue.ERROR;

            PreparedStatement createStatement = connection.prepareStatement(
                    "INSERT INTO public.vaccines_labs(vaccine_id, lab_id)" +
                        "VALUES (?, ?)"
            );
            createStatement.setInt(1, vaccineID);
            createStatement.setInt(2, labID);

            int affectedRows = createStatement.executeUpdate();

            if (affectedRows != 1) return ReturnValue.ERROR;

        } catch(SQLException exception) {
            return convertSqlState(exception.getSQLState());
        }

        return OK;
    }

    public static ReturnValue labStoppedProducingVaccine(Integer labID, Integer vaccineID) {


        try {
            Connection connection = DBConnector.getConnection();
            if(connection == null)  return ReturnValue.ERROR;

            PreparedStatement deleteStatement = connection.prepareStatement(
                    "DELETE FROM public.vaccines_labs vl " +
                        "WHERE vl.vaccine_id = ? AND vl.lab_id = ?"
            );
            deleteStatement.setInt(1, vaccineID);
            deleteStatement.setInt(2, labID);

            int affectedRows = deleteStatement.executeUpdate();

            deleteStatement.close();
            connection.close();

            if(affectedRows != 1) return NOT_EXISTS;

        } catch(SQLException exception) {
            return ReturnValue.ERROR;
        }

        return OK;
    }

    public static ReturnValue vaccineSold(Integer vaccineID, Integer amount) {

        try {
            Connection connection = DBConnector.getConnection();
            if(connection == null) return ReturnValue.ERROR;


            PreparedStatement createEmployeeStatement = connection.prepareStatement(
                    "UPDATE vaccines " +
                        "SET stock = stock - ?, " +
                        "cost = cost * 2, " +
                        "productivity = LEAST(productivity + 15, 100), " +
                        "total_sales =  total_sales + (cost * ?) " +
                        "WHERE id = ?"
            );
            createEmployeeStatement.setInt(1, amount);
            createEmployeeStatement.setInt(2, amount);
            createEmployeeStatement.setInt(3, vaccineID);

            int affectedRows = createEmployeeStatement.executeUpdate();

            if (affectedRows != 1) return ReturnValue.ERROR;

        } catch(SQLException exception) {
            return convertSqlState(exception.getSQLState());
        }

        return OK;    }

    public static ReturnValue vaccineProduced(Integer vaccineID, Integer amount) {

        if(amount < 0) return BAD_PARAMS;

        try {
            Connection connection = DBConnector.getConnection();
            if(connection == null) return ReturnValue.ERROR;

            PreparedStatement createEmployeeStatement = connection.prepareStatement(
                    "UPDATE vaccines v " +
                        "SET stock = stock + ?, " +
                        "cost = cost / 2, " +
                        "productivity = GREATEST(productivity - 15, 0)" +
                        "WHERE id = ?"
            );
            createEmployeeStatement.setInt(1, amount);
            createEmployeeStatement.setInt(2, vaccineID);

            int affectedRows = createEmployeeStatement.executeUpdate();

            if (affectedRows != 1) return ReturnValue.ERROR;

        } catch(SQLException exception) {
            return convertSqlState(exception.getSQLState());
        }

        return OK;
    }

    public static Boolean isLabPopular(Integer labID) {
        boolean isPopular = true;
        try {
            Connection connection = DBConnector.getConnection();
            if(connection == null)  return false;

            PreparedStatement queryVaccineStatement = connection.prepareStatement(
                    "SELECT COUNT(*) " +
                        "FROM vaccines_labs_view vl " +
                        "WHERE vl.lab_id = ? " +
                        "AND vl.vaccine_productivity < 20"
            );
            queryVaccineStatement.setInt(1, labID);

            ResultSet resultSet = queryVaccineStatement.executeQuery();

            while(resultSet.next()) {
                int unproductiveVaccines = resultSet.getInt(1);
                isPopular = unproductiveVaccines == 0;
            }

            resultSet.close();
            queryVaccineStatement.close();
            connection.close();


        } catch(SQLException exception) {
            return false;
        }
        return isPopular;
    }

    public static Integer getIncomeFromVaccine(Integer vaccineID) {
        int result;
        try {
            Connection connection = DBConnector.getConnection();
            if(connection == null)  return 0;

            PreparedStatement queryStatement = connection.prepareStatement(
                    "SELECT total_sales " +
                        "FROM vaccines v " +
                        "WHERE v.id = ? "
            );
            queryStatement.setInt(1, vaccineID);

            ResultSet resultSet = queryStatement.executeQuery();

            result = readInt(resultSet);

            resultSet.close();
            queryStatement.close();
            connection.close();


        } catch(SQLException exception) {
            return 0;
        }
        return result;
    }

    public static Integer getTotalNumberOfWorkingVaccines() {
        int result;
        try {
            Connection connection = DBConnector.getConnection();
            if(connection == null)  return 0;

            PreparedStatement queryStatement = connection.prepareStatement(
                    "SELECT sum(v.stock) " +
                        "FROM public.vaccines v " +
                        "WHERE productivity >= 20"
            );

            ResultSet resultSet = queryStatement.executeQuery();

            result = readInt(resultSet);

            resultSet.close();
            queryStatement.close();
            connection.close();


        } catch(SQLException exception) {
            return 0;
        }
        return result;
    }

    public static Integer getTotalWages(Integer labID) {
        int result;
        try {
            Connection connection = DBConnector.getConnection();
            if(connection == null)  return 0;

            PreparedStatement queryStatement = connection.prepareStatement(
                    "SELECT COALESCE(SUM(salary), 0) " +
                        "FROM employees " +
                        "JOIN employees_labs el ON employees.id = el.employee_id " +
                        "JOIN labs l ON el.lab_id = l.id " +
                        "WHERE l.active = TRUE " +
                        "AND l.id = ?"
            );
            queryStatement.setInt(1, labID);

            ResultSet resultSet = queryStatement.executeQuery();

            result = readInt(resultSet);

            resultSet.close();
            queryStatement.close();
            connection.close();


        } catch(SQLException exception) {
            return 0;
        }
        return result;
    }

    public static Integer getBestLab() {
        int result;
        try {
            Connection connection = DBConnector.getConnection();
            if(connection == null)  return 0;

            PreparedStatement queryStatement = connection.prepareStatement(
                    "SELECT l.id " +
                        "FROM labs l " +
                        "JOIN employees_labs el ON l.id = el.lab_id " +
                        "JOIN employees e ON el.employee_id = e.id " +
                        "WHERE e.city = l.city " +
                        "GROUP BY l.id " +
                        "ORDER BY count(e.id) DESC, l.id ASC"
            );

            ResultSet resultSet = queryStatement.executeQuery();
            if(!resultSet.next())
                return 0;
            result = resultSet.getInt(1);

            resultSet.close();
            queryStatement.close();
            connection.close();


        } catch(SQLException exception) {
            return 0;
        }
        return result;
    }

    public static String getMostPopularCity() {
        String result;
        try {
            Connection connection = DBConnector.getConnection();
            if(connection == null)  return null;

            PreparedStatement queryStatement = connection.prepareStatement(
                    "SELECT elv.employee_city " +
                        "FROM employees_labs_view elv " +
                        "GROUP BY elv.employee_city " +
                        "ORDER BY count(elv.employee_city) DESC " +
                        "LIMIT 1"
            );

            ResultSet resultSet = queryStatement.executeQuery();
            if(!resultSet.next())
                return "";
            result = resultSet.getString(1);

            resultSet.close();
            queryStatement.close();
            connection.close();


        } catch(SQLException exception) {
            return null;
        }
        return result;
    }

    ////////////////////////////////////
    // Advanced API
    ////////////////////////////////////

    public static ArrayList<Integer> getPopularLabs() {
        ArrayList<Integer> result = new ArrayList<>();
        try {
            Connection connection = DBConnector.getConnection();
            if(connection == null)  return null;

            PreparedStatement queryStatement = connection.prepareStatement(
                    "SELECT l.id " +
                        "FROM labs l " +
                        "WHERE l.id NOT IN ( " +
                        "    SELECT vlv.lab_id " +
                        "    FROM vaccines_labs_view vlv " +
                        "    WHERE vlv.vaccine_productivity < 20 " +
                        ") " +
                        "AND l.id IN (SELECT vlv2.lab_id FROM vaccines_labs_view vlv2) " +
                        "ORDER BY l.id " +
                        "LIMIT 3"
            );

            ResultSet resultSet = queryStatement.executeQuery();
            while(resultSet.next()) {
                result.add(resultSet.getInt(1));
            }

            resultSet.close();
            queryStatement.close();
            connection.close();


        } catch(SQLException exception) {
            return null;
        }
        return result;
    }

    public static ArrayList<Integer> getMostRatedVaccines() {
        ArrayList<Integer> result = new ArrayList<>();
        try {
            Connection connection = DBConnector.getConnection();
            if(connection == null)  return null;

            PreparedStatement queryStatement = connection.prepareStatement(
                    "SELECT v.id " +
                        "FROM vaccines v " +
                        "ORDER BY (v.stock + v.productivity - v.cost) DESC, id " +
                        "LIMIT 10"
            );

            ResultSet resultSet = queryStatement.executeQuery();
            while(resultSet.next()) {
                result.add(resultSet.getInt(1));
            }

            resultSet.close();
            queryStatement.close();
            connection.close();


        } catch(SQLException exception) {
            return null;
        }
        return result;
    }

    public static ArrayList<Integer> getCloseEmployees(Integer employeeID) {
        ArrayList<Integer> result = new ArrayList<>();
        try {
            Connection connection = DBConnector.getConnection();
            if(connection == null)  return null;

            PreparedStatement queryStatement = connection.prepareStatement(
                    "SELECT elv1.employee_id " +
                        "FROM employees_labs_view elv1 " +
                        "WHERE elv1.lab_city IN (SELECT elv2.lab_city " +
                        "                          FROM employees_labs_view elv2 " +
                        "                         WHERE elv2.employee_id = ?) " +
                        "AND elv1.employee_id != ? " +
                        "GROUP BY elv1.employee_id " +
                        "HAVING 100 * count(elv1.employee_id) / (SELECT count(*) FROM (SELECT distinct elv3.lab_city " +
                        "                                    FROM employees_labs_view elv3 " +
                        "                                   WHERE elv3.employee_id = ?) as distinct_cities) >= 50 " +
                        "ORDER BY elv1.employee_id " +
                        "LIMIT 10"
            );
            queryStatement.setInt(1, employeeID);
            queryStatement.setInt(2, employeeID);
            queryStatement.setInt(3, employeeID);

            ResultSet resultSet = queryStatement.executeQuery();
            while(resultSet.next()) {
                result.add(resultSet.getInt(1));
            }

            resultSet.close();
            queryStatement.close();
            connection.close();


        } catch(SQLException exception) {
            return null;
        }
        return result;
    }

    ////////////////////////////////////
    // Utility Methods
    ////////////////////////////////////
    private static Lab readLab(ResultSet resultSet) throws SQLException {
        Lab lab;
        if(!resultSet.next()) return null;

        lab = new Lab();
        lab.setId(resultSet.getInt("id"));
        lab.setName(resultSet.getString("name"));
        lab.setCity(resultSet.getString("city"));
        lab.setIsActive(resultSet.getBoolean("active"));

        return lab;
    }

    private static Employee readEmployee(ResultSet resultSet) throws SQLException {
        Employee employee;
        if(!resultSet.next()) return null;

        employee = new Employee();
        employee.setId(resultSet.getInt("id"));
        employee.setName(resultSet.getString("name"));
        employee.setCity(resultSet.getString("city"));

        return employee;
    }

    private static Vaccine readVaccine(ResultSet resultSet) throws SQLException {
        Vaccine vaccine;

        if(!resultSet.next()) return null;
        vaccine = new Vaccine();
        vaccine.setId(resultSet.getInt("id"));
        vaccine.setName(resultSet.getString("name"));
        vaccine.setCost(resultSet.getInt("cost"));
        vaccine.setUnits(resultSet.getInt("stock"));
        vaccine.setProductivity(resultSet.getInt("productivity"));

        return vaccine;
    }

    private static int readInt(ResultSet resultSet) throws SQLException {
        if(resultSet.next())
            return resultSet.getInt(1);
        throw new SQLException("Could not parse int from the ResultSet");
    }

    private static ReturnValue convertSqlState(String sqlState) {
        if(sqlState.equals("" + PostgreSQLErrorCodes.UNIQUE_VIOLATION.getValue()))
            return ReturnValue.ALREADY_EXISTS;
        if(sqlState.equals("" + PostgreSQLErrorCodes.CHECK_VIOLATION.getValue()))
            return ReturnValue.BAD_PARAMS;
        if(sqlState.equals("" + PostgreSQLErrorCodes.NOT_NULL_VIOLATION.getValue()))
            return ReturnValue.BAD_PARAMS;
        if(sqlState.equals("" + PostgreSQLErrorCodes.FOREIGN_KEY_VIOLATION.getValue()))
            return ReturnValue.NOT_EXISTS;
        else
            return ReturnValue.ERROR;
    }
}

