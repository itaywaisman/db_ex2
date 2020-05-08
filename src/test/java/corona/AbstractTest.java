package corona;

import corona.business.Employee;
import corona.business.Lab;
import corona.business.Vaccine;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;

public class AbstractTest {

    public static Lab newLab(int id, String name, String city, boolean isActive) {
        Lab lab = new Lab();
        lab.setId(id);
        lab.setName(name);
        lab.setCity(city);
        lab.setIsActive(isActive);
        return lab;
    }

    public static Employee newEmployee(int id, String name, String birth_city) {
        Employee emp = new Employee();
        emp.setId(id);
        emp.setName(name);
        emp.setCity(birth_city);
        return emp;
    }

    public static Vaccine newVaccine(int id, String name, int cost, int units, int productivity) {
        Vaccine vaccine = new Vaccine();
        vaccine.setId(id);
        vaccine.setName(name);
        vaccine.setCost(cost);
        vaccine.setUnits(units);
        vaccine.setProductivity(productivity);
        return vaccine;
    }

    @BeforeClass
    public static void createTables() {

        Solution.createTables();
    }

    @AfterClass
    public static void dropTables()
    {
        Solution.dropTables();
    }

    @Before
    public void clearTables()
    {
        Solution.clearTables();
    }

    @Rule
    public
    TestName name = new TestName();

}
