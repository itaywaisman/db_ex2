
package corona;

import org.junit.Test;
import corona.business.*;
import static org.junit.Assert.assertEquals;
import static corona.business.ReturnValue.*;


public class BasicAPITests extends AbstractTest {
    @Test
    public void employeeJoinLabTest() {

        ReturnValue res;
        Lab s = new Lab();
        s.setId(1);
        s.setName("Technion");
        s.setCity("Haifa");
        s.setIsActive(true);


        res = Solution.addLab(s);
        assertEquals(OK, res);

        Employee a = new Employee();
        a.setId(2);
        a.setName("Roei");
        a.setCity("Haifa");
        ReturnValue ret = Solution.addEmployee(a);
        assertEquals(OK, ret);

        res = Solution.employeeJoinLab(2, 1, 100);
        assertEquals(OK, res);

        res = Solution.employeeJoinLab(1, -19, 100);
        assertEquals(NOT_EXISTS, res);
    }

    @Test
    public void PopularTest() {

        ReturnValue res;
        Lab s = new Lab();
        s.setId(1);
        s.setName("Technion");
        s.setCity("Haifa");
        s.setIsActive(true);


        res = Solution.addLab(s);
        assertEquals(OK, res);

        Vaccine v = new Vaccine();
        v.setId(5);
        v.setName("COVID-19");
        v.setCost(100);
        v.setProductivity(21);
        v.setUnits(30);
        res = Solution.addVaccine(v);
        assertEquals(OK, res);

        res = Solution.labProduceVaccine(5,1);
        assertEquals(OK, res);

        Boolean ret = Solution.isLabPopular(1);
        assertEquals(true, ret);
    }
}


