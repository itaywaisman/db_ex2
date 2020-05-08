package corona;

import corona.business.Employee;
import corona.business.Lab;
import corona.business.ReturnValue;
import corona.business.Vaccine;
import org.junit.Test;

import java.util.ArrayList;

import static corona.business.ReturnValue.*;
import static org.junit.Assert.*;

public class SimpleTest extends AbstractTest{

    @Test
    public void testAddLab() {
        ReturnValue ret = Solution.addLab(newLab(1, "Technion", "Haifa", true));
        assertEquals(OK, ret);

        ret = Solution.addLab(newLab(1, "Chicago", "Raanana", true));
        assertEquals(ALREADY_EXISTS, ret);

        ret = Solution.addLab(newLab(0, "Chicago", "Raanana", true));
        assertEquals(BAD_PARAMS, ret);
        
        // is null value possible in the API?
        ret = Solution.addLab(newLab(3, null, "Raanana", true));
        assertEquals(BAD_PARAMS, ret);

        ret = Solution.addLab(newLab(2, "Technion", "Haifa", true));
        assertEquals(OK, ret);
    }

    @Test
    public void testDeleteLab() {
        Lab a = newLab(10, "Technion", "Haifa", true);
        ReturnValue ret = Solution.deleteLab(a);
        assertEquals(NOT_EXISTS , ret);

        Solution.addLab(a);
        ret = Solution.deleteLab(a);
        assertEquals(OK , ret);
    }

    @Test
    public void testGetLabProfile() {
        Lab a = newLab(10, "Technion", "Haifa", true);
        Lab b = newLab(7, "Technion Away", "Tel Aviv", true);

        Lab ret = Solution.getLabProfile(5);
        assertEquals(Lab.badLab(), ret);

        Solution.addLab(a);
        ret = Solution.getLabProfile(a.getId());
        assertEquals(ret, a);

        Solution.addLab(b);
        ret = Solution.getLabProfile(a.getId());
        assertEquals(ret, a);

        ret = Solution.getLabProfile(5);
        assertEquals(Lab.badLab(), ret);
    }

    @Test
    public void testAddEmployee () {
        ReturnValue ret = Solution.addEmployee(newEmployee(1, "Avi", "Haifa"));
        assertEquals(OK, ret);

        ret = Solution.addEmployee(newEmployee(1, "Avihu", "Raanana"));
        assertEquals(ALREADY_EXISTS, ret);

        ret = Solution.addEmployee(newEmployee(0, "Avihu", "Raanana"));
        assertEquals(BAD_PARAMS, ret);

        ret = Solution.addEmployee(newEmployee(3, null, "Raanana"));
        assertEquals(BAD_PARAMS, ret);

        ret = Solution.addEmployee(newEmployee(2, "Avi", "Haifa"));
        assertEquals(OK, ret);
    }

    @Test
    public void testDeleteEmployee() {
        Employee a = newEmployee(10, "Avi", "Haifa");
        ReturnValue ret = Solution.deleteEmployee(a);
        assertEquals(NOT_EXISTS , ret);

        Solution.addEmployee(a);
        ret = Solution.deleteEmployee(a);
        assertEquals(OK , ret);
    }

    @Test
    public void testGetEmployeeProfile() {
        Employee a = newEmployee(10, "Avi", "Haifa");
        Employee b = newEmployee(7, "Avihu", "Raanana");

        Employee ret = Solution.getEmployeeProfile(5);
        assertEquals(Employee.badEmployee(), ret);

        Solution.addEmployee(a);
        ret = Solution.getEmployeeProfile(a.getId());
        assertEquals(ret, a);

        Solution.addEmployee(b);
        ret = Solution.getEmployeeProfile(a.getId());
        assertEquals(ret, a);

        ret = Solution.getEmployeeProfile(5);
        assertEquals(Employee.badEmployee(), ret);
    }

    @Test
    public void testAddVaccine () {
        ReturnValue ret = Solution.addVaccine(newVaccine(1, "Vaccinia",10, 0, 50));
        assertEquals(OK, ret);
        ret = Solution.addVaccine(newVaccine(2, "Trufa",0, 0, 0));
        assertEquals(OK, ret);

        ret = Solution.addVaccine(newVaccine(1, "Trufa",0, 0, 0));
        assertEquals(ALREADY_EXISTS, ret);

        ret = Solution.addVaccine(newVaccine(0, "Trufa",3, 700, 8));
        assertEquals(BAD_PARAMS, ret);

        ret = Solution.addVaccine(newVaccine(3, null,10, 10, 10));
        assertEquals(BAD_PARAMS, ret);

        ret = Solution.addVaccine(newVaccine(3, "Vaccinia",10, 0, 50));
        assertEquals(OK, ret);
    }

    @Test
    public void testDeleteVaccine() {
        Vaccine a = newVaccine(1, "Vaccinia",10, 0, 50);
        ReturnValue ret = Solution.deleteVaccine(a);
        assertEquals(NOT_EXISTS , ret);

        Solution.addVaccine(a);
        ret = Solution.deleteVaccine(a);
        assertEquals(OK , ret);
    }

    @Test
    public void testGetVaccineProfile() {
        Vaccine a = newVaccine(10, "Vaccinia",10, 0, 50);
        Vaccine b = newVaccine(7, "Trufa",20, 5, 10);

        Vaccine ret = Solution.getVaccineProfile(5);
        assertEquals(Vaccine.badVaccine(), ret);

        Solution.addVaccine(a);
        ret = Solution.getVaccineProfile(a.getId());
        assertEquals(ret, a);

        Solution.addVaccine(b);
        ret = Solution.getVaccineProfile(a.getId());
        assertEquals(ret, a);

        ret = Solution.getVaccineProfile(5);
        assertEquals(Vaccine.badVaccine(), ret);
    }

    // this test invokes every API function, but doesn't test any of
    // them thoroughly (sanity test)
    @Test
    public void testTouchEverything() {
        ReturnValue retval;
        Lab retlab;
        Employee retployee;
        Vaccine retcine;
        boolean retbull;
        int retnum;
        String retstr;
        ArrayList<Integer> retlist;

        Lab la = newLab(30, "LAlab", "Los Angeles", false);
        Lab lb = newLab(27, "Technion", "Haifa", true);
        Lab lc = newLab(25, "Technion Away", "Tel Aviv", true);
        Employee ea = newEmployee(20, "Avi", "Haifa");
        Employee eb = newEmployee(17, "Avihu", "Raanana");
        Employee ec = newEmployee(15, "Avihai", "Raanana");
        Vaccine va = newVaccine(10, "Vaccinia",32, 1000, 50);
        Vaccine vb = newVaccine(7, "Trufa",16, 50, 10);
        Vaccine vc = newVaccine(5, "Trufa",20, 25, 30);

        // CRUD add
        retval = Solution.addLab(la); assertEquals(OK, retval);
        retval = Solution.addLab(lb); assertEquals(OK, retval);
        retval = Solution.addLab(lc); assertEquals(OK, retval);
        retval = Solution.addEmployee(ea); assertEquals(OK, retval);
        retval = Solution.addEmployee(eb); assertEquals(OK, retval);
        retval = Solution.addEmployee(ec); assertEquals(OK, retval);
        retval = Solution.addVaccine(va); assertEquals(OK, retval);
        retval = Solution.addVaccine(vb); assertEquals(OK, retval);
        retval = Solution.addVaccine(vc); assertEquals(OK, retval);

        // CRUD del
        retval = Solution.deleteLab(lc); assertEquals(OK, retval);
        retval = Solution.deleteEmployee(ec); assertEquals(OK, retval);
        retval = Solution.deleteVaccine(vc); assertEquals(OK, retval);

        // CRUD get
        retlab = Solution.getLabProfile(la.getId()); assertEquals(la, retlab);
        retployee = Solution.getEmployeeProfile(ea.getId()); assertEquals(ea, retployee);
        retcine = Solution.getVaccineProfile(va.getId()); assertEquals(va, retcine);

        // basic API relations add
        retval = Solution.employeeJoinLab(ea.getId(), la.getId(), 2000);
        assertEquals(OK, retval);
        retval = Solution.employeeJoinLab(ea.getId(), lb.getId(), 500);
        assertEquals(OK, retval);
        retval = Solution.employeeJoinLab(eb.getId(), lb.getId(), 700);
        assertEquals(OK, retval);
        retval = Solution.employeeJoinLab(eb.getId(), la.getId(), 100);
        assertEquals(OK, retval);
        retval = Solution.labProduceVaccine(va.getId(), la.getId());
        assertEquals(OK, retval);
        retval = Solution.labProduceVaccine(va.getId(), lb.getId());
        assertEquals(OK, retval);
        retval = Solution.labProduceVaccine(vb.getId(), lb.getId());
        assertEquals(OK, retval);
        retval = Solution.labProduceVaccine(vb.getId(), la.getId());
        assertEquals(OK, retval);

        // basic API relations del
        retval = Solution.employeeLeftLab(la.getId(), eb.getId());
        assertEquals(OK, retval);
        retval = Solution.labStoppedProducingVaccine(la.getId(), vb.getId());
        assertEquals(OK, retval);

        // sell/produce vaccines
        retval = Solution.vaccineSold(va.getId(), 30);
        assertEquals(OK, retval);
        retval = Solution.vaccineProduced(vb.getId(), 20);
        assertEquals(OK, retval);

        // basic API queries
        retbull = Solution.isLabPopular(la.getId());
        assertTrue(retbull);
        retbull = Solution.isLabPopular(lb.getId());
        assertFalse(retbull);

        retnum = Solution.getIncomeFromVaccine(va.getId());
        assertEquals(960, retnum);
        retnum = Solution.getIncomeFromVaccine(vb.getId());
        assertEquals(0, retnum);

        retnum = Solution.getTotalNumberOfWorkingVaccines();
        assertEquals(970, retnum);

        retnum = Solution.getTotalWages(lb.getId());
        assertEquals(1200, retnum);

        retnum = Solution.getBestLab();
        assertEquals(lb.getId(), retnum);

        retstr = Solution.getMostPopularCity();
        assertEquals("Haifa", retstr);

        // advanced API queries
        retlist = Solution.getPopularLabs();
        assertNotNull(retlist);
        assertEquals(1, retlist.size());
        assertEquals(la.getId(), (int)retlist.get(0));

        retlist = Solution.getMostRatedVaccines();
        assertNotNull(retlist);
        assertEquals(2, retlist.size());
        assertEquals(va.getId(), (int)retlist.get(0));
        assertEquals(vb.getId(), (int)retlist.get(1));

        retlist = Solution.getCloseEmployees(ea.getId());
        assertNotNull(retlist);
        assertEquals(1, retlist.size());
        assertEquals(eb.getId(), (int)retlist.get(0));
        retlist = Solution.getCloseEmployees(eb.getId());
        assertNotNull(retlist);
        assertEquals(1, retlist.size());
        assertEquals(ea.getId(), (int)retlist.get(0));
    }
}
